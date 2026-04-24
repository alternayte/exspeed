use std::cmp::Ordering;
use std::collections::BinaryHeap;

use super::Operator;
use crate::parser::ast::OrderByItem;
use crate::runtime::eval::eval_expr;
use crate::types::Row;

pub struct TopNOperator {
    input: Box<dyn Operator>,
    order_by: Vec<OrderByItem>,
    limit: u64,
    result: Vec<HeapRow>,
    position: usize,
    computed: bool,
}

impl TopNOperator {
    pub fn new(input: Box<dyn Operator>, order_by: Vec<OrderByItem>, limit: u64) -> Self {
        Self {
            input,
            order_by,
            limit,
            result: Vec::new(),
            position: 0,
            computed: false,
        }
    }

    fn compute(&mut self) {
        let limit = self.limit as usize;
        let order_by = &self.order_by;

        // Collect the descending flags so each HeapRow can carry them.
        let desc_flags: Vec<bool> = order_by.iter().map(|item| item.descending).collect();

        let mut heap: BinaryHeap<HeapRow> = BinaryHeap::with_capacity(limit + 1);

        while let Some(row) = self.input.next() {
            let sort_keys: Vec<Vec<u8>> = order_by
                .iter()
                .map(|item| eval_expr(&item.expr, &row).to_sort_key())
                .collect();
            heap.push(HeapRow {
                row,
                sort_keys,
                desc_flags: desc_flags.clone(),
            });
            if heap.len() > limit {
                heap.pop();
            }
        }

        // Drain heap and sort by the actual ORDER BY criteria.
        self.result = heap.into_vec();
        let order_by_clone = self.order_by.clone();
        self.result.sort_by(|a, b| {
            for item in &order_by_clone {
                let va = eval_expr(&item.expr, &a.row);
                let vb = eval_expr(&item.expr, &b.row);
                let ka = va.to_sort_key();
                let kb = vb.to_sort_key();
                let cmp = ka.cmp(&kb);
                if cmp != Ordering::Equal {
                    return if item.descending { cmp.reverse() } else { cmp };
                }
            }
            Ordering::Equal
        });

        self.computed = true;
    }
}

impl Operator for TopNOperator {
    fn next(&mut self) -> Option<Row> {
        if !self.computed {
            self.compute();
        }
        if self.position < self.result.len() {
            let row = self.result[self.position].row.clone();
            self.position += 1;
            Some(row)
        } else {
            None
        }
    }

    fn columns(&self) -> Vec<String> {
        if self.computed {
            self.result
                .first()
                .map(|hr| hr.row.columns.clone())
                .unwrap_or_default()
        } else {
            self.input.columns()
        }
    }
}

struct HeapRow {
    row: Row,
    sort_keys: Vec<Vec<u8>>,
    /// Per-key descending flags — controls the eviction order so the
    /// max-heap evicts the *worst* candidate for the desired sort direction.
    desc_flags: Vec<bool>,
}

impl PartialEq for HeapRow {
    fn eq(&self, other: &Self) -> bool {
        self.sort_keys == other.sort_keys
    }
}

impl Eq for HeapRow {}

impl PartialOrd for HeapRow {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for HeapRow {
    fn cmp(&self, other: &Self) -> Ordering {
        // BinaryHeap is a max-heap: `pop()` removes the element with the
        // greatest `Ord` value. We want to evict the row that would appear
        // *last* in the final sorted output, so:
        //
        //   ASC column  → the *largest* sort_key is worst  → natural order
        //   DESC column → the *smallest* sort_key is worst → reverse order
        //
        // This way `pop()` always removes the worst candidate.
        for (i, (sk_a, sk_b)) in self.sort_keys.iter().zip(&other.sort_keys).enumerate() {
            let desc = self.desc_flags.get(i).copied().unwrap_or(false);
            let cmp = if desc {
                sk_a.cmp(sk_b).reverse()
            } else {
                sk_a.cmp(sk_b)
            };
            if cmp != Ordering::Equal {
                return cmp;
            }
        }
        Ordering::Equal
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parser::ast::Expr;
    use crate::runtime::operators::scan::ScanOperator;
    use crate::types::Value;

    fn sample_rows() -> Vec<Row> {
        vec![
            Row {
                columns: vec!["score".into()],
                values: vec![Value::Int(70)],
            },
            Row {
                columns: vec!["score".into()],
                values: vec![Value::Int(95)],
            },
            Row {
                columns: vec!["score".into()],
                values: vec![Value::Int(80)],
            },
            Row {
                columns: vec!["score".into()],
                values: vec![Value::Int(60)],
            },
            Row {
                columns: vec!["score".into()],
                values: vec![Value::Int(90)],
            },
        ]
    }

    #[test]
    fn topn_ascending_limit_3() {
        let order_by = vec![OrderByItem {
            expr: Expr::Column {
                table: None,
                name: "score".into(),
            },
            descending: false,
        }];
        let scan = Box::new(ScanOperator::new(sample_rows()));
        let mut topn = TopNOperator::new(scan, order_by, 3);

        assert_eq!(topn.next().unwrap().values[0], Value::Int(60));
        assert_eq!(topn.next().unwrap().values[0], Value::Int(70));
        assert_eq!(topn.next().unwrap().values[0], Value::Int(80));
        assert!(topn.next().is_none());
    }

    #[test]
    fn topn_descending_limit_2() {
        let order_by = vec![OrderByItem {
            expr: Expr::Column {
                table: None,
                name: "score".into(),
            },
            descending: true,
        }];
        let scan = Box::new(ScanOperator::new(sample_rows()));
        let mut topn = TopNOperator::new(scan, order_by, 2);

        assert_eq!(topn.next().unwrap().values[0], Value::Int(95));
        assert_eq!(topn.next().unwrap().values[0], Value::Int(90));
        assert!(topn.next().is_none());
    }

    #[test]
    fn topn_limit_exceeds_rows() {
        let order_by = vec![OrderByItem {
            expr: Expr::Column {
                table: None,
                name: "score".into(),
            },
            descending: false,
        }];
        let scan = Box::new(ScanOperator::new(sample_rows()));
        let mut topn = TopNOperator::new(scan, order_by, 100);
        let mut count = 0;
        while topn.next().is_some() {
            count += 1;
        }
        assert_eq!(count, 5);
    }

    #[test]
    fn topn_empty_input() {
        let order_by = vec![OrderByItem {
            expr: Expr::Column {
                table: None,
                name: "score".into(),
            },
            descending: false,
        }];
        let scan = Box::new(ScanOperator::new(vec![]));
        let mut topn = TopNOperator::new(scan, order_by, 5);
        assert!(topn.next().is_none());
    }
}
