use crate::parser::ast::{Expr, SelectItem};
use std::collections::HashSet;

/// Summary of which scan-level columns an expression tree references.
///
/// `virtual_cols` is a subset of the scan's virtual columns (offset, timestamp,
/// key, subject). `payload_referenced` is true if the payload column is touched
/// either directly (bare `payload`) or via any `JsonAccess` chain rooted at it.
/// `select_star` is true if a `*` projection demands everything.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct ColumnSet {
    pub virtual_cols: HashSet<String>,
    pub payload_referenced: bool,
    pub select_star: bool,
}

impl ColumnSet {
    pub fn needs_everything() -> Self {
        let mut virtual_cols = HashSet::new();
        virtual_cols.insert("offset".into());
        virtual_cols.insert("timestamp".into());
        virtual_cols.insert("key".into());
        virtual_cols.insert("subject".into());
        Self {
            virtual_cols,
            payload_referenced: true,
            select_star: true,
        }
    }

    pub fn merge(&mut self, other: &ColumnSet) {
        self.virtual_cols.extend(other.virtual_cols.iter().cloned());
        self.payload_referenced |= other.payload_referenced;
        self.select_star |= other.select_star;
    }
}

const VIRTUAL_COLS: &[&str] = &["offset", "timestamp", "key", "subject"];

/// Returns the innermost column name referenced by a (possibly chained)
/// `JsonAccess` expression. Returns `None` if the root is not a `Column`.
fn json_access_root_column(mut expr: &Expr) -> Option<&str> {
    loop {
        match expr {
            Expr::JsonAccess { expr: inner, .. } => {
                expr = inner;
            }
            Expr::Column { name, .. } => return Some(name.as_str()),
            _ => return None,
        }
    }
}

/// Walk an expression and populate `out` with the scan-level columns it
/// references. Alias qualifiers on `Column` are ignored — this collector is
/// scan-local and its caller is responsible for filtering out unrelated aliases.
pub fn collect_columns(expr: &Expr, out: &mut ColumnSet) {
    match expr {
        Expr::Column { name, .. } => {
            if VIRTUAL_COLS.contains(&name.as_str()) {
                out.virtual_cols.insert(name.clone());
            } else if name == "payload" {
                out.payload_referenced = true;
            }
        }
        Expr::Literal(_) => {}
        Expr::BinaryOp { left, right, .. } => {
            collect_columns(left, out);
            collect_columns(right, out);
        }
        Expr::UnaryOp { expr, .. } => collect_columns(expr, out),
        Expr::Function { args, .. } => {
            for a in args {
                collect_columns(a, out);
            }
        }
        Expr::JsonAccess { expr, .. } => {
            match json_access_root_column(expr) {
                Some("payload") => out.payload_referenced = true,
                Some(other) if VIRTUAL_COLS.contains(&other) => {
                    out.virtual_cols.insert(other.to_string());
                }
                _ => {}
            }
            collect_columns(expr, out);
        }
        Expr::Cast { expr, .. } => collect_columns(expr, out),
        Expr::Case { conditions, else_val } => {
            for (c, r) in conditions {
                collect_columns(c, out);
                collect_columns(r, out);
            }
            if let Some(e) = else_val {
                collect_columns(e, out);
            }
        }
        Expr::Subquery(_) => {
            out.merge(&ColumnSet::needs_everything());
        }
        Expr::IsNull { expr, .. } => collect_columns(expr, out),
        Expr::InList { expr, list, .. } => {
            collect_columns(expr, out);
            for e in list {
                collect_columns(e, out);
            }
        }
        Expr::Between { expr, low, high, .. } => {
            collect_columns(expr, out);
            collect_columns(low, out);
            collect_columns(high, out);
        }
        Expr::Wildcard { .. } => {
            out.merge(&ColumnSet::needs_everything());
        }
        Expr::Aggregate { expr, .. } => {
            // `count(*)` is idiomatic for "count rows"; the Wildcard inside does
            // NOT mean "all columns" at the scan level. Skip the recursion in that
            // case so count(*) doesn't incorrectly flip `select_star` on the scan.
            if !matches!(expr.as_ref(), Expr::Wildcard { .. }) {
                collect_columns(expr, out);
            }
        }
        Expr::Interval { .. } => {}
    }
}

/// Walk a list of projection items (the `SELECT x, y, *` items).
/// `SelectItem` is a struct `{ expr, alias }`; a `*` lives inside `.expr` as
/// `Expr::Wildcard`, which `collect_columns` already promotes to needs-everything.
pub fn collect_from_projection(items: &[SelectItem], out: &mut ColumnSet) {
    for item in items {
        collect_columns(&item.expr, out);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parser::ast::{BinaryOperator, Expr, LiteralValue};

    fn col(name: &str) -> Expr {
        Expr::Column { table: None, name: name.into() }
    }

    fn cs() -> ColumnSet { ColumnSet::default() }

    #[test]
    fn bare_virtual_column() {
        let mut out = cs();
        collect_columns(&col("offset"), &mut out);
        assert!(out.virtual_cols.contains("offset"));
        assert!(!out.payload_referenced);
    }

    #[test]
    fn bare_payload_column() {
        let mut out = cs();
        collect_columns(&col("payload"), &mut out);
        assert!(out.payload_referenced);
        assert!(out.virtual_cols.is_empty());
    }

    #[test]
    fn unknown_column_is_ignored() {
        let mut out = cs();
        collect_columns(&col("foreign_col"), &mut out);
        assert!(out.virtual_cols.is_empty());
        assert!(!out.payload_referenced);
    }

    #[test]
    fn json_access_on_payload() {
        let e = Expr::JsonAccess {
            expr: Box::new(col("payload")),
            field: "status".into(),
            as_text: true,
        };
        let mut out = cs();
        collect_columns(&e, &mut out);
        assert!(out.payload_referenced);
    }

    #[test]
    fn chained_json_access_on_payload() {
        let e = Expr::JsonAccess {
            expr: Box::new(Expr::JsonAccess {
                expr: Box::new(col("payload")),
                field: "user".into(),
                as_text: false,
            }),
            field: "name".into(),
            as_text: true,
        };
        let mut out = cs();
        collect_columns(&e, &mut out);
        assert!(out.payload_referenced);
    }

    #[test]
    fn binary_op_mixes_columns() {
        let e = Expr::BinaryOp {
            left: Box::new(col("offset")),
            op: BinaryOperator::Gt,
            right: Box::new(Expr::Literal(LiteralValue::Int(5))),
        };
        let mut out = cs();
        collect_columns(&e, &mut out);
        assert!(out.virtual_cols.contains("offset"));
    }

    #[test]
    fn wildcard_needs_everything() {
        let mut out = cs();
        collect_columns(&Expr::Wildcard { table: None }, &mut out);
        assert!(out.select_star);
        assert!(out.payload_referenced);
        assert!(out.virtual_cols.contains("offset"));
    }

    #[test]
    fn projection_with_star() {
        let mut out = cs();
        let items = [SelectItem {
            expr: Expr::Wildcard { table: None },
            alias: None,
        }];
        collect_from_projection(&items, &mut out);
        assert!(out.select_star);
        assert!(out.payload_referenced);
    }

    #[test]
    fn merge_is_commutative_and_additive() {
        let mut a = cs();
        a.virtual_cols.insert("offset".into());
        let mut b = cs();
        b.payload_referenced = true;
        a.merge(&b);
        assert!(a.virtual_cols.contains("offset"));
        assert!(a.payload_referenced);
    }

    #[test]
    fn needs_everything_is_saturated() {
        let e = ColumnSet::needs_everything();
        assert!(e.select_star);
        assert!(e.payload_referenced);
        assert_eq!(e.virtual_cols.len(), 4);
    }

    #[test]
    fn count_star_does_not_flip_select_star() {
        let e = Expr::Aggregate {
            func: crate::parser::ast::AggregateFunc::Count,
            expr: Box::new(Expr::Wildcard { table: None }),
            distinct: false,
        };
        let mut out = cs();
        collect_columns(&e, &mut out);
        assert!(!out.select_star);
        assert!(!out.payload_referenced);
        assert!(out.virtual_cols.is_empty());
    }

    #[test]
    fn aggregate_over_json_access_marks_payload() {
        // SUM(payload->>'amount')
        let e = Expr::Aggregate {
            func: crate::parser::ast::AggregateFunc::Sum,
            expr: Box::new(Expr::JsonAccess {
                expr: Box::new(col("payload")),
                field: "amount".into(),
                as_text: true,
            }),
            distinct: false,
        };
        let mut out = cs();
        collect_columns(&e, &mut out);
        assert!(out.payload_referenced);
    }
}
