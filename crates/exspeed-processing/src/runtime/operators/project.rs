use super::Operator;
use crate::parser::ast::{AggregateFunc, Expr, SelectItem};
use crate::runtime::eval::eval_expr;
use crate::types::{Row, Value};

/// Evaluates SELECT expressions against each input row to produce output columns.
pub struct ProjectOperator {
    input: Box<dyn Operator>,
    items: Vec<SelectItem>,
    input_columns: Vec<String>,
}

impl ProjectOperator {
    pub fn new(input: Box<dyn Operator>, items: Vec<SelectItem>) -> Self {
        let input_columns = input.columns();
        Self {
            input,
            items,
            input_columns,
        }
    }
}

impl Operator for ProjectOperator {
    fn next(&mut self) -> Option<Row> {
        let input_row = self.input.next()?;
        let mut columns = Vec::new();
        let mut values = Vec::new();

        for item in &self.items {
            match &item.expr {
                Expr::Wildcard { table: None } => {
                    columns.extend(input_row.columns.clone());
                    values.extend(input_row.values.clone());
                }
                Expr::Wildcard { table: Some(t) } => {
                    let prefix = format!("{t}.");
                    for (i, col) in input_row.columns.iter().enumerate() {
                        if col.starts_with(&prefix) || !col.contains('.') {
                            columns.push(col.clone());
                            values.push(input_row.values[i].clone());
                        }
                    }
                }
                expr => {
                    let name = item
                        .alias
                        .clone()
                        .unwrap_or_else(|| derive_column_name(expr));
                    // For aggregate expressions, the value has already been
                    // computed by an upstream AggregateOperator and stored
                    // in the row under the column name (alias or derived).
                    // Look it up directly instead of re-evaluating.
                    let val = if matches!(expr, Expr::Aggregate { .. }) {
                        input_row.get(&name).cloned().unwrap_or_else(|| {
                            // Fallback: try the default aggregate column name
                            let default_name = derive_column_name(expr);
                            input_row.get(&default_name).cloned().unwrap_or(Value::Null)
                        })
                    } else {
                        eval_expr(expr, &input_row)
                    };
                    columns.push(name);
                    values.push(val);
                }
            }
        }

        Some(Row { columns, values })
    }

    fn columns(&self) -> Vec<String> {
        let mut cols = Vec::new();
        for item in &self.items {
            match &item.expr {
                Expr::Wildcard { table: None } => {
                    cols.extend(self.input_columns.clone());
                }
                Expr::Wildcard { table: Some(t) } => {
                    let prefix = format!("{t}.");
                    for col in &self.input_columns {
                        if col.starts_with(&prefix) || !col.contains('.') {
                            cols.push(col.clone());
                        }
                    }
                }
                expr => {
                    let name = item
                        .alias
                        .clone()
                        .unwrap_or_else(|| derive_column_name(expr));
                    cols.push(name);
                }
            }
        }
        cols
    }
}

/// Derive a human-readable column name from an expression.
fn derive_column_name(expr: &Expr) -> String {
    match expr {
        Expr::Column { name, .. } => name.clone(),
        Expr::JsonAccess { field, .. } => field.clone(),
        Expr::Aggregate { func, .. } => match func {
            AggregateFunc::Count => "count".into(),
            AggregateFunc::Sum => "sum".into(),
            AggregateFunc::Avg => "avg".into(),
            AggregateFunc::Min => "min".into(),
            AggregateFunc::Max => "max".into(),
        },
        Expr::Function { name, .. } => name.to_lowercase(),
        Expr::Cast { expr, .. } => derive_column_name(expr),
        _ => "?column?".into(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parser::ast::LiteralValue;
    use crate::runtime::operators::scan::ScanOperator;
    use crate::types::Value;

    fn sample_rows() -> Vec<Row> {
        vec![
            Row {
                columns: vec!["id".into(), "name".into(), "score".into()],
                values: vec![
                    Value::Int(1),
                    Value::Text("Alice".into()),
                    Value::Int(95),
                ],
            },
            Row {
                columns: vec!["id".into(), "name".into(), "score".into()],
                values: vec![
                    Value::Int(2),
                    Value::Text("Bob".into()),
                    Value::Int(80),
                ],
            },
        ]
    }

    #[test]
    fn project_select_columns() {
        // SELECT name, score
        let items = vec![
            SelectItem {
                expr: Expr::Column {
                    table: None,
                    name: "name".into(),
                },
                alias: None,
            },
            SelectItem {
                expr: Expr::Column {
                    table: None,
                    name: "score".into(),
                },
                alias: None,
            },
        ];

        let scan = Box::new(ScanOperator::new(sample_rows()));
        let mut project = ProjectOperator::new(scan, items);
        assert_eq!(project.columns(), vec!["name", "score"]);

        let r1 = project.next().unwrap();
        assert_eq!(r1.columns, vec!["name", "score"]);
        assert_eq!(r1.values, vec![Value::Text("Alice".into()), Value::Int(95)]);

        let r2 = project.next().unwrap();
        assert_eq!(r2.values, vec![Value::Text("Bob".into()), Value::Int(80)]);

        assert!(project.next().is_none());
    }

    #[test]
    fn project_wildcard() {
        let items = vec![SelectItem {
            expr: Expr::Wildcard { table: None },
            alias: None,
        }];

        let scan = Box::new(ScanOperator::new(sample_rows()));
        let mut project = ProjectOperator::new(scan, items);

        let r1 = project.next().unwrap();
        assert_eq!(r1.columns, vec!["id", "name", "score"]);
        assert_eq!(r1.values.len(), 3);
    }

    #[test]
    fn project_with_alias() {
        // SELECT name AS full_name
        let items = vec![SelectItem {
            expr: Expr::Column {
                table: None,
                name: "name".into(),
            },
            alias: Some("full_name".into()),
        }];

        let scan = Box::new(ScanOperator::new(sample_rows()));
        let mut project = ProjectOperator::new(scan, items);

        let r1 = project.next().unwrap();
        assert_eq!(r1.columns, vec!["full_name"]);
        assert_eq!(r1.values, vec![Value::Text("Alice".into())]);
    }

    #[test]
    fn project_with_literal_expr() {
        // SELECT 42 AS answer
        let items = vec![SelectItem {
            expr: Expr::Literal(LiteralValue::Int(42)),
            alias: Some("answer".into()),
        }];

        let scan = Box::new(ScanOperator::new(sample_rows()));
        let mut project = ProjectOperator::new(scan, items);

        let r1 = project.next().unwrap();
        assert_eq!(r1.columns, vec!["answer"]);
        assert_eq!(r1.values, vec![Value::Int(42)]);
    }
}
