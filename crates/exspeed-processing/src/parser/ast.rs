/// ExQL AST types — Exspeed-owned, no dependency on sqlparser.
/// The parser (Task 3) transforms sqlparser's AST into these types.

// ---------------------------------------------------------------------------
// Top-level statements
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub enum ExqlStatement {
    Query(QueryExpr),
    CreateStream {
        name: String,
        query: QueryExpr,
        emit: EmitMode,
    },
    DropStream(String),
}

#[derive(Debug, Clone)]
pub enum EmitMode {
    Changes,
}

// ---------------------------------------------------------------------------
// Query expression
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub struct QueryExpr {
    pub ctes: Vec<Cte>,
    pub select: Vec<SelectItem>,
    pub from: FromClause,
    pub joins: Vec<JoinClause>,
    pub filter: Option<Expr>,
    pub group_by: Vec<Expr>,
    pub order_by: Vec<OrderByItem>,
    pub limit: Option<u64>,
    pub offset: Option<u64>,
}

// ---------------------------------------------------------------------------
// Supporting types
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub struct Cte {
    pub name: String,
    pub query: Box<QueryExpr>,
}

#[derive(Debug, Clone)]
pub struct SelectItem {
    pub expr: Expr,
    pub alias: Option<String>,
}

#[derive(Debug, Clone)]
pub enum FromClause {
    Stream {
        name: String,
        alias: Option<String>,
    },
    Subquery {
        query: Box<QueryExpr>,
        alias: String,
    },
    External {
        connection: String,
        table: String,
        alias: Option<String>,
        driver: Option<String>,
    },
}

#[derive(Debug, Clone)]
pub struct JoinClause {
    pub join_type: JoinType,
    pub source: FromClause,
    pub on: Expr,
}

#[derive(Debug, Clone)]
pub enum JoinType {
    Inner,
    Left,
}

#[derive(Debug, Clone)]
pub struct OrderByItem {
    pub expr: Expr,
    pub descending: bool,
}

// ---------------------------------------------------------------------------
// Core expression tree (recursive via Box)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub enum Expr {
    Column {
        table: Option<String>,
        name: String,
    },
    Literal(LiteralValue),
    BinaryOp {
        left: Box<Expr>,
        op: BinaryOperator,
        right: Box<Expr>,
    },
    UnaryOp {
        op: UnaryOperator,
        expr: Box<Expr>,
    },
    Function {
        name: String,
        args: Vec<Expr>,
    },
    JsonAccess {
        expr: Box<Expr>,
        field: String,
        as_text: bool,
    },
    Cast {
        expr: Box<Expr>,
        to_type: String,
    },
    Case {
        conditions: Vec<(Expr, Expr)>,
        else_val: Option<Box<Expr>>,
    },
    Subquery(Box<QueryExpr>),
    IsNull {
        expr: Box<Expr>,
        negated: bool,
    },
    InList {
        expr: Box<Expr>,
        list: Vec<Expr>,
        negated: bool,
    },
    Between {
        expr: Box<Expr>,
        low: Box<Expr>,
        high: Box<Expr>,
        negated: bool,
    },
    Wildcard {
        table: Option<String>,
    },
    Aggregate {
        func: AggregateFunc,
        expr: Box<Expr>,
        distinct: bool,
    },
    Interval {
        value: String,
    },
}

// ---------------------------------------------------------------------------
// Leaf / operator types
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub enum LiteralValue {
    Int(i64),
    Float(f64),
    String(String),
    Bool(bool),
    Null,
}

#[derive(Debug, Clone)]
pub enum BinaryOperator {
    Eq,
    Neq,
    Lt,
    Gt,
    Lte,
    Gte,
    And,
    Or,
    Add,
    Sub,
    Mul,
    Div,
    Mod,
    Like,
}

#[derive(Debug, Clone)]
pub enum UnaryOperator {
    Not,
    Neg,
}

#[derive(Debug, Clone)]
pub enum AggregateFunc {
    Count,
    Sum,
    Avg,
    Min,
    Max,
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn recursive_box_expr() {
        // Build a BinaryOp with Column children to verify Box<Expr> recursion.
        let expr = Expr::BinaryOp {
            left: Box::new(Expr::Column {
                table: Some("t".into()),
                name: "a".into(),
            }),
            op: BinaryOperator::Eq,
            right: Box::new(Expr::Column {
                table: None,
                name: "b".into(),
            }),
        };

        // Nest further to prove arbitrary depth works.
        let nested = Expr::BinaryOp {
            left: Box::new(expr.clone()),
            op: BinaryOperator::And,
            right: Box::new(Expr::Literal(LiteralValue::Bool(true))),
        };

        // Just assert we can debug-print without stack overflow.
        let dbg = format!("{nested:?}");
        assert!(dbg.contains("BinaryOp"));
        assert!(dbg.contains("Column"));
    }

    #[test]
    fn query_expr_all_fields() {
        let query = QueryExpr {
            ctes: vec![Cte {
                name: "cte1".into(),
                query: Box::new(simple_query()),
            }],
            select: vec![
                SelectItem {
                    expr: Expr::Wildcard { table: None },
                    alias: None,
                },
                SelectItem {
                    expr: Expr::Column {
                        table: Some("t".into()),
                        name: "id".into(),
                    },
                    alias: Some("the_id".into()),
                },
            ],
            from: FromClause::Stream {
                name: "events".into(),
                alias: Some("e".into()),
            },
            joins: vec![JoinClause {
                join_type: JoinType::Left,
                source: FromClause::External {
                    connection: "pg".into(),
                    table: "users".into(),
                    alias: Some("u".into()),
                    driver: Some("postgres".into()),
                },
                on: Expr::BinaryOp {
                    left: Box::new(Expr::Column {
                        table: Some("e".into()),
                        name: "user_id".into(),
                    }),
                    op: BinaryOperator::Eq,
                    right: Box::new(Expr::Column {
                        table: Some("u".into()),
                        name: "id".into(),
                    }),
                },
            }],
            filter: Some(Expr::IsNull {
                expr: Box::new(Expr::Column {
                    table: None,
                    name: "deleted_at".into(),
                }),
                negated: true,
            }),
            group_by: vec![Expr::Column {
                table: None,
                name: "status".into(),
            }],
            order_by: vec![OrderByItem {
                expr: Expr::Column {
                    table: None,
                    name: "created_at".into(),
                },
                descending: true,
            }],
            limit: Some(100),
            offset: Some(10),
        };

        let dbg = format!("{query:?}");
        assert!(dbg.contains("events"));
        assert!(dbg.contains("cte1"));
        assert!(dbg.contains("the_id"));
        assert!(dbg.contains("Left"));
        assert!(dbg.contains("deleted_at"));
    }

    /// Helper: minimal valid QueryExpr for embedding in CTEs / subqueries.
    fn simple_query() -> QueryExpr {
        QueryExpr {
            ctes: vec![],
            select: vec![SelectItem {
                expr: Expr::Wildcard { table: None },
                alias: None,
            }],
            from: FromClause::Stream {
                name: "s".into(),
                alias: None,
            },
            joins: vec![],
            filter: None,
            group_by: vec![],
            order_by: vec![],
            limit: None,
            offset: None,
        }
    }
}
