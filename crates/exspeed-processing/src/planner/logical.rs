use crate::parser::ast::*;

#[derive(Debug, Clone)]
pub enum LogicalPlan {
    Scan { stream: String, alias: Option<String> },
    ExternalScan { connection: String, table: String, alias: String, driver: Option<String> },
    Filter { input: Box<LogicalPlan>, predicate: Expr },
    Project { input: Box<LogicalPlan>, items: Vec<SelectItem> },
    Join { left: Box<LogicalPlan>, right: Box<LogicalPlan>, on: Expr, join_type: JoinType },
    Aggregate { input: Box<LogicalPlan>, group_by: Vec<Expr>, select_items: Vec<SelectItem> },
    Sort { input: Box<LogicalPlan>, order_by: Vec<OrderByItem> },
    Limit { input: Box<LogicalPlan>, limit: Option<u64>, offset: Option<u64> },
}
