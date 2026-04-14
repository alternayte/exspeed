use crate::parser::ast::*;

#[derive(Debug, Clone)]
pub enum PhysicalPlan {
    SeqScan { stream: String, alias: Option<String> },
    ExternalScan { connection: String, table: String, alias: String, driver: Option<String> },
    Filter { input: Box<PhysicalPlan>, predicate: Expr },
    Project { input: Box<PhysicalPlan>, items: Vec<SelectItem> },
    HashJoin { left: Box<PhysicalPlan>, right: Box<PhysicalPlan>, on: Expr, join_type: JoinType },
    HashAggregate { input: Box<PhysicalPlan>, group_by: Vec<Expr>, select_items: Vec<SelectItem> },
    Sort { input: Box<PhysicalPlan>, order_by: Vec<OrderByItem> },
    Limit { input: Box<PhysicalPlan>, limit: Option<u64>, offset: Option<u64> },
}
