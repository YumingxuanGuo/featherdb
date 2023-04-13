use crate::error::Result;
use crate::sql::types::{Expression, Value};
use super::Node;

/// A plan optimizer.
pub trait Optimizer {
    /// Optimizes a plan node, consuming it.
    fn optimize(&self, node: Node) -> Result<Node>;
}

/// A constant folding optimizer, which replaces constant expressions with their evaluated value,
/// to prevent it from being re-evaluated over and over again during plan execution.
pub struct ConstantFolder;

impl Optimizer for ConstantFolder {
    fn optimize(&self, node: Node) -> Result<Node> {
        node.transform(
            &|n| Ok(n),
            &|n| {
                n.transform_expressions(
                    &|expr| {
                        if !expr.contains(&|e| matches!(e, Expression::Field(_, _))) {
                            Ok(Expression::Constant(expr.evaluate(None)?))
                        } else {
                            Ok(expr)
                        }
                    },
                    &|expr| Ok(expr),
                )
            },
        )
    }
}

/// A filter pushdown optimizer, which moves filter predicates into or closer to the source node.
pub struct FilterPushdown;

impl Optimizer for FilterPushdown {
    fn optimize(&self, node: Node) -> Result<Node> {
        node.transform(
            &|n| match n {
                Node::Filter { mut source, predicate } => {
                    // We don't replace the filter node here, since doing so would cause transform()
                    // to skip the source as it won't reapply the transform to the "same" node.
                    // We leave a noop filter node instead, which will be cleaned up by NoopCleaner.
                    if let Some(remainder) = self.pushdown(predicate, &mut *source) {
                        Ok(Node::Filter { source, predicate: remainder })
                    } else {
                        Ok(Node::Filter {
                            source,
                            predicate: Expression::Constant(Value::Boolean(true)),
                        })
                    }
                },
                Node::NestedLoopJoin {
                    mut left,
                    left_size,
                    mut right,
                    predicate: Some(predicate),
                    outer,
                } => {
                    let predicate = self.pushdown_join(predicate, &mut left, &mut right, left_size);
                    Ok(Node::NestedLoopJoin { left, left_size, right, predicate, outer })
                }
                n => Ok(n),
            },
            &|n| Ok(n),
        )
    }
}

impl FilterPushdown {
    /// Attempts to push an expression down into a target node, returns any remaining expression.
    fn pushdown(&self, mut expression: Expression, target: &mut Node) -> Option<Expression> {
        todo!()
    }

    /// Attempts to partition a join predicate and push parts of it down into either source,
    /// returning any remaining expression.
    fn pushdown_join(
        &self,
        predicate: Expression,
        left: &mut Node,
        right: &mut Node,
        boundary: usize,
    ) -> Option<Expression> {
        todo!()
    }
}