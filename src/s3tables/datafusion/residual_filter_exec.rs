// MinIO Rust Library for Amazon S3 Compatible Cloud Storage
// Copyright 2025 MinIO, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Residual filter executor for client-side filtering.
//!
//! This module provides residual filter handling infrastructure for filters that
//! couldn't be pushed to the server during query planning. These are filters involving:
//! - Scalar functions (UPPER, LOWER, etc.)
//! - Aggregate functions
//! - Subqueries
//! - Window functions
//! - Complex expressions
//!
//! # Implementation Strategy
//!
//! Rather than creating a new ExecutionPlan wrapper, residual filters are applied
//! by composing them with the existing DataFusion FilterExec during the table
//! provider's scan() method. This is simpler and more maintainable than implementing
//! a custom ExecutionPlan.

use datafusion::common::DFSchema;
use datafusion::execution::context::ExecutionProps;
use datafusion::logical_expr::Expr;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_expr::create_physical_expr;
use std::sync::Arc;

/// Represents a set of residual filter expressions that must be applied client-side.
///
/// Residual filters are expressions that cannot be pushed to the server and must
/// be evaluated locally after receiving data.
///
/// # Fields
/// * `expressions` - The residual filter expressions to apply
/// * `pushdown_ratio` - Estimated selectivity (0.0 = all filtered, 1.0 = none filtered)
#[derive(Debug, Clone)]
pub struct ResidualFilters {
    /// The filter expressions that couldn't be pushed to the server
    expressions: Vec<Expr>,
    /// Estimated data reduction from these filters (0.0-1.0 scale)
    /// Used for query planning but not enforced
    pushdown_ratio: f32,
}

impl ResidualFilters {
    /// Create a new residual filter set.
    ///
    /// # Arguments
    /// * `expressions` - The filter expressions
    ///
    /// # Returns
    /// A new `ResidualFilters` instance with default pushdown ratio of 1.0
    pub fn new(expressions: Vec<Expr>) -> Self {
        Self {
            expressions,
            pushdown_ratio: 1.0,
        }
    }

    /// Create a residual filter set with a specific pushdown ratio.
    ///
    /// # Arguments
    /// * `expressions` - The filter expressions
    /// * `pushdown_ratio` - Estimated selectivity (0.0-1.0)
    ///
    /// # Returns
    /// A new `ResidualFilters` instance
    pub fn with_ratio(expressions: Vec<Expr>, pushdown_ratio: f32) -> Self {
        Self {
            expressions,
            pushdown_ratio: pushdown_ratio.clamp(0.0, 1.0),
        }
    }

    /// Get the filter expressions.
    pub fn expressions(&self) -> &[Expr] {
        &self.expressions
    }

    /// Get the estimated pushdown ratio.
    pub fn pushdown_ratio(&self) -> f32 {
        self.pushdown_ratio
    }

    /// Check if there are any residual filters.
    pub fn is_empty(&self) -> bool {
        self.expressions.is_empty()
    }

    /// Convert residual filter expressions to physical expressions
    ///
    /// This converts logical expressions to physical expressions for execution.
    /// Returns a combined AND expression of all residual filters.
    ///
    /// # Arguments
    /// * `arrow_schema` - The Arrow schema of the input data for expression resolution
    /// * `props` - Execution properties for expression conversion
    ///
    /// # Returns
    /// A physical expression representing all residual filters ANDed together,
    /// or None if there are no expressions
    pub fn to_physical_expr(
        &self,
        arrow_schema: &datafusion::arrow::datatypes::Schema,
        props: &ExecutionProps,
    ) -> Result<Option<Arc<dyn PhysicalExpr>>, String> {
        if self.expressions.is_empty() {
            return Ok(None);
        }

        // Combine all expressions with AND using the and() helper
        let combined_expr = if self.expressions.len() == 1 {
            self.expressions[0].clone()
        } else {
            // Create AND expression from all filters using and() helper
            let mut combined = self.expressions[0].clone();
            for expr in &self.expressions[1..] {
                combined = combined.and(expr.clone());
            }
            combined
        };

        // Convert Arrow schema to DFSchema
        let df_schema = DFSchema::try_from(arrow_schema.clone())
            .map_err(|e| format!("Failed to convert schema: {}", e))?;

        // Convert logical expression to physical expression
        let physical_expr = create_physical_expr(&combined_expr, &df_schema, props)
            .map_err(|e| format!("Failed to create physical expression: {}", e))?;

        Ok(Some(physical_expr))
    }

    /// Combine multiple residual filters with AND logic
    ///
    /// Used to merge residual filters from different sources.
    pub fn combine(mut self, other: ResidualFilters) -> Self {
        self.expressions.extend(other.expressions);
        // Use minimum ratio as conservative estimate
        self.pushdown_ratio = self.pushdown_ratio.min(other.pushdown_ratio);
        self
    }
}

/// Represents a single residual filter expression with metadata.
///
/// Used for tracking and debugging individual residual filter expressions.
#[derive(Debug, Clone)]
pub struct ResidualFilter {
    /// The filter expression
    expression: Expr,
    /// Human-readable description of why this couldn't be pushed
    reason: String,
}

impl ResidualFilter {
    /// Create a new residual filter.
    ///
    /// # Arguments
    /// * `expression` - The filter expression
    /// * `reason` - Explanation for why it couldn't be pushed
    ///
    /// # Returns
    /// A new `ResidualFilter` instance
    pub fn new(expression: Expr, reason: impl Into<String>) -> Self {
        Self {
            expression,
            reason: reason.into(),
        }
    }

    /// Get the filter expression.
    pub fn expression(&self) -> &Expr {
        &self.expression
    }

    /// Get the reason why this couldn't be pushed.
    pub fn reason(&self) -> &str {
        &self.reason
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::logical_expr::{col, lit};

    // ============================================================================
    // ResidualFilters Tests
    // ============================================================================

    #[test]
    fn test_residual_filters_new() {
        let filters = vec![];
        let residual = ResidualFilters::new(filters);
        assert!(residual.is_empty());
        assert_eq!(residual.pushdown_ratio(), 1.0);
    }

    #[test]
    fn test_residual_filters_with_ratio() {
        let filters = vec![];
        let residual = ResidualFilters::with_ratio(filters, 0.5);
        assert!(residual.is_empty());
        assert_eq!(residual.pushdown_ratio(), 0.5);
    }

    #[test]
    fn test_residual_filters_ratio_clamping() {
        let filters = vec![];

        // Test lower bound
        let residual = ResidualFilters::with_ratio(filters.clone(), -1.0);
        assert_eq!(residual.pushdown_ratio(), 0.0);

        // Test upper bound
        let residual = ResidualFilters::with_ratio(filters, 2.0);
        assert_eq!(residual.pushdown_ratio(), 1.0);
    }

    #[test]
    fn test_residual_filters_with_expressions() {
        let expr1 = col("age").gt(lit(18));
        let expr2 = col("status").eq(lit("active"));
        let filters = vec![expr1, expr2];

        let residual = ResidualFilters::new(filters.clone());
        assert!(!residual.is_empty());
        assert_eq!(residual.expressions().len(), 2);
        assert_eq!(residual.pushdown_ratio(), 1.0);
    }

    #[test]
    fn test_residual_filters_expressions_accessor() {
        let expr1 = col("name").eq(lit("John"));
        let expr2 = col("city").eq(lit("NYC"));
        let filters = vec![expr1.clone(), expr2.clone()];

        let residual = ResidualFilters::new(filters);
        let retrieved = residual.expressions();

        assert_eq!(retrieved.len(), 2);
        // Verify we can access the expressions
        assert_eq!(retrieved[0].to_string(), expr1.to_string());
        assert_eq!(retrieved[1].to_string(), expr2.to_string());
    }

    #[test]
    fn test_residual_filters_single_expression() {
        let expr = col("price").lt(lit(100.0));
        let filters = vec![expr];

        let residual = ResidualFilters::new(filters);
        assert!(!residual.is_empty());
        assert_eq!(residual.expressions().len(), 1);
    }

    #[test]
    fn test_residual_filters_multiple_expressions() {
        let expressions: Vec<Expr> = (0..10)
            .map(|i| col(format!("col_{}", i)).eq(lit(i)))
            .collect();

        let residual = ResidualFilters::new(expressions);
        assert!(!residual.is_empty());
        assert_eq!(residual.expressions().len(), 10);
    }

    #[test]
    fn test_residual_filters_custom_ratios() {
        let expr = col("id").gt(lit(0));
        let filters = vec![expr];

        // Test various ratio values
        let ratios = vec![0.0, 0.25, 0.5, 0.75, 1.0];
        for ratio in ratios {
            let residual = ResidualFilters::with_ratio(filters.clone(), ratio);
            assert_eq!(residual.pushdown_ratio(), ratio);
        }
    }

    // ============================================================================
    // ResidualFilter Tests
    // ============================================================================

    #[test]
    fn test_residual_filter_new() {
        let expr = col("age").gt(lit(18));
        let reason = "scalar function in filter";
        let filter = ResidualFilter::new(expr.clone(), reason);

        assert_eq!(filter.expression(), &expr);
        assert_eq!(filter.reason(), reason);
    }

    #[test]
    fn test_residual_filter_reason_string_conversion() {
        let expr = col("status").eq(lit("active"));
        let reason_string = String::from("subquery not supported");
        let filter = ResidualFilter::new(expr, reason_string.clone());

        assert_eq!(filter.reason(), reason_string.as_str());
    }

    #[test]
    fn test_residual_filter_various_reasons() {
        let expr = col("value").gt(lit(0));
        let reasons = vec![
            "scalar function: UPPER()",
            "aggregate function: COUNT()",
            "window function: ROW_NUMBER()",
            "subquery in WHERE clause",
            "complex expression with nested functions",
        ];

        for reason in reasons {
            let filter = ResidualFilter::new(expr.clone(), reason);
            assert_eq!(filter.reason(), reason);
        }
    }

    #[test]
    fn test_residual_filter_expression_preservation() {
        let expr1 = col("a").eq(lit(1));
        let expr2 = col("b").lt(lit(2.0));
        let expr3 = col("c").gt(lit(3));

        let filter1 = ResidualFilter::new(expr1.clone(), "reason1");
        let filter2 = ResidualFilter::new(expr2.clone(), "reason2");
        let filter3 = ResidualFilter::new(expr3.clone(), "reason3");

        assert_eq!(filter1.expression(), &expr1);
        assert_eq!(filter2.expression(), &expr2);
        assert_eq!(filter3.expression(), &expr3);
    }

    // ============================================================================
    // Integration Tests: ResidualFilters + ResidualFilter
    // ============================================================================

    #[test]
    fn test_residual_filters_and_individual_filters() {
        let expr1 = col("status").eq(lit("active"));
        let expr2 = col("age").gt(lit(17));

        // Create ResidualFilters from expressions
        let residual_filters = ResidualFilters::new(vec![expr1.clone(), expr2.clone()]);

        // Create individual ResidualFilter entries
        let filter1 = ResidualFilter::new(expr1.clone(), "status check");
        let filter2 = ResidualFilter::new(expr2.clone(), "age verification");

        // Verify consistency
        assert_eq!(
            residual_filters.expressions()[0].to_string(),
            filter1.expression().to_string()
        );
        assert_eq!(
            residual_filters.expressions()[1].to_string(),
            filter2.expression().to_string()
        );
    }

    #[test]
    fn test_residual_filters_tracking_selectivity() {
        let expressions = vec![
            col("col1").eq(lit(1)),
            col("col2").eq(lit(2)),
            col("col3").eq(lit(3)),
        ];

        // High selectivity (few rows pass)
        let high_selectivity = ResidualFilters::with_ratio(expressions.clone(), 0.1);
        assert_eq!(high_selectivity.pushdown_ratio(), 0.1);

        // Medium selectivity (half rows pass)
        let medium_selectivity = ResidualFilters::with_ratio(expressions.clone(), 0.5);
        assert_eq!(medium_selectivity.pushdown_ratio(), 0.5);

        // Low selectivity (most rows pass)
        let low_selectivity = ResidualFilters::with_ratio(expressions, 0.9);
        assert_eq!(low_selectivity.pushdown_ratio(), 0.9);
    }

    #[test]
    fn test_residual_filters_empty_vs_nonempty() {
        let empty = ResidualFilters::new(vec![]);
        assert!(empty.is_empty());
        assert_eq!(empty.expressions().len(), 0);

        let non_empty = ResidualFilters::new(vec![col("x").eq(lit(1))]);
        assert!(!non_empty.is_empty());
        assert_eq!(non_empty.expressions().len(), 1);
    }

    #[test]
    fn test_residual_filters_cloneable() {
        let expr = col("value").gt(lit(10));
        let original = ResidualFilters::new(vec![expr]);

        let cloned = original.clone();
        assert_eq!(cloned.expressions().len(), original.expressions().len());
        assert_eq!(cloned.pushdown_ratio(), original.pushdown_ratio());
    }

    #[test]
    fn test_residual_filter_cloneable() {
        let expr = col("status").eq(lit("pending"));
        let original = ResidualFilter::new(expr, "test reason");

        let cloned = original.clone();
        assert_eq!(cloned.expression(), original.expression());
        assert_eq!(cloned.reason(), original.reason());
    }

    // ============================================================================
    // ResidualFilters Conversion and Combination Tests
    // ============================================================================

    #[test]
    fn test_residual_filters_combine_empty() {
        let filter1 = ResidualFilters::new(vec![col("a").eq(lit(1))]);
        let filter2 = ResidualFilters::new(vec![]);

        let combined = filter1.combine(filter2);
        assert_eq!(combined.expressions().len(), 1);
    }

    #[test]
    fn test_residual_filters_combine_multiple() {
        let expr1 = col("a").eq(lit(1));
        let expr2 = col("b").eq(lit(2));
        let expr3 = col("c").eq(lit(3));

        let filter1 = ResidualFilters::new(vec![expr1, expr2]);
        let filter2 = ResidualFilters::new(vec![expr3]);

        let combined = filter1.combine(filter2);
        assert_eq!(combined.expressions().len(), 3);
    }

    #[test]
    fn test_residual_filters_combine_ratios() {
        let expr1 = col("a").eq(lit(1));
        let expr2 = col("b").eq(lit(2));

        let filter1 = ResidualFilters::with_ratio(vec![expr1], 0.8);
        let filter2 = ResidualFilters::with_ratio(vec![expr2], 0.5);

        let combined = filter1.combine(filter2);
        // Should use minimum ratio (conservative estimate)
        assert_eq!(combined.pushdown_ratio(), 0.5);
    }

    #[test]
    fn test_residual_filters_to_physical_expr_empty() {
        use datafusion::arrow::datatypes::{DataType, Field};

        let filters = ResidualFilters::new(vec![]);
        let arrow_schema = datafusion::arrow::datatypes::Schema::new(vec![Field::new(
            "x",
            DataType::Int32,
            false,
        )]);
        let props = ExecutionProps::new();

        let result = filters.to_physical_expr(&arrow_schema, &props);
        assert!(result.is_ok());
        assert!(result.unwrap().is_none());
    }

    #[test]
    fn test_residual_filters_to_physical_expr_with_single_expression() {
        use datafusion::arrow::datatypes::{DataType, Field};

        let filters = ResidualFilters::new(vec![col("x").gt(lit(10))]);
        let arrow_schema = datafusion::arrow::datatypes::Schema::new(vec![Field::new(
            "x",
            DataType::Int32,
            false,
        )]);
        let props = ExecutionProps::new();

        let result = filters.to_physical_expr(&arrow_schema, &props);
        assert!(result.is_ok());
        let physical_expr = result.unwrap();
        assert!(physical_expr.is_some());
    }

    #[test]
    fn test_residual_filters_to_physical_expr_with_multiple_expressions() {
        use datafusion::arrow::datatypes::{DataType, Field};

        let expr1 = col("x").gt(lit(10));
        let expr2 = col("y").lt(lit(20));
        let filters = ResidualFilters::new(vec![expr1, expr2]);
        let arrow_schema = datafusion::arrow::datatypes::Schema::new(vec![
            Field::new("x", DataType::Int32, false),
            Field::new("y", DataType::Int32, false),
        ]);
        let props = ExecutionProps::new();

        let result = filters.to_physical_expr(&arrow_schema, &props);
        assert!(result.is_ok());
        let physical_expr = result.unwrap();
        assert!(physical_expr.is_some());
    }
}
