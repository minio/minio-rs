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

//! DataFusion filter pushdown support for MinIO S3 Tables.
//!
//! This module provides utilities for filter classification and conversion to Iceberg format
//! for server-side query evaluation. It enables integration of MinIO query pushdown
//! with DataFusion's query optimization.
//!
//! # Integration Points
//!
//! Filter pushdown can be integrated at two levels:
//!
//! 1. **TableProvider Level** (Recommended)
//!    - Implement `supports_filters_pushdown()` to indicate which filters can be pushed
//!    - Override `scan()` to extract filters from the logical plan
//!    - Call `plan_table_scan()` with filters before creating execution plan
//!
//! 2. **Optimizer Rule Level** (Advanced)
//!    - Register a custom `PhysicalOptimizerRule` with SessionState
//!    - Analyze physical plan to identify pushable filters
//!    - Inject filter context before execution
//!
//! # Example
//!
//! ```ignore
//! use datafusion::logical_expr::{col, lit};
//! use minio::s3tables::datafusion::MinioFilterPushdownSupport;
//!
//! // Create a filter expression
//! let expr = col("age").gt(lit(18));
//!
//! // Check if it can be pushed down
//! if MinioFilterPushdownSupport::can_push_down(&expr) {
//!     let json = MinioFilterPushdownSupport::filters_to_iceberg_json(&[expr])?;
//!     // Send json to MinIO plan_table_scan() API
//! }
//! ```

use crate::s3tables::filter::Filter;
use datafusion::logical_expr::Expr;

/// Filter pushdown utilities for MinIO S3 Tables integration with DataFusion.
///
/// This struct provides helper methods for analyzing and converting DataFusion filter expressions
/// to Iceberg format suitable for server-side evaluation by MinIO.
pub struct MinioFilterPushdownSupport;

impl MinioFilterPushdownSupport {
    /// Analyze a predicate expression to determine if it can be pushed down.
    ///
    /// This utility can be called from `TableProvider::supports_filters_pushdown()`
    /// to indicate which filters can be pushed to MinIO.
    ///
    /// # Arguments
    ///
    /// * `expr` - DataFusion expression to analyze
    ///
    /// # Returns
    ///
    /// `true` if the expression can be converted to an Iceberg filter, `false` otherwise.
    pub fn can_push_down(expr: &Expr) -> bool {
        super::filter_translator::expr_to_filter(expr).is_some()
    }

    /// Extract filters from a list, separating pushable and residual filters.
    ///
    /// Separates a list of DataFusion filter expressions into two categories:
    /// - **Pushable**: Can be converted to Iceberg filters and sent to MinIO
    /// - **Residual**: Must be applied by DataFusion after data returns from MinIO
    ///
    /// # Arguments
    ///
    /// * `filters` - List of DataFusion filter expressions
    ///
    /// # Returns
    ///
    /// A tuple of `(pushable_filters, residual_filters)`
    pub fn extract_pushable_filters(filters: &[Expr]) -> (Vec<Expr>, Vec<Expr>) {
        let mut pushable = Vec::new();
        let mut residual = Vec::new();

        for filter in filters {
            if Self::can_push_down(filter) {
                pushable.push(filter.clone());
            } else {
                residual.push(filter.clone());
            }
        }

        (pushable, residual)
    }

    /// Convert pushable filters to Iceberg filter JSON for `plan_table_scan()` API.
    ///
    /// Combines multiple DataFusion filter expressions into a single Iceberg filter
    /// JSON representation. Multiple filters are combined using AND logic.
    ///
    /// # Arguments
    ///
    /// * `filters` - List of DataFusion filter expressions (should be pushable)
    ///
    /// # Returns
    ///
    /// `Some(json)` if conversion succeeds, `None` if the filter list is empty or
    /// cannot be converted.
    pub fn filters_to_iceberg_json(filters: &[Expr]) -> Option<serde_json::Value> {
        if filters.is_empty() {
            return None;
        }

        // Convert first filter
        let mut combined: Filter = super::filter_translator::expr_to_filter(&filters[0])?;

        // Combine remaining filters with AND
        for filter_expr in &filters[1..] {
            let next_filter = super::filter_translator::expr_to_filter(filter_expr)?;
            combined = combined.and(next_filter);
        }

        Some(combined.to_json())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::logical_expr::{col, lit};

    #[test]
    fn test_extract_pushable_filters_empty() {
        let filters: Vec<Expr> = vec![];

        let (pushable, residual) = MinioFilterPushdownSupport::extract_pushable_filters(&filters);

        assert_eq!(pushable.len(), 0);
        assert_eq!(residual.len(), 0);
    }

    #[test]
    fn test_filters_to_iceberg_json_empty() {
        let filters: Vec<Expr> = vec![];

        let json = MinioFilterPushdownSupport::filters_to_iceberg_json(&filters);
        assert!(json.is_none());
    }

    #[test]
    fn test_can_push_down_simple_comparison() {
        let expr = col("age").gt(lit(18));
        assert!(MinioFilterPushdownSupport::can_push_down(&expr));
    }

    #[test]
    fn test_can_push_down_and_expression() {
        let expr = col("age").gt(lit(18)).and(col("status").eq(lit("active")));
        assert!(MinioFilterPushdownSupport::can_push_down(&expr));
    }

    #[test]
    fn test_can_push_down_or_expression() {
        let expr = col("status")
            .eq(lit("active"))
            .or(col("status").eq(lit("pending")));
        assert!(MinioFilterPushdownSupport::can_push_down(&expr));
    }

    #[test]
    fn test_can_push_down_is_null() {
        let expr = col("optional_field").is_null();
        assert!(MinioFilterPushdownSupport::can_push_down(&expr));
    }

    #[test]
    fn test_extract_pushable_filters_single() {
        let expr = col("age").gt(lit(18));
        let filters = vec![expr];

        let (pushable, residual) = MinioFilterPushdownSupport::extract_pushable_filters(&filters);

        assert_eq!(pushable.len(), 1);
        assert_eq!(residual.len(), 0);
    }

    #[test]
    fn test_filters_to_iceberg_json_single_filter() {
        let expr = col("age").gt(lit(18));
        let filters = vec![expr];

        let json = MinioFilterPushdownSupport::filters_to_iceberg_json(&filters);
        assert!(json.is_some());
        let json_val = json.unwrap();
        // Just verify we got a valid JSON object back
        assert!(json_val.is_object());
    }

    #[test]
    fn test_filters_to_iceberg_json_multiple_filters() {
        let expr1 = col("age").gt(lit(18));
        let expr2 = col("status").eq(lit("active"));
        let filters = vec![expr1, expr2];

        let json = MinioFilterPushdownSupport::filters_to_iceberg_json(&filters);
        assert!(json.is_some());
        let json_val = json.unwrap();
        assert_eq!(json_val["type"], "and");
        assert!(json_val["left"].is_object());
        assert!(json_val["right"].is_object());
    }

    #[test]
    fn test_extract_pushable_filters_complex_expression() {
        let expr1 = col("age").gt(lit(18)).and(col("status").eq(lit("active")));
        let expr2 = col("country").eq(lit("US"));
        let filters = vec![expr1, expr2];

        let (pushable, residual) = MinioFilterPushdownSupport::extract_pushable_filters(&filters);

        assert_eq!(pushable.len(), 2);
        assert_eq!(residual.len(), 0);
    }

    #[test]
    fn test_filters_to_iceberg_json_string_operators() {
        // Note: starts_with is a custom operator that may not be directly supported
        // but the filter translator should handle comparable expressions
        let expr = col("name").eq(lit("Alice"));
        let filters = vec![expr];

        let json = MinioFilterPushdownSupport::filters_to_iceberg_json(&filters);
        assert!(json.is_some());
        let json_val = json.unwrap();
        // Just verify we got a valid JSON object back
        assert!(json_val.is_object());
    }
}
