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

//! Iceberg filter expression builders for query pushdown.
//!
//! This module provides a fluent API for constructing Iceberg filter expressions
//! to push down predicates to MinIO S3 Tables for server-side filtering.
//!
//! Filter expressions follow the Iceberg REST Catalog OpenAPI specification:
//! <https://github.com/apache/iceberg/blob/main/open-api/rest-catalog-open-api.yaml>
//!
//! See the `Expression` schema for the discriminated union of expression types.
//!
//! # Example
//!
//! ```
//! use minio::s3tables::filter::{FilterBuilder, ComparisonOp};
//!
//! // Build: age >= 18 AND status == "active"
//! let filter = FilterBuilder::column("age")
//!     .gte(18)
//!     .and(
//!         FilterBuilder::column("status")
//!             .eq("active")
//!     );
//!
//! let json = filter.to_json();
//! ```

use serde_json::{Value, json};
use std::ops::Not;

/// Comparison operators for filter expressions.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ComparisonOp {
    /// Equal to
    Eq,
    /// Not equal to
    NotEq,
    /// Less than
    Lt,
    /// Less than or equal to
    Lte,
    /// Greater than
    Gt,
    /// Greater than or equal to
    Gte,
    /// String starts with (for VARCHAR/STRING, LIKE 'prefix%')
    StartsWith,
    /// String ends with (for VARCHAR/STRING, LIKE '%suffix')
    EndsWith,
    /// String contains substring (for VARCHAR/STRING, LIKE '%middle%')
    Contains,
    /// Case-insensitive string starts with (for VARCHAR/STRING, ILIKE 'prefix%')
    StartsWithI,
    /// Case-insensitive string ends with (for VARCHAR/STRING, ILIKE '%suffix')
    EndsWithI,
    /// Case-insensitive string contains substring (for VARCHAR/STRING, ILIKE '%middle%')
    ContainsI,
    /// Value is contained in set (IN operator)
    In,
    /// Value is not in set (NOT IN operator)
    NotIn,
    /// Is null
    IsNull,
    /// Is not null
    NotNull,
    /// Is NaN (for floating-point numeric types)
    IsNan,
    /// Is not NaN (for floating-point numeric types)
    NotNan,
}

impl ComparisonOp {
    /// Returns the Iceberg REST Catalog filter type string for this operator.
    /// Spec: https://github.com/apache/iceberg/blob/main/open-api/rest-catalog-open-api.yaml
    fn as_str(self) -> &'static str {
        match self {
            ComparisonOp::Eq => "eq",
            ComparisonOp::NotEq => "neq",
            ComparisonOp::Lt => "lt",
            ComparisonOp::Lte => "lte",
            ComparisonOp::Gt => "gt",
            ComparisonOp::Gte => "gte",
            ComparisonOp::StartsWith => "starts-with",
            ComparisonOp::EndsWith => "ends-with",
            ComparisonOp::Contains => "contains",
            ComparisonOp::StartsWithI => "starts-with-i",
            ComparisonOp::EndsWithI => "ends-with-i",
            ComparisonOp::ContainsI => "contains-i",
            ComparisonOp::In => "in",
            ComparisonOp::NotIn => "not-in",
            ComparisonOp::IsNull => "is-null",
            ComparisonOp::NotNull => "not-null",
            ComparisonOp::IsNan => "is-nan",
            ComparisonOp::NotNan => "not-nan",
        }
    }
}

/// Represents an Iceberg filter expression for query pushdown.
///
/// Filter expressions can be:
/// - Comparison expressions (e.g., column > value)
/// - Logical expressions (AND, OR, NOT)
/// - Complex nested expressions
#[derive(Debug, Clone)]
pub enum Filter {
    /// Comparison: column op value
    Comparison {
        column: String,
        op: ComparisonOp,
        value: Value,
    },
    /// Logical AND of two filters
    And(Box<Filter>, Box<Filter>),
    /// Logical OR of two filters
    Or(Box<Filter>, Box<Filter>),
    /// Logical NOT of a filter
    Not(Box<Filter>),
}

impl Filter {
    /// Converts the filter expression to a JSON value suitable for the REST API.
    ///
    /// Produces Iceberg REST Catalog filter format per specification:
    /// Spec: https://github.com/apache/iceberg/blob/main/open-api/rest-catalog-open-api.yaml
    ///
    /// Format examples:
    /// - Comparison: `{"type": "eq", "term": "column", "value": 42}`
    /// - AND: `{"type": "and", "left": {...}, "right": {...}}`
    /// - OR: `{"type": "or", "left": {...}, "right": {...}}`
    /// - NOT: `{"type": "not", "child": {...}}`
    pub fn to_json(&self) -> Value {
        match self {
            Filter::Comparison { column, op, value } => {
                match op {
                    ComparisonOp::IsNull | ComparisonOp::NotNull => {
                        // NULL checks: {"type": "is-null", "term": "column"}
                        json!({
                            "type": op.as_str(),
                            "term": column,
                        })
                    }
                    ComparisonOp::IsNan | ComparisonOp::NotNan => {
                        // NaN checks: {"type": "is-nan", "term": "column"}
                        json!({
                            "type": op.as_str(),
                            "term": column,
                        })
                    }
                    ComparisonOp::In | ComparisonOp::NotIn => {
                        // IN/NOT IN: {"type": "in", "term": "column", "values": [...]}
                        json!({
                            "type": op.as_str(),
                            "term": column,
                            "values": value,
                        })
                    }
                    _ => {
                        // Standard comparison: {"type": "eq", "term": "column", "value": 42}
                        json!({
                            "type": op.as_str(),
                            "term": column,
                            "value": value,
                        })
                    }
                }
            }
            Filter::And(left, right) => {
                json!({
                    "type": "and",
                    "left": left.to_json(),
                    "right": right.to_json(),
                })
            }
            Filter::Or(left, right) => {
                json!({
                    "type": "or",
                    "left": left.to_json(),
                    "right": right.to_json(),
                })
            }
            Filter::Not(inner) => {
                json!({
                    "type": "not",
                    "child": inner.to_json(),
                })
            }
        }
    }

    /// Combines this filter with another using AND.
    pub fn and(self, other: Filter) -> Filter {
        Filter::And(Box::new(self), Box::new(other))
    }

    /// Combines this filter with another using OR.
    pub fn or(self, other: Filter) -> Filter {
        Filter::Or(Box::new(self), Box::new(other))
    }
}

impl Not for Filter {
    type Output = Filter;

    /// Negates this filter using the `!` operator.
    fn not(self) -> Filter {
        Filter::Not(Box::new(self))
    }
}

/// Fluent builder for constructing Iceberg filter expressions.
pub struct FilterBuilder {
    column: String,
}

impl FilterBuilder {
    /// Starts building a filter for the given column.
    ///
    /// # Example
    ///
    /// ```
    /// use minio::s3tables::filter::FilterBuilder;
    ///
    /// let filter = FilterBuilder::column("age").gte(18);
    /// ```
    pub fn column(name: impl Into<String>) -> Self {
        FilterBuilder {
            column: name.into(),
        }
    }

    /// Creates an equality filter: column = value
    pub fn eq<V: Into<Value>>(self, value: V) -> Filter {
        Filter::Comparison {
            column: self.column,
            op: ComparisonOp::Eq,
            value: value.into(),
        }
    }

    /// Creates a not-equal filter: column != value
    pub fn neq<V: Into<Value>>(self, value: V) -> Filter {
        Filter::Comparison {
            column: self.column,
            op: ComparisonOp::NotEq,
            value: value.into(),
        }
    }

    /// Creates a less-than filter: column < value
    pub fn lt<V: Into<Value>>(self, value: V) -> Filter {
        Filter::Comparison {
            column: self.column,
            op: ComparisonOp::Lt,
            value: value.into(),
        }
    }

    /// Creates a less-than-or-equal filter: column <= value
    pub fn lte<V: Into<Value>>(self, value: V) -> Filter {
        Filter::Comparison {
            column: self.column,
            op: ComparisonOp::Lte,
            value: value.into(),
        }
    }

    /// Creates a greater-than filter: column > value
    pub fn gt<V: Into<Value>>(self, value: V) -> Filter {
        Filter::Comparison {
            column: self.column,
            op: ComparisonOp::Gt,
            value: value.into(),
        }
    }

    /// Creates a greater-than-or-equal filter: column >= value
    pub fn gte<V: Into<Value>>(self, value: V) -> Filter {
        Filter::Comparison {
            column: self.column,
            op: ComparisonOp::Gte,
            value: value.into(),
        }
    }

    /// Creates a "starts with" filter for string columns: column starts_with value
    pub fn starts_with(self, value: impl Into<String>) -> Filter {
        Filter::Comparison {
            column: self.column,
            op: ComparisonOp::StartsWith,
            value: Value::String(value.into()),
        }
    }

    /// Creates an "ends with" filter for string columns: column ends_with value
    pub fn ends_with(self, value: impl Into<String>) -> Filter {
        Filter::Comparison {
            column: self.column,
            op: ComparisonOp::EndsWith,
            value: Value::String(value.into()),
        }
    }

    /// Creates a "contains" filter for string columns: column contains value
    pub fn contains(self, value: impl Into<String>) -> Filter {
        Filter::Comparison {
            column: self.column,
            op: ComparisonOp::Contains,
            value: Value::String(value.into()),
        }
    }

    /// Creates a case-insensitive "starts with" filter (ILIKE): column starts_with_i value
    pub fn starts_with_i(self, value: impl Into<String>) -> Filter {
        Filter::Comparison {
            column: self.column,
            op: ComparisonOp::StartsWithI,
            value: Value::String(value.into()),
        }
    }

    /// Creates a case-insensitive "ends with" filter (ILIKE): column ends_with_i value
    pub fn ends_with_i(self, value: impl Into<String>) -> Filter {
        Filter::Comparison {
            column: self.column,
            op: ComparisonOp::EndsWithI,
            value: Value::String(value.into()),
        }
    }

    /// Creates a case-insensitive "contains" filter (ILIKE): column contains_i value
    pub fn contains_i(self, value: impl Into<String>) -> Filter {
        Filter::Comparison {
            column: self.column,
            op: ComparisonOp::ContainsI,
            value: Value::String(value.into()),
        }
    }

    /// Creates an IN filter: column IN (values...)
    ///
    /// # Example
    ///
    /// ```
    /// use minio::s3tables::filter::FilterBuilder;
    /// use serde_json::json;
    ///
    /// let filter = FilterBuilder::column("status")
    ///     .is_in(json!(["active", "pending"]));
    /// ```
    pub fn is_in(self, values: Value) -> Filter {
        Filter::Comparison {
            column: self.column,
            op: ComparisonOp::In,
            value: values,
        }
    }

    /// Creates a NOT IN filter: column NOT IN (values...)
    pub fn not_in(self, values: Value) -> Filter {
        Filter::Comparison {
            column: self.column,
            op: ComparisonOp::NotIn,
            value: values,
        }
    }

    /// Creates an IS NULL filter: column IS NULL
    pub fn is_null(self) -> Filter {
        Filter::Comparison {
            column: self.column,
            op: ComparisonOp::IsNull,
            value: json!(null),
        }
    }

    /// Creates an IS NOT NULL filter: column IS NOT NULL
    pub fn is_not_null(self) -> Filter {
        Filter::Comparison {
            column: self.column,
            op: ComparisonOp::NotNull,
            value: json!(null),
        }
    }

    /// Creates an IS NAN filter for floating-point columns: column IS NAN
    pub fn is_nan(self) -> Filter {
        Filter::Comparison {
            column: self.column,
            op: ComparisonOp::IsNan,
            value: json!(null),
        }
    }

    /// Creates an IS NOT NAN filter for floating-point columns: column IS NOT NAN
    pub fn is_not_nan(self) -> Filter {
        Filter::Comparison {
            column: self.column,
            op: ComparisonOp::NotNan,
            value: json!(null),
        }
    }

    /// Creates a BETWEEN filter: column >= lower AND column <= upper
    ///
    /// This is syntactic sugar for a compound AND filter that checks if a value
    /// is within a range (inclusive on both ends).
    ///
    /// # Example
    ///
    /// ```
    /// use minio::s3tables::filter::FilterBuilder;
    ///
    /// let filter = FilterBuilder::column("age").between(18, 65);
    /// // Equivalent to: (age >= 18) AND (age <= 65)
    /// ```
    pub fn between<V: Into<Value>>(self, lower: V, upper: V) -> Filter {
        let lower_val = lower.into();
        let upper_val = upper.into();

        let lower_filter = Filter::Comparison {
            column: self.column.clone(),
            op: ComparisonOp::Gte,
            value: lower_val,
        };

        let upper_filter = Filter::Comparison {
            column: self.column,
            op: ComparisonOp::Lte,
            value: upper_val,
        };

        lower_filter.and(upper_filter)
    }
}

/// Helper function to create a combined filter from multiple conditions.
///
/// # Example
///
/// ```
/// use minio::s3tables::filter::{FilterBuilder, and_all};
///
/// let filters = vec![
///     FilterBuilder::column("age").gte(18),
///     FilterBuilder::column("status").eq("active"),
///     FilterBuilder::column("country").is_in(serde_json::json!(["US", "CA"])),
/// ];
///
/// let combined = and_all(filters);
/// ```
pub fn and_all(filters: Vec<Filter>) -> Option<Filter> {
    let mut iter = filters.into_iter();
    let first = iter.next()?;
    Some(iter.fold(first, |acc, f| acc.and(f)))
}

/// Helper function to create an OR-combined filter from multiple conditions.
pub fn or_all(filters: Vec<Filter>) -> Option<Filter> {
    let mut iter = filters.into_iter();
    let first = iter.next()?;
    Some(iter.fold(first, |acc, f| acc.or(f)))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_simple_comparison() {
        let filter = FilterBuilder::column("age").gte(18);
        let json = filter.to_json();

        // New format: {"type": "gte", "term": "age", "value": 18}
        assert_eq!(json.get("type").and_then(|v| v.as_str()), Some("gte"));
        assert_eq!(json.get("term").and_then(|v| v.as_str()), Some("age"));
        assert_eq!(json.get("value").and_then(|v| v.as_i64()), Some(18));
    }

    #[test]
    fn test_and_filter() {
        let filter = FilterBuilder::column("age")
            .gte(18)
            .and(FilterBuilder::column("status").eq("active"));

        let json = filter.to_json();
        assert_eq!(json.get("type").and_then(|v| v.as_str()), Some("and"));

        let left = json.get("left").expect("left should exist");
        assert_eq!(left.get("type").and_then(|v| v.as_str()), Some("gte"));

        let right = json.get("right").expect("right should exist");
        assert_eq!(right.get("type").and_then(|v| v.as_str()), Some("eq"));
    }

    #[test]
    fn test_or_filter() {
        let filter = FilterBuilder::column("status")
            .eq("active")
            .or(FilterBuilder::column("status").eq("pending"));

        let json = filter.to_json();
        assert_eq!(json.get("type").and_then(|v| v.as_str()), Some("or"));
    }

    #[test]
    fn test_is_null_filter() {
        let filter = FilterBuilder::column("optional_field").is_null();
        let json = filter.to_json();

        // New format: {"type": "is-null", "term": "optional_field"}
        assert_eq!(json.get("type").and_then(|v| v.as_str()), Some("is-null"));
        assert_eq!(
            json.get("term").and_then(|v| v.as_str()),
            Some("optional_field")
        );
    }

    #[test]
    fn test_in_filter() {
        let filter =
            FilterBuilder::column("status").is_in(json!(["active", "pending", "processing"]));

        let json = filter.to_json();
        assert_eq!(json.get("type").and_then(|v| v.as_str()), Some("in"));
        assert_eq!(json.get("term").and_then(|v| v.as_str()), Some("status"));
        assert!(json.get("values").is_some());
    }

    #[test]
    fn test_and_all() {
        let filters = vec![
            FilterBuilder::column("age").gte(18),
            FilterBuilder::column("status").eq("active"),
        ];

        let combined = and_all(filters).unwrap();
        let json = combined.to_json();
        assert_eq!(json.get("type").and_then(|v| v.as_str()), Some("and"));
    }

    #[test]
    fn test_complex_filter() {
        // (age >= 18 AND status = "active") OR country IN ["US", "CA"]
        let filter = FilterBuilder::column("age")
            .gte(18)
            .and(FilterBuilder::column("status").eq("active"))
            .or(FilterBuilder::column("country").is_in(json!(["US", "CA"])));

        let json = filter.to_json();
        assert_eq!(json.get("type").and_then(|v| v.as_str()), Some("or"));
    }

    #[test]
    fn test_is_nan_filter() {
        let filter = FilterBuilder::column("value").is_nan();
        let json = filter.to_json();

        // New format: {"type": "is-nan", "term": "value"}
        assert_eq!(json.get("type").and_then(|v| v.as_str()), Some("is-nan"));
        assert_eq!(json.get("term").and_then(|v| v.as_str()), Some("value"));
    }

    #[test]
    fn test_is_not_nan_filter() {
        let filter = FilterBuilder::column("value").is_not_nan();
        let json = filter.to_json();

        // New format: {"type": "not-nan", "term": "value"}
        assert_eq!(json.get("type").and_then(|v| v.as_str()), Some("not-nan"));
        assert_eq!(json.get("term").and_then(|v| v.as_str()), Some("value"));
    }

    #[test]
    fn test_between_filter() {
        let filter = FilterBuilder::column("age").between(18, 65);
        let json = filter.to_json();

        // between(18, 65) produces: (age >= 18) AND (age <= 65)
        assert_eq!(json.get("type").and_then(|v| v.as_str()), Some("and"));

        let left = json.get("left").expect("left should exist");
        assert_eq!(left.get("type").and_then(|v| v.as_str()), Some("gte"));
        assert_eq!(left.get("term").and_then(|v| v.as_str()), Some("age"));
        assert_eq!(left.get("value").and_then(|v| v.as_i64()), Some(18));

        let right = json.get("right").expect("right should exist");
        assert_eq!(right.get("type").and_then(|v| v.as_str()), Some("lte"));
        assert_eq!(right.get("term").and_then(|v| v.as_str()), Some("age"));
        assert_eq!(right.get("value").and_then(|v| v.as_i64()), Some(65));
    }

    #[test]
    fn test_between_with_floats() {
        let filter = FilterBuilder::column("temperature").between(32.5, 98.6);
        let json = filter.to_json();

        assert_eq!(json.get("type").and_then(|v| v.as_str()), Some("and"));
    }

    /// Verify JSON format matches Iceberg REST Catalog OpenAPI specification.
    /// Spec: https://github.com/apache/iceberg/blob/main/open-api/rest-catalog-open-api.yaml
    ///
    /// This test ensures we don't regress to invalid formats like:
    /// - {"type": "unbound", "op": ">=", ...} (WRONG - "unbound" is not a valid type)
    /// - {"type": "and", "left": {"type": "literal", ...}} (WRONG - literals aren't standalone)
    ///
    /// The correct format uses the operation as the type discriminator:
    /// - {"type": "eq", "term": "column", "value": 42}
    #[test]
    fn test_json_format_matches_iceberg_rest_spec() {
        // Equality: {"type": "eq", "term": "id", "value": 42}
        let eq_filter = FilterBuilder::column("id").eq(42);
        assert_eq!(
            eq_filter.to_json(),
            json!({"type": "eq", "term": "id", "value": 42})
        );

        // Greater than: {"type": "gt", "term": "id", "value": 100}
        let gt_filter = FilterBuilder::column("id").gt(100);
        assert_eq!(
            gt_filter.to_json(),
            json!({"type": "gt", "term": "id", "value": 100})
        );

        // Less than or equal: {"type": "lte", "term": "price", "value": 99.99}
        let lte_filter = FilterBuilder::column("price").lte(99.99);
        assert_eq!(
            lte_filter.to_json(),
            json!({"type": "lte", "term": "price", "value": 99.99})
        );

        // IS NULL: {"type": "is-null", "term": "nullable_col"}
        let null_filter = FilterBuilder::column("nullable_col").is_null();
        assert_eq!(
            null_filter.to_json(),
            json!({"type": "is-null", "term": "nullable_col"})
        );

        // IS NOT NULL: {"type": "not-null", "term": "required_col"}
        let not_null_filter = FilterBuilder::column("required_col").is_not_null();
        assert_eq!(
            not_null_filter.to_json(),
            json!({"type": "not-null", "term": "required_col"})
        );

        // IN: {"type": "in", "term": "status", "values": ["a", "b"]}
        let in_filter = FilterBuilder::column("status").is_in(json!(["active", "pending"]));
        assert_eq!(
            in_filter.to_json(),
            json!({"type": "in", "term": "status", "values": ["active", "pending"]})
        );

        // AND: {"type": "and", "left": {...}, "right": {...}}
        let and_filter = FilterBuilder::column("id")
            .gt(10)
            .and(FilterBuilder::column("id").lt(100));
        assert_eq!(
            and_filter.to_json(),
            json!({
                "type": "and",
                "left": {"type": "gt", "term": "id", "value": 10},
                "right": {"type": "lt", "term": "id", "value": 100}
            })
        );

        // OR: {"type": "or", "left": {...}, "right": {...}}
        let or_filter = FilterBuilder::column("status")
            .eq("active")
            .or(FilterBuilder::column("status").eq("pending"));
        assert_eq!(
            or_filter.to_json(),
            json!({
                "type": "or",
                "left": {"type": "eq", "term": "status", "value": "active"},
                "right": {"type": "eq", "term": "status", "value": "pending"}
            })
        );

        // NOT: {"type": "not", "child": {...}}
        let not_filter = !FilterBuilder::column("deleted").eq(true);
        assert_eq!(
            not_filter.to_json(),
            json!({
                "type": "not",
                "child": {"type": "eq", "term": "deleted", "value": true}
            })
        );
    }

    /// Verify the JSON does NOT contain invalid Iceberg expression types.
    /// These formats were incorrectly used before and caused server rejection.
    #[test]
    fn test_json_does_not_contain_invalid_types() {
        let filters = vec![
            FilterBuilder::column("id").eq(1),
            FilterBuilder::column("id").gt(1),
            FilterBuilder::column("id").lt(1),
            FilterBuilder::column("id").gte(1),
            FilterBuilder::column("id").lte(1),
            FilterBuilder::column("id").is_null(),
            FilterBuilder::column("id").is_not_null(),
        ];

        for filter in filters {
            let json_str = filter.to_json().to_string();

            // Must NOT contain these invalid type values
            assert!(
                !json_str.contains("\"type\":\"unbound\""),
                "Filter JSON must not use 'unbound' type: {}",
                json_str
            );
            assert!(
                !json_str.contains("\"type\":\"literal\""),
                "Filter JSON must not use 'literal' type: {}",
                json_str
            );
            assert!(
                !json_str.contains("\"op\":"),
                "Filter JSON must not use 'op' field: {}",
                json_str
            );
        }
    }
}
