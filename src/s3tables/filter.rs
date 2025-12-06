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
    fn as_str(self) -> &'static str {
        match self {
            ComparisonOp::Eq => "=",
            ComparisonOp::NotEq => "!=",
            ComparisonOp::Lt => "<",
            ComparisonOp::Lte => "<=",
            ComparisonOp::Gt => ">",
            ComparisonOp::Gte => ">=",
            ComparisonOp::StartsWith => "starts_with",
            ComparisonOp::EndsWith => "ends_with",
            ComparisonOp::Contains => "contains",
            ComparisonOp::StartsWithI => "starts_with_i",
            ComparisonOp::EndsWithI => "ends_with_i",
            ComparisonOp::ContainsI => "contains_i",
            ComparisonOp::In => "in",
            ComparisonOp::NotIn => "not_in",
            ComparisonOp::IsNull => "is_null",
            ComparisonOp::NotNull => "not_null",
            ComparisonOp::IsNan => "is_nan",
            ComparisonOp::NotNan => "not_nan",
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
    pub fn to_json(&self) -> Value {
        match self {
            Filter::Comparison { column, op, value } => {
                match op {
                    ComparisonOp::IsNull
                    | ComparisonOp::NotNull
                    | ComparisonOp::IsNan
                    | ComparisonOp::NotNan => {
                        // NULL and NaN checks don't need a value
                        json!({
                            "type": "unbound",
                            "op": op.as_str(),
                            "term": column,
                        })
                    }
                    ComparisonOp::In | ComparisonOp::NotIn => {
                        // IN/NOT IN take an array value
                        json!({
                            "type": "in",
                            "term": {
                                "type": "unbound",
                                "term": column,
                            },
                            "values": value,
                        })
                    }
                    _ => {
                        // Standard comparison
                        json!({
                            "type": "and",
                            "left": {
                                "type": "unbound",
                                "op": op.as_str(),
                                "term": column,
                            },
                            "right": {
                                "type": "literal",
                                "value": value,
                            }
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
                    "inner": inner.to_json(),
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

        assert_eq!(json.get("type").and_then(|v| v.as_str()), Some("and"));
        assert_eq!(
            json.get("left")
                .and_then(|v| v.get("op"))
                .and_then(|v| v.as_str()),
            Some(">=")
        );
    }

    #[test]
    fn test_and_filter() {
        let filter = FilterBuilder::column("age")
            .gte(18)
            .and(FilterBuilder::column("status").eq("active"));

        let json = filter.to_json();
        assert_eq!(json.get("type").and_then(|v| v.as_str()), Some("and"));
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

        assert_eq!(json.get("op").and_then(|v| v.as_str()), Some("is_null"));
    }

    #[test]
    fn test_in_filter() {
        let filter =
            FilterBuilder::column("status").is_in(json!(["active", "pending", "processing"]));

        let json = filter.to_json();
        assert_eq!(json.get("type").and_then(|v| v.as_str()), Some("in"));
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

        assert_eq!(json.get("type").and_then(|v| v.as_str()), Some("unbound"));
        assert_eq!(json.get("op").and_then(|v| v.as_str()), Some("is_nan"));
        assert_eq!(json.get("term").and_then(|v| v.as_str()), Some("value"));
    }

    #[test]
    fn test_is_not_nan_filter() {
        let filter = FilterBuilder::column("value").is_not_nan();
        let json = filter.to_json();

        assert_eq!(json.get("type").and_then(|v| v.as_str()), Some("unbound"));
        assert_eq!(json.get("op").and_then(|v| v.as_str()), Some("not_nan"));
        assert_eq!(json.get("term").and_then(|v| v.as_str()), Some("value"));
    }

    #[test]
    fn test_between_filter() {
        let filter = FilterBuilder::column("age").between(18, 65);
        let json = filter.to_json();

        assert_eq!(json.get("type").and_then(|v| v.as_str()), Some("and"));

        let left = json.get("left").expect("left should exist");
        assert_eq!(left.get("type").and_then(|v| v.as_str()), Some("and"));
        assert_eq!(
            left.get("left")
                .and_then(|v| v.get("op"))
                .and_then(|v| v.as_str()),
            Some(">=")
        );

        let right = json.get("right").expect("right should exist");
        assert_eq!(right.get("type").and_then(|v| v.as_str()), Some("and"));
        assert_eq!(
            right
                .get("left")
                .and_then(|v| v.get("op"))
                .and_then(|v| v.as_str()),
            Some("<=")
        );
    }

    #[test]
    fn test_between_with_floats() {
        let filter = FilterBuilder::column("temperature").between(32.5, 98.6);
        let json = filter.to_json();

        assert_eq!(json.get("type").and_then(|v| v.as_str()), Some("and"));
    }
}
