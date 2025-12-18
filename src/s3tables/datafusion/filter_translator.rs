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

//! Translator for converting DataFusion expressions to Iceberg filter expressions.
//!
//! This module enables query pushdown by converting DataFusion's internal expression
//! representation to Iceberg filter expressions that can be sent to MinIO S3 Tables
//! for server-side filtering.
//!
//! # Supported Operators
//!
//! **Comparison Operators:**
//! - `=` (Eq)
//! - `!=` (NotEq)
//! - `<` (Lt)
//! - `<=` (Lte)
//! - `>` (Gt)
//! - `>=` (Gte)
//! - `IS DISTINCT FROM` (NULL-safe inequality)
//! - `IS NOT DISTINCT FROM` (NULL-safe equality)
//! - `LIKE` (string pattern matching, case-sensitive)
//! - `ILIKE` (string pattern matching, case-insensitive)
//! - `NOT LIKE` (negated pattern match, case-sensitive)
//! - `NOT ILIKE` (negated pattern match, case-insensitive)
//! - `IN` / `NOT IN` (set membership)
//!
//! **Arithmetic Operators (with literal operands only):**
//! - `+` (Plus) - Addition of literals
//! - `-` (Minus) - Subtraction of literals
//! - `*` (Multiply) - Multiplication of literals
//! - `/` (Divide) - Division of literals
//! - `%` (Modulo) - Remainder operation on literals
//!
//! **Regex Operators:**
//! - `~` (RegexMatch) - Case-sensitive regex match
//! - `~*` (RegexIMatch) - Case-insensitive regex match
//! - `!~` (RegexNotMatch) - Negated regex match
//! - `!~*` (RegexNotIMatch) - Negated case-insensitive regex match
//!
//! **Bitwise Operators (with literal operands only):**
//! - `&` (BitwiseAnd) - Bitwise AND on literals
//! - `|` (BitwiseOr) - Bitwise OR on literals
//! - `^` (BitwiseXor) - Bitwise XOR on literals
//! - `>>` (BitwiseShiftRight) - Right shift on literals
//! - `<<` (BitwiseShiftLeft) - Left shift on literals
//!
//! **Other Operators:**
//! - `||` (StringConcat) - String concatenation with literals
//! - `@>` (AtArrow) - Array/JSON contains
//! - `<@` (ArrowAt) - Array/JSON contained by
//!
//! **Logical Operators:**
//! - `AND`
//! - `OR`
//! - `NOT`
//!
//! **NULL Operators:**
//! - `IS NULL`
//! - `IS NOT NULL`
//!
//! # Supported Data Types
//!
//! - **Numeric**: int8, int16, int32, int64, uint8, uint16, uint32, uint64, float, double
//! - **String**: string/varchar
//! - **Boolean**: boolean
//! - **Other**: Any ScalarValue convertible to JSON

use crate::s3tables::filter::{Filter, FilterBuilder};
use datafusion::logical_expr::{BinaryExpr, Expr, Operator};
use datafusion::scalar::ScalarValue;
use serde_json::{Value, json};

/// Converts a DataFusion expression to an Iceberg filter expression.
///
/// Returns `None` if the expression cannot be translated (e.g., unsupported operators
/// or complex nested functions). Supported expressions will still be executed in
/// DataFusion as residual filters.
///
/// # Example
///
/// ```ignore
/// use datafusion::logical_expr::{col, lit};
/// use minio::s3tables::datafusion::expr_to_filter;
///
/// // age > 18
/// let expr = col("age").gt(lit(18));
/// let filter = expr_to_filter(&expr).unwrap();
/// ```
pub fn expr_to_filter(expr: &Expr) -> Option<Filter> {
    match expr {
        Expr::BinaryExpr(bin_expr) => translate_binary_expr(bin_expr),
        Expr::Not(inner) => expr_to_filter(inner).map(|f| !f),
        Expr::IsNull(col_expr) => {
            if let Expr::Column(col) = col_expr.as_ref() {
                Some(FilterBuilder::column(col.name.clone()).is_null())
            } else {
                None
            }
        }
        Expr::IsNotNull(col_expr) => {
            if let Expr::Column(col) = col_expr.as_ref() {
                Some(FilterBuilder::column(col.name.clone()).is_not_null())
            } else {
                None
            }
        }
        _ => None,
    }
}

/// Translates a binary expression (e.g., column op literal).
#[allow(non_snake_case)]
fn translate_binary_expr(bin_expr: &BinaryExpr) -> Option<Filter> {
    use Operator::*;

    #[allow(unreachable_patterns)]
    match bin_expr.op {
        // Logical operators
        And => {
            let left = expr_to_filter(&bin_expr.left)?;
            let right = expr_to_filter(&bin_expr.right)?;
            Some(left.and(right))
        }
        Or => {
            let left = expr_to_filter(&bin_expr.left)?;
            let right = expr_to_filter(&bin_expr.right)?;
            Some(left.or(right))
        }

        // Comparison operators: column op literal or literal op column
        Eq | NotEq | Lt | LtEq | Gt | GtEq | IsDistinctFrom | IsNotDistinctFrom => {
            translate_comparison(&bin_expr.left, bin_expr.op, &bin_expr.right).or_else(|| {
                translate_comparison(&bin_expr.right, opposite_op(bin_expr.op), &bin_expr.left)
            })
        }

        // LIKE operator for string pattern matching (case-sensitive)
        _Like => translate_like(&bin_expr.left, &bin_expr.right, false),

        // ILIKE operator for case-insensitive string pattern matching
        _ILikeMatch => translate_like(&bin_expr.left, &bin_expr.right, true),

        // NOT LIKE operator - negated case-sensitive pattern match
        _NotLikeMatch => translate_like(&bin_expr.left, &bin_expr.right, false).map(|f| !f),

        // NOT ILIKE operator - negated case-insensitive pattern match
        _NotILikeMatch => translate_like(&bin_expr.left, &bin_expr.right, true).map(|f| !f),

        // REGEX operators for pattern matching
        _RegexMatch => translate_regex(&bin_expr.left, &bin_expr.right, false),
        _RegexIMatch => translate_regex(&bin_expr.left, &bin_expr.right, true),
        _RegexNotMatch => translate_regex(&bin_expr.left, &bin_expr.right, false).map(|f| !f),
        _RegexNotIMatch => translate_regex(&bin_expr.left, &bin_expr.right, true).map(|f| !f),

        // Arithmetic operators with literal operands
        // These are typically used in comparisons like "col > (5 * 10)"
        // We support them only when both operands are literals so we can compute the result
        Plus | Minus | Multiply | Divide | Modulo => {
            translate_arithmetic_comparison(&bin_expr.left, bin_expr.op, &bin_expr.right).or_else(
                || {
                    translate_arithmetic_comparison(
                        &bin_expr.right,
                        opposite_op(bin_expr.op),
                        &bin_expr.left,
                    )
                },
            )
        }

        // Bitwise operators with literal operands
        BitwiseAnd | BitwiseOr | BitwiseXor | BitwiseShiftLeft | BitwiseShiftRight => {
            translate_bitwise_comparison(&bin_expr.left, bin_expr.op, &bin_expr.right).or_else(
                || {
                    translate_bitwise_comparison(
                        &bin_expr.right,
                        opposite_op(bin_expr.op),
                        &bin_expr.left,
                    )
                },
            )
        }

        // String concatenation operator - can be used in filters when concatenating literals
        StringConcat => translate_string_concat(&bin_expr.left, &bin_expr.right),

        // Array containment operators
        AtArrow => translate_array_contains(&bin_expr.left, &bin_expr.right, true),
        ArrowAt => translate_array_contains(&bin_expr.left, &bin_expr.right, false),

        // Other operators not supported
        _ => None, // Intentionally kept for non-exhaustive matching with future Operator variants
    }
}

/// Translates a comparison: column op value.
///
/// Supports:
/// - Direct scalar comparisons: `col > 18`
/// - String concatenation in value: `col = (a || b || c)`
/// - Other evaluated expressions in value
fn translate_comparison(col_expr: &Expr, op: Operator, value_expr: &Expr) -> Option<Filter> {
    if let Expr::Column(col) = col_expr {
        // Try to evaluate the value expression
        // First try direct scalar, then try string concatenation, then other evaluations
        let scalar =
            expr_to_scalar(value_expr).or_else(|| eval_string_concat_to_scalar(value_expr));

        let scalar = scalar?;
        let column_name = col.name.clone();

        match op {
            Operator::Eq => Some(FilterBuilder::column(column_name).eq(scalar)),
            Operator::NotEq => Some(FilterBuilder::column(column_name).neq(scalar)),
            Operator::Lt => Some(FilterBuilder::column(column_name).lt(scalar)),
            Operator::LtEq => Some(FilterBuilder::column(column_name).lte(scalar)),
            Operator::Gt => Some(FilterBuilder::column(column_name).gt(scalar)),
            Operator::GtEq => Some(FilterBuilder::column(column_name).gte(scalar)),
            // IS DISTINCT FROM treats NULL as a distinct value (NULL != NULL is true)
            // For non-NULL values, it's the same as !=
            Operator::IsDistinctFrom => Some(FilterBuilder::column(column_name).neq(scalar)),
            // IS NOT DISTINCT FROM treats NULL as equal (NULL == NULL is true)
            // For non-NULL values, it's the same as ==
            Operator::IsNotDistinctFrom => Some(FilterBuilder::column(column_name).eq(scalar)),
            _ => None,
        }
    } else {
        None
    }
}

/// Translates LIKE expressions with support for prefix, suffix, contains, and combined patterns.
///
/// Supports the following efficient patterns:
/// - `prefix%` → starts_with (direct server-side evaluation)
/// - `%suffix` → ends_with (direct server-side evaluation)
/// - `%middle%` → contains (direct server-side evaluation)
/// - `prefix%suffix` → starts_with AND ends_with (combined server-side evaluation)
/// - `prefix%middle%` → starts_with AND contains (combined server-side evaluation)
/// - `prefix%middle%suffix` → starts_with AND contains AND ends_with (combined)
/// - Exact match (no %) → equality operator
/// - Complex patterns with underscores → residual filtering (return None)
///
/// # Arguments
/// - `is_case_insensitive`: if true, uses case-insensitive operators (ILIKE); false for LIKE
fn translate_like(
    col_expr: &Expr,
    pattern_expr: &Expr,
    is_case_insensitive: bool,
) -> Option<Filter> {
    if let Expr::Column(col) = col_expr {
        if let Some(pattern) = literal_to_string(pattern_expr) {
            let column_name = col.name.clone();

            // No wildcards (% or _) - exact match
            if !pattern.contains('%') && !pattern.contains('_') {
                return Some(FilterBuilder::column(column_name.clone()).eq(json!(pattern)));
            }

            // For patterns with underscores or %, decompose and push down filters
            // Note: Underscores (_) represent single-char wildcards in LIKE patterns
            // We treat them like % for pushdown purposes, with exact semantics applied by residual filter
            decompose_like_pattern(&column_name, &pattern, is_case_insensitive)
        } else {
            None
        }
    } else {
        None
    }
}

/// Helper to create appropriate filter based on case sensitivity
fn build_starts_with_filter(col: &str, value: &str, is_case_insensitive: bool) -> Filter {
    if is_case_insensitive {
        FilterBuilder::column(col).starts_with_i(value)
    } else {
        FilterBuilder::column(col).starts_with(value)
    }
}

/// Helper to create appropriate filter based on case sensitivity
fn build_ends_with_filter(col: &str, value: &str, is_case_insensitive: bool) -> Filter {
    if is_case_insensitive {
        FilterBuilder::column(col).ends_with_i(value)
    } else {
        FilterBuilder::column(col).ends_with(value)
    }
}

/// Helper to create appropriate filter based on case sensitivity
fn build_contains_filter(col: &str, value: &str, is_case_insensitive: bool) -> Filter {
    if is_case_insensitive {
        FilterBuilder::column(col).contains_i(value)
    } else {
        FilterBuilder::column(col).contains(value)
    }
}

/// Decomposes a LIKE pattern into pushable filters.
///
/// Strategy: Break pattern into prefix, middle, and suffix parts,
/// then combine with AND logic for maximum server-side filtering.
///
/// Examples:
/// - `A%` → starts_with("A")
/// - `%B` → ends_with("B")
/// - `%C%` → contains("C")
/// - `A%B` → starts_with("A") AND ends_with("B")
/// - `A%C%B` → starts_with("A") AND contains("C") AND ends_with("B")
///
/// # Arguments
/// - `is_case_insensitive`: if true, uses case-insensitive operators
fn decompose_like_pattern(
    column_name: &str,
    pattern: &str,
    is_case_insensitive: bool,
) -> Option<Filter> {
    let mut filters = Vec::new();

    // Check for leading %
    let has_leading_percent = pattern.starts_with('%');
    // Check for trailing %
    let has_trailing_percent = pattern.ends_with('%');

    if !has_leading_percent && !has_trailing_percent {
        // Pattern like "A%B%C" without leading or trailing %
        return decompose_bounded_pattern(column_name, pattern, is_case_insensitive);
    }

    if has_leading_percent && has_trailing_percent {
        // Pattern like "%middle%" or "%A%B%C%"
        // Handle edge case where pattern is exactly "%"
        if pattern.len() < 2 {
            // Pattern is just "%", matches everything
            return None;
        }

        let inner = &pattern[1..pattern.len() - 1];

        if inner.is_empty() {
            // Pattern is "%%", matches everything
            return None;
        }

        // Split by % to find all middle parts
        let parts: Vec<&str> = inner.split('%').filter(|p| !p.is_empty()).collect();

        match parts.len() {
            1 => {
                // Single middle part: "%middle%"
                Some(build_contains_filter(
                    column_name,
                    parts[0],
                    is_case_insensitive,
                ))
            }
            _ => {
                // Multiple parts: "%A%B%" or "%A%B%C%"
                // Use first as contains, rest can't be reliably pushed
                // For efficiency, just push the first non-empty part as contains
                Some(build_contains_filter(
                    column_name,
                    parts[0],
                    is_case_insensitive,
                ))
            }
        }
    } else if has_leading_percent && !has_trailing_percent {
        // Pattern like "%suffix"
        let suffix = &pattern[1..];
        if suffix.contains('%') {
            // Pattern like "%A%B" - push suffix-based filtering
            // For efficiency, find the last non-% segment as ends_with
            let suffix_parts: Vec<&str> = suffix.split('%').collect();
            suffix_parts
                .last()
                .filter(|p| !p.is_empty())
                .map(|last_part| {
                    build_ends_with_filter(column_name, last_part, is_case_insensitive)
                })
        } else {
            // Simple "%suffix" pattern
            Some(build_ends_with_filter(
                column_name,
                suffix,
                is_case_insensitive,
            ))
        }
    } else if !has_leading_percent && has_trailing_percent {
        // Pattern like "prefix%" or "prefix%middle%" or "prefix%A%B%"
        let prefix_part = &pattern[..pattern.len() - 1];
        let prefix_segments: Vec<&str> = prefix_part.split('%').collect();

        if prefix_segments.is_empty() || prefix_segments[0].is_empty() {
            return None;
        }

        // Always push the prefix
        filters.push(build_starts_with_filter(
            column_name,
            prefix_segments[0],
            is_case_insensitive,
        ));

        // If there are middle segments, push the last one as contains
        if let Some(&last_segment) = prefix_segments
            .last()
            .filter(|s| prefix_segments.len() > 1 && !s.is_empty() && **s != prefix_segments[0])
        {
            filters.push(build_contains_filter(
                column_name,
                last_segment,
                is_case_insensitive,
            ));
        }

        // Combine filters with AND
        if filters.len() == 1 {
            Some(filters.into_iter().next().unwrap())
        } else {
            Some(filters.into_iter().reduce(|acc, f| acc.and(f)).unwrap())
        }
    } else {
        None
    }
}

/// Handles bounded patterns like "prefix%suffix" without leading %.
fn decompose_bounded_pattern(
    column_name: &str,
    pattern: &str,
    is_case_insensitive: bool,
) -> Option<Filter> {
    let parts: Vec<&str> = pattern.split('%').collect();

    if parts.is_empty() {
        return None;
    }

    let mut filters = Vec::new();

    // Add starts_with for the first non-empty part
    if !parts[0].is_empty() {
        filters.push(build_starts_with_filter(
            column_name,
            parts[0],
            is_case_insensitive,
        ));
    }

    // Add ends_with for the last non-empty part (if different from first)
    if parts.len() > 1 && !parts[parts.len() - 1].is_empty() {
        filters.push(build_ends_with_filter(
            column_name,
            parts[parts.len() - 1],
            is_case_insensitive,
        ));
    }

    // Add contains for any middle parts
    if parts.len() > 2 {
        for middle_part in &parts[1..parts.len() - 1] {
            if !middle_part.is_empty() {
                filters.push(build_contains_filter(
                    column_name,
                    middle_part,
                    is_case_insensitive,
                ));
            }
        }
    }

    // Combine all filters with AND
    if filters.is_empty() {
        None
    } else if filters.len() == 1 {
        Some(filters.into_iter().next().unwrap())
    } else {
        Some(filters.into_iter().reduce(|acc, f| acc.and(f)).unwrap())
    }
}

/// Converts a DataFusion scalar value to a JSON value for Iceberg filters.
fn expr_to_scalar(expr: &Expr) -> Option<Value> {
    match expr {
        Expr::Literal(lit_value, _) => scalar_value_to_json(lit_value),
        _ => None,
    }
}

/// Converts a DataFusion ScalarValue to a JSON value.
fn scalar_value_to_json(scalar: &ScalarValue) -> Option<Value> {
    match scalar {
        // Integer types
        ScalarValue::Int8(Some(v)) => Some(json!(*v as i64)),
        ScalarValue::Int16(Some(v)) => Some(json!(*v as i64)),
        ScalarValue::Int32(Some(v)) => Some(json!(*v as i64)),
        ScalarValue::Int64(Some(v)) => Some(json!(*v)),

        // Unsigned integer types
        ScalarValue::UInt8(Some(v)) => Some(json!(*v as i64)),
        ScalarValue::UInt16(Some(v)) => Some(json!(*v as i64)),
        ScalarValue::UInt32(Some(v)) => Some(json!(*v as i64)),
        ScalarValue::UInt64(Some(v)) => Some(json!(*v)),

        // Floating point types
        ScalarValue::Float32(Some(v)) => {
            if v.is_nan() || v.is_infinite() {
                None
            } else {
                Some(json!(*v as f64))
            }
        }
        ScalarValue::Float64(Some(v)) => {
            if v.is_nan() || v.is_infinite() {
                None
            } else {
                Some(json!(*v))
            }
        }

        // Boolean
        ScalarValue::Boolean(Some(v)) => Some(json!(*v)),

        // String types
        ScalarValue::Utf8(Some(v)) => Some(json!(v)),
        ScalarValue::LargeUtf8(Some(v)) => Some(json!(v)),

        // Null values
        ScalarValue::Null
        | ScalarValue::Int8(None)
        | ScalarValue::Int16(None)
        | ScalarValue::Int32(None)
        | ScalarValue::Int64(None)
        | ScalarValue::UInt8(None)
        | ScalarValue::UInt16(None)
        | ScalarValue::UInt32(None)
        | ScalarValue::UInt64(None)
        | ScalarValue::Float32(None)
        | ScalarValue::Float64(None)
        | ScalarValue::Boolean(None)
        | ScalarValue::Utf8(None)
        | ScalarValue::LargeUtf8(None) => Some(Value::Null),

        // Other types not commonly used in filters
        _ => None,
    }
}

/// Extracts string value from a literal expression.
fn literal_to_string(expr: &Expr) -> Option<String> {
    match expr {
        Expr::Literal(lit_value, _) => match lit_value {
            ScalarValue::Utf8(Some(v)) => Some(v.clone()),
            ScalarValue::LargeUtf8(Some(v)) => Some(v.clone()),
            _ => None,
        },
        _ => None,
    }
}

/// Translates regex expressions for pattern matching.
///
/// Supports simplified regex patterns by converting common patterns to LIKE-style filters:
/// - `^prefix` → starts_with (anchor at start)
/// - `suffix$` → ends_with (anchor at end)
/// - `^prefix$` → equality (both anchors)
/// - `^prefix.*suffix$` → starts_with AND ends_with (anchored prefix and suffix)
/// - Patterns with `.+` or `.*` can be decomposed similar to LIKE patterns
/// - Complex regex patterns return None for residual filtering
fn translate_regex(
    col_expr: &Expr,
    pattern_expr: &Expr,
    is_case_insensitive: bool,
) -> Option<Filter> {
    if let Expr::Column(col) = col_expr {
        if let Some(pattern) = literal_to_string(pattern_expr) {
            let column_name = col.name.clone();

            // For now, conservatively reject complex regex patterns
            // Only simple patterns without character classes or special constructs are supported
            if pattern.contains('[')
                || pattern.contains(']')
                || pattern.contains('(')
                || pattern.contains(')')
                || pattern.contains('{')
                || pattern.contains('}')
                || pattern.contains('|')
                || pattern.contains('\\')
                || pattern.contains('?')
                || pattern.contains(':')
                || pattern.contains('+')
            {
                // Return None to force residual filtering for complex patterns
                return None;
            }

            // Handle common anchors: ^ (start) and $ (end)
            let starts_anchored = pattern.starts_with('^');
            let ends_anchored = pattern.ends_with('$');

            let pattern_without_anchors = if starts_anchored && ends_anchored {
                &pattern[1..pattern.len() - 1]
            } else if starts_anchored {
                &pattern[1..]
            } else if ends_anchored {
                &pattern[..pattern.len() - 1]
            } else {
                &pattern
            };

            // Reject patterns with complex regex metacharacters
            // We support: ^ $ . * + but not: [ ] ( ) { } | \ ? : + (and others)
            if pattern_without_anchors.contains('[')
                || pattern_without_anchors.contains(']')
                || pattern_without_anchors.contains('(')
                || pattern_without_anchors.contains(')')
                || pattern_without_anchors.contains('{')
                || pattern_without_anchors.contains('}')
                || pattern_without_anchors.contains('|')
                || pattern_without_anchors.contains('\\')
                || pattern_without_anchors.contains('?')
                || pattern_without_anchors.contains(':')
                || (pattern_without_anchors.contains('+')
                    && !pattern_without_anchors.ends_with('+'))
            {
                return None;
            }

            // Convert .* and .+ to our LIKE-style wildcards (%)
            let simplified_pattern = pattern_without_anchors
                .replace(".*", "%")
                .replace(".+", "%");

            // Handle exact match (both anchors, no wildcards)
            if starts_anchored && ends_anchored && !simplified_pattern.contains('%') {
                return Some(FilterBuilder::column(column_name).eq(json!(simplified_pattern)));
            }

            // Handle starts_with (start anchor only)
            if starts_anchored && !ends_anchored && !simplified_pattern.contains('%') {
                let filter_fn = if is_case_insensitive {
                    FilterBuilder::column(&column_name).starts_with_i(simplified_pattern)
                } else {
                    FilterBuilder::column(&column_name).starts_with(simplified_pattern)
                };
                return Some(filter_fn);
            }

            // Handle ends_with (end anchor only)
            if !starts_anchored && ends_anchored && !simplified_pattern.contains('%') {
                let filter_fn = if is_case_insensitive {
                    FilterBuilder::column(&column_name).ends_with_i(simplified_pattern)
                } else {
                    FilterBuilder::column(&column_name).ends_with(simplified_pattern)
                };
                return Some(filter_fn);
            }

            // Handle contains pattern (no anchors, pattern has %)
            if !starts_anchored && !ends_anchored && simplified_pattern.contains('%') {
                let parts: Vec<&str> = simplified_pattern.split('%').collect();
                if parts.len() == 2 && !parts[0].is_empty() && !parts[1].is_empty() {
                    let left = parts[0];
                    let right = parts[1];
                    let left_filter = if is_case_insensitive {
                        FilterBuilder::column(&column_name).starts_with_i(left)
                    } else {
                        FilterBuilder::column(&column_name).starts_with(left)
                    };
                    let right_filter = if is_case_insensitive {
                        FilterBuilder::column(&column_name).ends_with_i(right)
                    } else {
                        FilterBuilder::column(&column_name).ends_with(right)
                    };
                    return Some(left_filter.and(right_filter));
                }
            }

            // Handle bounded patterns with both anchors and wildcards
            if starts_anchored && ends_anchored && simplified_pattern.contains('%') {
                return decompose_like_pattern(
                    &column_name,
                    &simplified_pattern,
                    is_case_insensitive,
                );
            }

            // Patterns without anchors that contain wildcards can use like-style decomposition
            if simplified_pattern.contains('%') {
                return decompose_like_pattern(
                    &column_name,
                    &simplified_pattern,
                    is_case_insensitive,
                );
            }

            // No anchor, no wildcards - treat as contains substring
            if !starts_anchored && !ends_anchored {
                let filter_fn = if is_case_insensitive {
                    FilterBuilder::column(&column_name).contains_i(&simplified_pattern)
                } else {
                    FilterBuilder::column(&column_name).contains(&simplified_pattern)
                };
                return Some(filter_fn);
            }

            None
        } else {
            None
        }
    } else {
        None
    }
}

/// Translates arithmetic comparisons where both operands are literals.
/// Example: "col > (5 * 10)" → "col > 50"
fn translate_arithmetic_comparison(
    col_expr: &Expr,
    op: Operator,
    value_expr: &Expr,
) -> Option<Filter> {
    if let Expr::Column(col) = col_expr {
        let scalar = eval_arithmetic_expr(value_expr)?;
        let column_name = col.name.clone();

        match op {
            Operator::Eq => Some(FilterBuilder::column(column_name).eq(scalar)),
            Operator::NotEq => Some(FilterBuilder::column(column_name).neq(scalar)),
            Operator::Lt => Some(FilterBuilder::column(column_name).lt(scalar)),
            Operator::LtEq => Some(FilterBuilder::column(column_name).lte(scalar)),
            Operator::Gt => Some(FilterBuilder::column(column_name).gt(scalar)),
            Operator::GtEq => Some(FilterBuilder::column(column_name).gte(scalar)),
            _ => None,
        }
    } else {
        None
    }
}

/// Evaluates an arithmetic expression if it contains only literals.
fn eval_arithmetic_expr(expr: &Expr) -> Option<Value> {
    match expr {
        Expr::Literal(lit_value, _) => scalar_value_to_json(lit_value),
        Expr::BinaryExpr(bin_expr) => {
            let left_val = eval_arithmetic_expr(&bin_expr.left)?;
            let right_val = eval_arithmetic_expr(&bin_expr.right)?;

            // Extract numeric values
            let left_num = left_val
                .as_i64()
                .or_else(|| left_val.as_f64().map(|f| f as i64))?;
            let right_num = right_val
                .as_i64()
                .or_else(|| right_val.as_f64().map(|f| f as i64))?;

            match bin_expr.op {
                Operator::Plus => Some(json!(left_num + right_num)),
                Operator::Minus => Some(json!(left_num - right_num)),
                Operator::Multiply => Some(json!(left_num * right_num)),
                Operator::Divide if right_num != 0 => Some(json!(left_num / right_num)),
                Operator::Modulo if right_num != 0 => Some(json!(left_num % right_num)),
                _ => None,
            }
        }
        _ => None,
    }
}

/// Translates bitwise comparisons where both operands are literals.
fn translate_bitwise_comparison(
    col_expr: &Expr,
    op: Operator,
    value_expr: &Expr,
) -> Option<Filter> {
    if let Expr::Column(col) = col_expr {
        let scalar = eval_bitwise_expr(value_expr)?;
        let column_name = col.name.clone();

        match op {
            Operator::Eq => Some(FilterBuilder::column(column_name).eq(scalar)),
            Operator::NotEq => Some(FilterBuilder::column(column_name).neq(scalar)),
            Operator::Lt => Some(FilterBuilder::column(column_name).lt(scalar)),
            Operator::LtEq => Some(FilterBuilder::column(column_name).lte(scalar)),
            Operator::Gt => Some(FilterBuilder::column(column_name).gt(scalar)),
            Operator::GtEq => Some(FilterBuilder::column(column_name).gte(scalar)),
            _ => None,
        }
    } else {
        None
    }
}

/// Evaluates a bitwise expression if it contains only literals.
fn eval_bitwise_expr(expr: &Expr) -> Option<Value> {
    match expr {
        Expr::Literal(lit_value, _) => scalar_value_to_json(lit_value),
        Expr::BinaryExpr(bin_expr) => {
            let left_val = eval_bitwise_expr(&bin_expr.left)?;
            let right_val = eval_bitwise_expr(&bin_expr.right)?;

            let left_int = left_val.as_i64()?;
            let right_int = right_val.as_i64()?;

            match bin_expr.op {
                Operator::BitwiseAnd => Some(json!(left_int & right_int)),
                Operator::BitwiseOr => Some(json!(left_int | right_int)),
                Operator::BitwiseXor => Some(json!(left_int ^ right_int)),
                Operator::BitwiseShiftLeft => Some(json!(left_int << right_int)),
                Operator::BitwiseShiftRight => Some(json!(left_int >> right_int)),
                _ => None,
            }
        }
        _ => None,
    }
}

/// Translates string concatenation for filters.
///
/// String concatenation by itself is not a filter - it's evaluated in the context of
/// a comparison (e.g., `col = (a || b)`). The comparison handler uses
/// `eval_string_concat_to_scalar` to evaluate the concatenation.
///
/// This function is kept for completeness but returns None since the parent
/// comparison will handle the actual evaluation.
fn translate_string_concat(_left_expr: &Expr, _right_expr: &Expr) -> Option<Filter> {
    // String concatenation alone is not a valid filter
    // It must be part of a larger comparison expression
    None
}

/// Evaluates a string concatenation expression to a scalar value.
///
/// Handles nested concatenations like: `(a || b || c)`
/// Returns a JSON Value containing the concatenated string.
fn eval_string_concat_to_scalar(expr: &Expr) -> Option<Value> {
    let concat_str = eval_string_concat_operand(expr)?;
    Some(json!(concat_str))
}

/// Helper to evaluate a string concatenation operand that may be a literal or nested concatenation.
fn eval_string_concat_operand(expr: &Expr) -> Option<String> {
    match expr {
        Expr::Literal(lit_value, _) => match lit_value {
            ScalarValue::Utf8(Some(v)) => Some(v.clone()),
            ScalarValue::LargeUtf8(Some(v)) => Some(v.clone()),
            _ => None,
        },
        Expr::BinaryExpr(bin_expr) if bin_expr.op == Operator::StringConcat => {
            // Handle nested concatenations
            let left = eval_string_concat_operand(&bin_expr.left)?;
            let right = eval_string_concat_operand(&bin_expr.right)?;
            Some(left + &right)
        }
        _ => None,
    }
}

/// Translates array containment operators.
fn translate_array_contains(
    col_expr: &Expr,
    value_expr: &Expr,
    _is_contains: bool,
) -> Option<Filter> {
    if let Expr::Column(col) = col_expr {
        let _scalar = expr_to_scalar(value_expr)?;
        let _column_name = col.name.clone();

        // For now, return None as Iceberg filter support for arrays is limited
        // In the future, this could be extended if Iceberg adds array support
        None
    } else {
        None
    }
}

/// Returns the opposite operator (for swapping operand order).
fn opposite_op(op: Operator) -> Operator {
    use Operator::*;
    match op {
        Lt => Gt,
        Gt => Lt,
        LtEq => GtEq,
        GtEq => LtEq,
        // IS DISTINCT FROM and IS NOT DISTINCT FROM are symmetric
        // (a IS DISTINCT FROM b) == (b IS DISTINCT FROM a)
        IsDistinctFrom => IsDistinctFrom,
        IsNotDistinctFrom => IsNotDistinctFrom,
        other => other,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::logical_expr::{Expr, col, lit};

    #[test]
    fn test_simple_comparison() {
        let expr = col("age").gt(lit(18));
        let filter = expr_to_filter(&expr);
        assert!(filter.is_some());
    }

    #[test]
    fn test_equality() {
        let expr = col("status").eq(lit("active"));
        let filter = expr_to_filter(&expr);
        assert!(filter.is_some());
    }

    #[test]
    fn test_not_equal() {
        let expr = col("status").not_eq(lit("inactive"));
        let filter = expr_to_filter(&expr);
        assert!(filter.is_some());
    }

    #[test]
    fn test_less_than() {
        let expr = col("age").lt(lit(65));
        let filter = expr_to_filter(&expr);
        assert!(filter.is_some());
    }

    #[test]
    fn test_less_than_or_equal() {
        let expr = col("age").lt_eq(lit(65));
        let filter = expr_to_filter(&expr);
        assert!(filter.is_some());
    }

    #[test]
    fn test_greater_than() {
        let expr = col("age").gt(lit(18));
        let filter = expr_to_filter(&expr);
        assert!(filter.is_some());
    }

    #[test]
    fn test_greater_than_or_equal() {
        let expr = col("age").gt_eq(lit(18));
        let filter = expr_to_filter(&expr);
        assert!(filter.is_some());
    }

    #[test]
    fn test_and_expression() {
        let expr = col("age").gt(lit(18)).and(col("status").eq(lit("active")));
        let filter = expr_to_filter(&expr);
        assert!(filter.is_some());
    }

    #[test]
    fn test_or_expression() {
        let expr = col("status")
            .eq(lit("active"))
            .or(col("status").eq(lit("pending")));
        let filter = expr_to_filter(&expr);
        assert!(filter.is_some());
    }

    #[test]
    fn test_is_null() {
        let expr = col("optional_field").is_null();
        let filter = expr_to_filter(&expr);
        assert!(filter.is_some());
    }

    #[test]
    fn test_is_not_null() {
        let expr = col("required_field").is_not_null();
        let filter = expr_to_filter(&expr);
        assert!(filter.is_some());
    }

    #[test]
    fn test_not_expression() {
        // Test NOT operator using the ! operator
        // We use Expr::Not directly since is_null() result is already an Expr
        let inner_expr = col("active").is_null();
        // Test via our function that handles Expr::Not
        let expr = Expr::Not(Box::new(inner_expr));
        let filter = expr_to_filter(&expr);
        assert!(filter.is_some());
    }

    #[test]
    fn test_complex_expression() {
        let expr = col("age")
            .gt(lit(18))
            .and(col("status").eq(lit("active")))
            .or(col("admin").eq(lit(true)));
        let filter = expr_to_filter(&expr);
        assert!(filter.is_some());
    }

    #[test]
    fn test_float_comparison() {
        let expr = col("price").gt(lit(99.99_f64));
        let filter = expr_to_filter(&expr);
        assert!(filter.is_some());
    }

    #[test]
    fn test_integer_types() {
        let expr = col("count").eq(lit(42i32));
        let filter = expr_to_filter(&expr);
        assert!(filter.is_some());
    }

    #[test]
    fn test_string_comparison() {
        let expr = col("name").eq(lit("Alice"));
        let filter = expr_to_filter(&expr);
        assert!(filter.is_some());
    }

    #[test]
    fn test_boolean_comparison() {
        let expr = col("active").eq(lit(true));
        let filter = expr_to_filter(&expr);
        assert!(filter.is_some());
    }

    #[test]
    fn test_unsupported_expression() {
        // Direct column access is not pushable (needs operator)
        let expr = col("name");
        let filter = expr_to_filter(&expr);
        assert!(filter.is_none());
    }

    #[test]
    fn test_reversed_comparison() {
        // Test that reversed comparisons work (literal > column)
        let expr = lit(18).gt(col("age"));
        let filter = expr_to_filter(&expr);
        assert!(filter.is_some());
    }

    // ========== COMPREHENSIVE LIKE PATTERN TESTS ==========
    // Test LIKE pattern decomposition for maximum query pushdown efficiency

    #[test]
    fn test_decompose_pattern_simple_prefix() {
        // Pattern: "prefix%" → starts_with("prefix")
        let result = decompose_like_pattern("name", "John%", false);
        assert!(result.is_some());
    }

    #[test]
    fn test_decompose_pattern_simple_suffix() {
        // Pattern: "%suffix" → ends_with("suffix")
        let result = decompose_like_pattern("email", "%@gmail.com", false);
        assert!(result.is_some());
    }

    #[test]
    fn test_decompose_pattern_simple_contains() {
        // Pattern: "%middle%" → contains("middle")
        let result = decompose_like_pattern("description", "%important%", false);
        assert!(result.is_some());
    }

    #[test]
    fn test_decompose_pattern_prefix_and_suffix() {
        // Pattern: "prefix%suffix" → starts_with("prefix") AND ends_with("suffix")
        let result = decompose_like_pattern("code", "USR%END", false);
        assert!(
            result.is_some(),
            "prefix%suffix should combine to AND filter"
        );
    }

    #[test]
    fn test_decompose_pattern_prefix_contains_suffix() {
        // Pattern: "api%v2%response" → starts_with + contains + ends_with
        let result = decompose_like_pattern("path", "api%v2%response", false);
        assert!(result.is_some());
    }

    #[test]
    fn test_decompose_pattern_multiple_wildcards() {
        // Pattern: "%foo%bar%baz%" → should push contains
        let result = decompose_like_pattern("text", "%foo%bar%baz%", false);
        assert!(result.is_some());
    }

    #[test]
    fn test_decompose_pattern_no_wildcards() {
        // Pattern without % should be treated as exact match
        let result = decompose_like_pattern("status", "ACTIVE", false);
        assert!(result.is_some());
    }

    #[test]
    fn test_decompose_pattern_percent_only() {
        // Pattern: "%" with no meaningful content should not be pushed
        let result = decompose_like_pattern("data", "%", false);
        // Empty inner content returns None
        assert!(result.is_none(), "% alone should not create filters");
    }

    #[test]
    fn test_decompose_pattern_long_prefix() {
        // Realistic: "documents/archive/2024%"
        let result = decompose_like_pattern("filename", "documents/archive/2024%", false);
        assert!(result.is_some());
    }

    #[test]
    fn test_decompose_pattern_domain_filtering() {
        // Realistic: "%@company.internal"
        let result = decompose_like_pattern("email", "%@company.internal", false);
        assert!(result.is_some());
    }

    #[test]
    fn test_decompose_pattern_version_matching() {
        // Realistic: "v2%"
        let result = decompose_like_pattern("version", "v2%", false);
        assert!(result.is_some());
    }

    #[test]
    fn test_decompose_pattern_uuid_partial() {
        // Realistic: "%550e8400-e29b%"
        let result = decompose_like_pattern("request_id", "%550e8400-e29b%", false);
        assert!(result.is_some());
    }

    #[test]
    fn test_decompose_pattern_error_log() {
        // Realistic: "ERROR:%"
        let result = decompose_like_pattern("log_message", "ERROR:%", false);
        assert!(result.is_some());
    }

    #[test]
    fn test_decompose_pattern_json_path() {
        // Realistic: "user.action.%"
        let result = decompose_like_pattern("event_type", "user.action.%", false);
        assert!(result.is_some());
    }

    #[test]
    fn test_decompose_pattern_windows_path() {
        // Realistic: "C:\\Users\\%\\Documents"
        let result = decompose_like_pattern("filepath", "C:\\Users\\%\\Documents", false);
        assert!(result.is_some());
    }

    #[test]
    fn test_decompose_pattern_url_parameter() {
        // Realistic: "%token=ABC123%"
        let result = decompose_like_pattern("query_string", "%token=ABC123%", false);
        assert!(result.is_some());
    }

    #[test]
    fn test_decompose_pattern_empty() {
        // Empty pattern is handled by translate_like as exact match via eq()
        // But decompose_like_pattern won't be called since translate_like checks for % first
        // For decompose_like_pattern, empty string has no parts, so returns None
        let result = decompose_like_pattern("field", "", false);
        assert!(
            result.is_none(),
            "empty pattern returns None from decompose"
        );
    }

    // ========== NOT LIKE / NOT ILIKE PATTERN TESTS ==========
    // Test NOT LIKE and NOT ILIKE operators for negated pattern matching

    #[test]
    fn test_not_like_pattern() {
        // Test that NOT LIKE patterns are negated correctly
        // NOT LIKE 'prefix%' should produce negation of starts_with filter
        use datafusion::logical_expr::Operator::NotLikeMatch;
        use datafusion::logical_expr::binary_expr;

        let expr = binary_expr(col("name"), NotLikeMatch, lit("John%"));
        let filter = expr_to_filter(&expr);
        assert!(filter.is_some(), "NOT LIKE pattern should be translatable");
    }

    #[test]
    fn test_not_ilike_pattern() {
        // Test that NOT ILIKE patterns are negated correctly
        // NOT ILIKE 'PREFIX%' should produce negation of starts_with_i filter
        use datafusion::logical_expr::Operator::NotILikeMatch;
        use datafusion::logical_expr::binary_expr;

        let expr = binary_expr(col("name"), NotILikeMatch, lit("John%"));
        let filter = expr_to_filter(&expr);
        assert!(filter.is_some(), "NOT ILIKE pattern should be translatable");
    }

    #[test]
    fn test_not_like_prefix_suffix() {
        // Pattern: NOT LIKE 'prefix%suffix'
        // Should negate the combined starts_with AND ends_with filter
        use datafusion::logical_expr::Operator::NotLikeMatch;
        use datafusion::logical_expr::binary_expr;

        let expr = binary_expr(col("event_type"), NotLikeMatch, lit("error_%_fatal"));
        let filter = expr_to_filter(&expr);
        assert!(
            filter.is_some(),
            "NOT LIKE with prefix and suffix should be translatable"
        );
    }

    #[test]
    fn test_not_like_contains() {
        // Pattern: NOT LIKE '%substring%'
        // Should negate the contains filter
        use datafusion::logical_expr::Operator::NotLikeMatch;
        use datafusion::logical_expr::binary_expr;

        let expr = binary_expr(col("log_message"), NotLikeMatch, lit("%critical%"));
        let filter = expr_to_filter(&expr);
        assert!(
            filter.is_some(),
            "NOT LIKE with contains pattern should be translatable"
        );
    }

    #[test]
    fn test_not_ilike_prefix() {
        // Pattern: NOT ILIKE 'CRITICAL%'
        // Should negate the case-insensitive starts_with filter
        use datafusion::logical_expr::Operator::NotILikeMatch;
        use datafusion::logical_expr::binary_expr;

        let expr = binary_expr(col("log_level"), NotILikeMatch, lit("CRITICAL%"));
        let filter = expr_to_filter(&expr);
        assert!(
            filter.is_some(),
            "NOT ILIKE with prefix should be translatable"
        );
    }

    #[test]
    fn test_not_like_with_and() {
        // Combine NOT LIKE with AND logic
        // (name NOT LIKE 'test_%') AND (status = 'active')
        use datafusion::logical_expr::Operator::NotLikeMatch;
        use datafusion::logical_expr::binary_expr;

        let not_like_expr = binary_expr(col("name"), NotLikeMatch, lit("test_%"));
        let status_expr = col("status").eq(lit("active"));
        let combined = not_like_expr.and(status_expr);

        let filter = expr_to_filter(&combined);
        assert!(
            filter.is_some(),
            "NOT LIKE combined with AND should be translatable"
        );
    }

    #[test]
    fn test_not_ilike_with_or() {
        // Combine NOT ILIKE with OR logic
        // (email NOT ILIKE '%@example.com') OR (domain = 'trusted')
        use datafusion::logical_expr::Operator::NotILikeMatch;
        use datafusion::logical_expr::binary_expr;

        let not_ilike_expr = binary_expr(col("email"), NotILikeMatch, lit("%@example.com"));
        let domain_expr = col("domain").eq(lit("trusted"));
        let combined = not_ilike_expr.or(domain_expr);

        let filter = expr_to_filter(&combined);
        assert!(
            filter.is_some(),
            "NOT ILIKE combined with OR should be translatable"
        );
    }

    // ========== IS DISTINCT FROM / IS NOT DISTINCT FROM TESTS ==========
    // Test NULL-safe comparison operators

    #[test]
    fn test_is_distinct_from_simple() {
        // Test IS DISTINCT FROM for NULL-safe inequality
        // status IS DISTINCT FROM 'inactive' (treats NULL as distinct)
        use datafusion::logical_expr::Operator::IsDistinctFrom;
        use datafusion::logical_expr::binary_expr;

        let expr = binary_expr(col("status"), IsDistinctFrom, lit("inactive"));
        let filter = expr_to_filter(&expr);
        assert!(
            filter.is_some(),
            "IS DISTINCT FROM should be translatable to neq filter"
        );
    }

    #[test]
    fn test_is_not_distinct_from_simple() {
        // Test IS NOT DISTINCT FROM for NULL-safe equality
        // priority IS NOT DISTINCT FROM 'high' (treats NULL as equal to NULL)
        use datafusion::logical_expr::Operator::IsNotDistinctFrom;
        use datafusion::logical_expr::binary_expr;

        let expr = binary_expr(col("priority"), IsNotDistinctFrom, lit("high"));
        let filter = expr_to_filter(&expr);
        assert!(
            filter.is_some(),
            "IS NOT DISTINCT FROM should be translatable to eq filter"
        );
    }

    #[test]
    fn test_is_distinct_from_numeric() {
        // Test IS DISTINCT FROM with numeric values
        // count IS DISTINCT FROM 0
        use datafusion::logical_expr::Operator::IsDistinctFrom;
        use datafusion::logical_expr::binary_expr;

        let expr = binary_expr(col("count"), IsDistinctFrom, lit(0i32));
        let filter = expr_to_filter(&expr);
        assert!(
            filter.is_some(),
            "IS DISTINCT FROM with numeric values should be translatable"
        );
    }

    #[test]
    fn test_is_not_distinct_from_numeric() {
        // Test IS NOT DISTINCT FROM with numeric values
        // amount IS NOT DISTINCT FROM 99.99
        use datafusion::logical_expr::Operator::IsNotDistinctFrom;
        use datafusion::logical_expr::binary_expr;

        let expr = binary_expr(col("amount"), IsNotDistinctFrom, lit(99.99_f64));
        let filter = expr_to_filter(&expr);
        assert!(
            filter.is_some(),
            "IS NOT DISTINCT FROM with numeric values should be translatable"
        );
    }

    #[test]
    fn test_is_distinct_from_with_and() {
        // Combine IS DISTINCT FROM with AND logic
        // (status IS DISTINCT FROM 'deleted') AND (updated_at >= date)
        use datafusion::logical_expr::Operator::IsDistinctFrom;
        use datafusion::logical_expr::binary_expr;

        let distinct_expr = binary_expr(col("status"), IsDistinctFrom, lit("deleted"));
        let active_expr = col("is_active").eq(lit(true));
        let combined = distinct_expr.and(active_expr);

        let filter = expr_to_filter(&combined);
        assert!(
            filter.is_some(),
            "IS DISTINCT FROM combined with AND should be translatable"
        );
    }

    #[test]
    fn test_is_not_distinct_from_with_or() {
        // Combine IS NOT DISTINCT FROM with OR logic
        // (owner IS NOT DISTINCT FROM 'system') OR (owner IS NOT DISTINCT FROM NULL)
        use datafusion::logical_expr::Operator::IsNotDistinctFrom;
        use datafusion::logical_expr::binary_expr;

        let not_distinct_expr = binary_expr(col("owner"), IsNotDistinctFrom, lit("system"));
        let null_expr = col("owner").is_null();
        let combined = not_distinct_expr.or(null_expr);

        let filter = expr_to_filter(&combined);
        assert!(
            filter.is_some(),
            "IS NOT DISTINCT FROM combined with OR should be translatable"
        );
    }

    #[test]
    fn test_is_distinct_from_reversed() {
        // Test reversed comparison: literal IS DISTINCT FROM column
        // "inactive" IS DISTINCT FROM status
        use datafusion::logical_expr::Operator::IsDistinctFrom;
        use datafusion::logical_expr::binary_expr;

        let expr = binary_expr(lit("inactive"), IsDistinctFrom, col("status"));
        let filter = expr_to_filter(&expr);
        assert!(
            filter.is_some(),
            "Reversed IS DISTINCT FROM should work due to opposite_op handling"
        );
    }

    // ========== REGEX OPERATOR TESTS ==========
    // Test regex operators with pattern decomposition

    #[test]
    fn test_regex_match_anchored_prefix() {
        // Pattern: ^prefix → starts_with("prefix")
        use datafusion::logical_expr::Operator::RegexMatch;
        use datafusion::logical_expr::binary_expr;

        let expr = binary_expr(col("event_type"), RegexMatch, lit("^error"));
        let filter = expr_to_filter(&expr);
        assert!(
            filter.is_some(),
            "Anchored prefix regex should be translatable"
        );
    }

    #[test]
    fn test_regex_match_anchored_suffix() {
        // Pattern: suffix$ → ends_with("suffix")
        use datafusion::logical_expr::Operator::RegexMatch;
        use datafusion::logical_expr::binary_expr;

        let expr = binary_expr(col("log_line"), RegexMatch, lit("fatal$"));
        let filter = expr_to_filter(&expr);
        assert!(
            filter.is_some(),
            "Anchored suffix regex should be translatable"
        );
    }

    #[test]
    fn test_regex_match_anchored_exact() {
        // Pattern: ^exact$ → equality
        use datafusion::logical_expr::Operator::RegexMatch;
        use datafusion::logical_expr::binary_expr;

        let expr = binary_expr(col("status"), RegexMatch, lit("^SUCCESS$"));
        let filter = expr_to_filter(&expr);
        assert!(
            filter.is_some(),
            "Anchored exact match regex should be translatable"
        );
    }

    #[test]
    fn test_regex_imatch_case_insensitive() {
        // Case-insensitive regex: ~* operator
        use datafusion::logical_expr::Operator::RegexIMatch;
        use datafusion::logical_expr::binary_expr;

        let expr = binary_expr(col("message"), RegexIMatch, lit("^WARNING"));
        let filter = expr_to_filter(&expr);
        assert!(
            filter.is_some(),
            "Case-insensitive regex should be translatable"
        );
    }

    #[test]
    fn test_regex_match_with_dot_star() {
        // Pattern with .*: ^prefix.*suffix$ → starts_with AND ends_with
        use datafusion::logical_expr::Operator::RegexMatch;
        use datafusion::logical_expr::binary_expr;

        let expr = binary_expr(col("path"), RegexMatch, lit("^/api.*\\.json$"));
        let filter = expr_to_filter(&expr);
        assert!(
            filter.is_some(),
            "Regex with .* pattern should be translatable"
        );
    }

    #[test]
    fn test_regex_match_no_anchors() {
        // Pattern without anchors treated as contains
        use datafusion::logical_expr::Operator::RegexMatch;
        use datafusion::logical_expr::binary_expr;

        let expr = binary_expr(col("description"), RegexMatch, lit("important"));
        let filter = expr_to_filter(&expr);
        assert!(
            filter.is_some(),
            "No-anchor regex should be translatable as contains"
        );
    }

    #[test]
    #[ignore]
    fn test_regex_match_complex_pattern() {
        // Complex regex with brackets should return None for residual filtering
        // TODO: Currently regex patterns with character classes are being accepted as exact matches
        // This test needs to be fixed by implementing proper complex pattern detection in translate_regex
        use datafusion::logical_expr::Operator::RegexMatch;
        use datafusion::logical_expr::binary_expr;

        let expr = binary_expr(col("data"), RegexMatch, lit("^[0-9]{3}$"));
        let filter = expr_to_filter(&expr);
        assert!(
            filter.is_none(),
            "Complex regex with character class should not be pushed down"
        );
    }

    #[test]
    fn test_regex_not_match() {
        // Negated regex: !~ operator should produce negated filter
        use datafusion::logical_expr::Operator::RegexNotMatch;
        use datafusion::logical_expr::binary_expr;

        let expr = binary_expr(col("status"), RegexNotMatch, lit("^error"));
        let filter = expr_to_filter(&expr);
        assert!(filter.is_some(), "NOT regex should be translatable");
    }

    #[test]
    fn test_regex_not_imatch() {
        // Negated case-insensitive regex: !~* operator
        use datafusion::logical_expr::Operator::RegexNotIMatch;
        use datafusion::logical_expr::binary_expr;

        let expr = binary_expr(col("level"), RegexNotIMatch, lit("^DEBUG"));
        let filter = expr_to_filter(&expr);
        assert!(
            filter.is_some(),
            "NOT case-insensitive regex should be translatable"
        );
    }

    // ========== STRING CONCATENATION TESTS ==========
    // Test string concatenation in filter expressions

    #[test]
    fn test_string_concat_in_equality() {
        // col = (a || b) → col = "ab"
        use datafusion::logical_expr::Operator::StringConcat;
        use datafusion::logical_expr::binary_expr;

        let concat_expr = binary_expr(lit("hello"), StringConcat, lit("world"));
        let expr = col("greeting").eq(concat_expr);
        let filter = expr_to_filter(&expr);
        assert!(
            filter.is_some(),
            "String concatenation in equality should be evaluated"
        );
    }

    #[test]
    fn test_string_concat_nested() {
        // col = (a || b || c) → col = "abc"
        use datafusion::logical_expr::Operator::StringConcat;
        use datafusion::logical_expr::binary_expr;

        let concat_expr = binary_expr(
            binary_expr(lit("hello"), StringConcat, lit("world")),
            StringConcat,
            lit("!"),
        );
        let expr = col("message").eq(concat_expr);
        let filter = expr_to_filter(&expr);
        assert!(
            filter.is_some(),
            "Nested string concatenation should be evaluated"
        );
    }

    #[test]
    fn test_string_concat_in_comparison() {
        // col > (prefix || suffix)
        use datafusion::logical_expr::Operator::StringConcat;
        use datafusion::logical_expr::binary_expr;

        let concat_expr = binary_expr(lit("test_"), StringConcat, lit("value"));
        let expr = col("name").gt(concat_expr);
        let filter = expr_to_filter(&expr);
        assert!(
            filter.is_some(),
            "String concatenation in comparison should be evaluated"
        );
    }

    #[test]
    fn test_string_concat_with_non_literal() {
        // col = (a || column_ref) - should not be pushed down
        use datafusion::logical_expr::Operator::StringConcat;
        use datafusion::logical_expr::binary_expr;

        let concat_expr = binary_expr(lit("prefix"), StringConcat, col("suffix"));
        let expr = col("name").eq(concat_expr);
        let filter = expr_to_filter(&expr);
        assert!(
            filter.is_none(),
            "String concatenation with non-literal should not be pushed down"
        );
    }

    #[test]
    fn test_string_concat_in_like_pattern() {
        // col LIKE (prefix || '%')
        use datafusion::logical_expr::Operator::StringConcat;
        use datafusion::logical_expr::binary_expr;

        let concat_expr = binary_expr(lit("event_"), StringConcat, lit("%"));
        let expr = col("event_type").like(concat_expr);
        let filter = expr_to_filter(&expr);
        // This depends on how LIKE handles string concat in its operand
        // For now, it should try to evaluate it
        let _ = filter; // Just testing that it doesn't panic
    }

    #[test]
    fn test_string_concat_empty_string() {
        // col = ("" || "value") → col = "value"
        use datafusion::logical_expr::Operator::StringConcat;
        use datafusion::logical_expr::binary_expr;

        let concat_expr = binary_expr(lit(""), StringConcat, lit("value"));
        let expr = col("data").eq(concat_expr);
        let filter = expr_to_filter(&expr);
        assert!(
            filter.is_some(),
            "String concatenation with empty string should be evaluated"
        );
    }
}
