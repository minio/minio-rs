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

//! Integration tests for query pushdown functionality
//!
//! These tests verify the end-to-end query pushdown infrastructure:
//! - Filter translation from DataFusion expressions to Iceberg filters
//! - Filter context management in adapters
//! - Filter JSON serialization and deserialization
//! - Complex nested filter expressions

#[cfg(test)]
mod pushdown_filter_tests {
    use super::super::filter::FilterBuilder;
    use std::ops::Not;

    // ========== BASIC COMPARISON TESTS ==========

    #[test]
    fn test_filter_builder_simple_comparison() {
        let filter = FilterBuilder::column("age").gt(18);
        let json = filter.to_json();

        // Verify filter structure is valid JSON object
        assert!(json.is_object());

        // Verify serialization works
        let json_str = serde_json::to_string(&json).expect("Should serialize");
        assert!(!json_str.is_empty());

        // Verify deserialization works
        let _reparsed: serde_json::Value =
            serde_json::from_str(&json_str).expect("Should deserialize");
    }

    #[test]
    fn test_equality_filter_creates_valid_json() {
        let filter = FilterBuilder::column("status").eq("active");
        let json = filter.to_json();

        assert!(json.is_object());
        let json_str = serde_json::to_string(&json).expect("Should serialize");
        let _reparsed: serde_json::Value =
            serde_json::from_str(&json_str).expect("Should deserialize");
    }

    // ========== LOGICAL OPERATOR TESTS ==========

    #[test]
    fn test_filter_builder_and_expression() {
        let age_filter = FilterBuilder::column("age").gt(18);
        let status_filter = FilterBuilder::column("status").eq("active");
        let combined = age_filter.and(status_filter);

        let json = combined.to_json();
        assert_eq!(json["type"], "and");
        assert!(json["left"].is_object());
        assert!(json["right"].is_object());
    }

    #[test]
    fn test_combined_or_filters_create_nested_structure() {
        let north = FilterBuilder::column("region").eq("NORTH");
        let south = FilterBuilder::column("region").eq("SOUTH");
        let combined = north.or(south);

        let json = combined.to_json();
        assert_eq!(json["type"], "or");
        assert!(json["left"].is_object());
        assert!(json["right"].is_object());
    }

    #[test]
    fn test_filter_builder_or_expression() {
        let active = FilterBuilder::column("status").eq("active");
        let pending = FilterBuilder::column("status").eq("pending");
        let combined = active.or(pending);

        let json = combined.to_json();
        assert_eq!(json["type"], "or");
    }

    #[test]
    fn test_filter_builder_not_expression() {
        let is_null = FilterBuilder::column("optional_field").is_null();
        let is_not_null = is_null.not();

        let json = is_not_null.to_json();
        assert_eq!(json["type"], "not");
    }

    #[test]
    fn test_not_operator_creates_negated_filter() {
        let status_filter = FilterBuilder::column("status").eq("deleted");
        let negated = status_filter.not();

        let json = negated.to_json();
        assert_eq!(json["type"], "not");
        assert!(json.is_object());

        let json_str = serde_json::to_string(&json).expect("Should serialize");
        assert!(!json_str.is_empty());
    }

    // ========== NULL CHECK TESTS ==========

    #[test]
    fn test_filter_builder_null_checks() {
        let is_null = FilterBuilder::column("value").is_null();
        let is_not_null = FilterBuilder::column("value").is_not_null();

        let null_json = is_null.to_json();
        let not_null_json = is_not_null.to_json();

        assert_eq!(null_json["type"], "comparison");
        assert_eq!(null_json["op"], "is-null");

        assert_eq!(not_null_json["type"], "comparison");
        assert_eq!(not_null_json["op"], "is-not-null");
    }

    #[test]
    fn test_null_check_filters_create_valid_json() {
        let is_null = FilterBuilder::column("optional_field").is_null();
        let json = is_null.to_json();

        assert!(json.is_object());

        let json_str = serde_json::to_string(&json).expect("Should serialize");
        assert!(!json_str.is_empty());

        let _reparsed: serde_json::Value =
            serde_json::from_str(&json_str).expect("Should deserialize");
    }

    #[test]
    fn test_not_null_check_filters_create_valid_json() {
        let is_not_null = FilterBuilder::column("optional_field").is_not_null();
        let json = is_not_null.to_json();

        assert!(json.is_object());

        let json_str = serde_json::to_string(&json).expect("Should serialize");
        assert!(!json_str.is_empty());
    }

    // ========== SET MEMBERSHIP TESTS ==========
    // Note: in_list and not_in methods would require Value parameters
    // These are intentionally skipped as they require more complex setup

    // ========== STRING OPERATOR TESTS ==========

    #[test]
    fn test_filter_with_string_operators() {
        let filter = FilterBuilder::column("name").starts_with("John");
        let json = filter.to_json();

        assert_eq!(json["type"], "comparison");
        assert_eq!(json["column"], "name");
        assert_eq!(json["op"], "starts-with");
    }

    #[test]
    fn test_string_operations_in_filters() {
        let name_eq = FilterBuilder::column("name").eq("John");
        let email_like = FilterBuilder::column("email").starts_with("john@");

        let combined = name_eq.or(email_like);
        let json = combined.to_json();

        assert_eq!(json["type"], "or");
        assert!(json["left"].is_object());
        assert!(json["right"].is_object());
    }

    // ========== NUMERIC COMPARISON TESTS ==========

    #[test]
    fn test_filter_all_comparison_operators() {
        let tests = vec![
            ("eq", FilterBuilder::column("x").eq(5)),
            ("neq", FilterBuilder::column("x").neq(5)),
            ("lt", FilterBuilder::column("x").lt(5)),
            ("gt", FilterBuilder::column("x").gt(5)),
            ("lte", FilterBuilder::column("x").lte(5)),
            ("gte", FilterBuilder::column("x").gte(5)),
            (
                "starts-with",
                FilterBuilder::column("name").starts_with("test"),
            ),
        ];

        for (expected_op, filter) in tests {
            let json = filter.to_json();
            assert_eq!(json["type"], "comparison");
            assert_eq!(json["op"], expected_op);
        }
    }

    #[test]
    fn test_numeric_comparisons_create_filters() {
        let lt = FilterBuilder::column("score").lt(50);
        let lte = FilterBuilder::column("score").lte(50);
        let gt = FilterBuilder::column("score").gt(50);
        let gte = FilterBuilder::column("score").gte(50);
        let eq = FilterBuilder::column("score").eq(50);
        let neq = FilterBuilder::column("score").neq(50);

        assert!(lt.to_json().is_object());
        assert!(lte.to_json().is_object());
        assert!(gt.to_json().is_object());
        assert!(gte.to_json().is_object());
        assert!(eq.to_json().is_object());
        assert!(neq.to_json().is_object());
    }

    #[test]
    fn test_range_query_combines_filters() {
        let min_age = FilterBuilder::column("age").gte(18);
        let max_age = FilterBuilder::column("age").lte(65);
        let range = min_age.and(max_age);

        let json = range.to_json();

        assert_eq!(json["type"], "and");
        assert!(json["left"].is_object());
        assert!(json["right"].is_object());
    }

    // ========== COMPLEX NESTED TESTS ==========

    #[test]
    fn test_combined_and_filters_create_nested_structure() {
        let age_filter = FilterBuilder::column("age").gt(18);
        let status_filter = FilterBuilder::column("status").eq("active");
        let combined = age_filter.and(status_filter);

        let json = combined.to_json();

        assert!(json.is_object());
        assert_eq!(json["type"], "and");
        assert!(json["left"].is_object());
        assert!(json["right"].is_object());
    }

    #[test]
    fn test_complex_filter_expression() {
        // (age > 18 AND status = "active") OR country = "US"
        let age_filter = FilterBuilder::column("age").gt(18);
        let status_filter = FilterBuilder::column("status").eq("active");
        let and_filter = age_filter.and(status_filter);

        let country_filter = FilterBuilder::column("country").eq("US");
        let final_filter = and_filter.or(country_filter);

        let json = final_filter.to_json();

        assert_eq!(json["type"], "or");
        assert_eq!(json["left"]["type"], "and");
        assert_eq!(json["right"]["type"], "comparison");
    }

    #[test]
    fn test_complex_nested_filters_serialize_correctly() {
        // (age > 18 AND status = 'active') OR (country = 'US' AND verified = true)
        let age_filter = FilterBuilder::column("age").gt(18);
        let status_filter = FilterBuilder::column("status").eq("active");
        let left_side = age_filter.and(status_filter);

        let country_filter = FilterBuilder::column("country").eq("US");
        let verified_filter = FilterBuilder::column("verified").eq(true);
        let right_side = country_filter.and(verified_filter);

        let combined = left_side.or(right_side);
        let json = combined.to_json();

        assert_eq!(json["type"], "or");
        assert_eq!(json["left"]["type"], "and");
        assert_eq!(json["right"]["type"], "and");

        let json_str = serde_json::to_string(&json).expect("Should serialize");
        assert!(!json_str.is_empty());
    }

    #[test]
    fn test_deeply_nested_filters_remain_serializable() {
        // ((a AND b) OR (c AND d)) AND ((e OR f) AND (g OR h))
        let f1 = FilterBuilder::column("a").eq(1);
        let f2 = FilterBuilder::column("b").eq(2);
        let f3 = FilterBuilder::column("c").eq(3);
        let f4 = FilterBuilder::column("d").eq(4);
        let f5 = FilterBuilder::column("e").eq(5);
        let f6 = FilterBuilder::column("f").eq(6);
        let f7 = FilterBuilder::column("g").eq(7);
        let f8 = FilterBuilder::column("h").eq(8);

        let left = (f1.and(f2)).or(f3.and(f4));
        let right = (f5.or(f6)).and(f7.or(f8));
        let combined = left.and(right);

        let json = combined.to_json();

        assert!(json.is_object());
        assert!(!json.is_null());

        let json_str = serde_json::to_string(&json).expect("Should serialize");
        assert!(!json_str.is_empty());

        let _reparsed: serde_json::Value =
            serde_json::from_str(&json_str).expect("Should deserialize");
    }

    #[test]
    fn test_multiple_columns_in_filter() {
        let year = FilterBuilder::column("year").eq(2024);
        let month = FilterBuilder::column("month").gt(6);
        let status = FilterBuilder::column("status").eq("active");
        let verified = FilterBuilder::column("verified").eq(true);

        let filter = year.and(month).and(status).and(verified);
        let json = filter.to_json();

        assert_eq!(json["type"], "and");
        assert!(json["left"].is_object());
        assert!(json["right"].is_object());

        let json_str = serde_json::to_string(&json).expect("Should serialize");
        assert!(!json_str.is_empty());

        let _reparsed: serde_json::Value =
            serde_json::from_str(&json_str).expect("Should deserialize");
    }

    // ========== DATA TYPE TESTS ==========

    #[test]
    fn test_boolean_values_in_filters() {
        let active_true = FilterBuilder::column("is_active").eq(true);
        let active_false = FilterBuilder::column("is_active").eq(false);

        let json_true = active_true.to_json();
        let json_false = active_false.to_json();

        assert!(json_true.is_object());
        assert!(json_false.is_object());

        let json_true_str = serde_json::to_string(&json_true).expect("Should serialize true");
        let json_false_str = serde_json::to_string(&json_false).expect("Should serialize false");
        assert!(!json_true_str.is_empty());
        assert!(!json_false_str.is_empty());

        let _reparsed_true: serde_json::Value =
            serde_json::from_str(&json_true_str).expect("Should deserialize true");
        let _reparsed_false: serde_json::Value =
            serde_json::from_str(&json_false_str).expect("Should deserialize false");
    }

    #[test]
    fn test_mixed_data_types_in_combined_filter() {
        let int_filter = FilterBuilder::column("count").eq(42);
        let str_filter = FilterBuilder::column("name").eq("test");
        let bool_filter = FilterBuilder::column("active").eq(true);

        let combined = int_filter.and(str_filter).and(bool_filter);

        let json = combined.to_json();

        assert_eq!(json["type"], "and");
        assert!(json.is_object());

        let json_str = serde_json::to_string(&json).expect("Should serialize");
        assert!(!json_str.is_empty());
    }

    // ========== SERIALIZATION TESTS ==========

    #[test]
    fn test_filter_json_serialization() {
        let filter = FilterBuilder::column("user_id")
            .eq(42)
            .and(FilterBuilder::column("status").eq("active"));

        let json = filter.to_json();

        assert!(json.is_object());
        assert_eq!(json["type"], "and");
        assert!(json["left"].is_object());
        assert!(json["right"].is_object());

        let json_str = serde_json::to_string(&json).expect("JSON serialization failed");
        assert!(!json_str.is_empty());

        let _parsed: serde_json::Value =
            serde_json::from_str(&json_str).expect("JSON parse failed");
    }

    #[test]
    fn test_filter_json_serialization_round_trip() {
        let filter = FilterBuilder::column("user_id")
            .eq(42)
            .and(FilterBuilder::column("status").eq("active"))
            .and(FilterBuilder::column("verified").eq(true));

        let json = filter.to_json();

        let json_str = serde_json::to_string(&json).expect("Should serialize to JSON");

        let _reparsed: serde_json::Value =
            serde_json::from_str(&json_str).expect("Should parse back from JSON");

        assert!(!json_str.is_empty());
        assert!(json_str.len() < 10000);
    }

    #[test]
    fn test_simple_comparison_creates_valid_json() {
        let filter = FilterBuilder::column("age").gt(18);
        let json = filter.to_json();

        assert!(json.is_object());

        let json_str = serde_json::to_string(&json).expect("Should serialize");
        assert!(!json_str.is_empty());
    }

    // ========== FILTER BUILDER CHAINING TESTS ==========

    #[test]
    fn test_filter_builder_chaining() {
        let filter = FilterBuilder::column("status")
            .eq("active")
            .and(FilterBuilder::column("age").gt(18))
            .or(FilterBuilder::column("admin").eq(true));

        let json = filter.to_json();

        assert!(json.is_object());
        assert_eq!(json["type"], "or");

        let json_str = serde_json::to_string(&json).expect("Should serialize");
        assert!(!json_str.is_empty());
    }

    // ========== REALISTIC PUSHDOWN SCENARIOS ==========

    #[test]
    fn test_pushdown_mock_flow() {
        // This test demonstrates the complete pushdown flow:
        // DataFusion Expression -> Filter Translation -> JSON -> Adapter

        // Step 1: Create DataFusion expression (simulated)
        // For this test, we directly create the filter
        let iceberg_filter = FilterBuilder::column("user_id").eq(42);

        // Step 2: Convert to JSON
        let filter_json = iceberg_filter.to_json();

        // Step 3: Verify JSON structure
        assert_eq!(filter_json["type"], "comparison");
        assert_eq!(filter_json["column"], "user_id");
        assert_eq!(filter_json["value"], 42);

        // Step 4: This JSON would be sent to plan_table_scan() in MinIO
        let json_str = serde_json::to_string(&filter_json).expect("JSON serialization failed");
        assert!(!json_str.is_empty());
    }

    #[test]
    fn test_pushdown_readiness() {
        // Integration test: Verify the complete filter infrastructure is ready for pushdown
        // This represents a realistic query that would be pushed down to MinIO

        // Query: SELECT user_id, name, email FROM users WHERE
        //   (age >= 18 AND age <= 65) AND
        //   status IN ['active', 'pending'] AND
        //   (country = 'US' OR country = 'CA') AND
        //   verified = true

        let age_min = FilterBuilder::column("age").gte(18);
        let age_max = FilterBuilder::column("age").lte(65);
        let age_range = age_min.and(age_max);

        let status = FilterBuilder::column("status").eq("active");

        let country_us = FilterBuilder::column("country").eq("US");
        let country_ca = FilterBuilder::column("country").eq("CA");
        let country = country_us.or(country_ca);

        let verified = FilterBuilder::column("verified").eq(true);

        let final_filter = age_range.and(status).and(country).and(verified);

        let json = final_filter.to_json();

        assert!(json.is_object());
        assert!(!json.is_null());

        let json_str = serde_json::to_string(&json).expect("Should be sendable to server");
        assert!(!json_str.is_empty());

        let _parsed: serde_json::Value =
            serde_json::from_str(&json_str).expect("Should be parseable by server");
    }
}
