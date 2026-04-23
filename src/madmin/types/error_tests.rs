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

//! Error path tests for madmin types
//!
//! This module tests error handling and edge cases in deserialization,
//! validation, and type conversions using generic test structures.

#[cfg(test)]
mod tests {
    use serde::{Deserialize, Serialize};
    use std::collections::HashMap;

    // Define simple test structs for generic testing
    #[derive(Debug, Deserialize, Serialize)]
    struct TestStruct {
        name: String,
        count: i32,
    }

    #[derive(Debug, Deserialize, Serialize)]
    struct TestWithOptional {
        required: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        optional: Option<String>,
    }

    // Test invalid JSON deserialization
    #[test]
    fn test_invalid_json() {
        let invalid_json = "{not valid json";
        let result: Result<TestStruct, _> = serde_json::from_str(invalid_json);
        assert!(result.is_err(), "Should fail on invalid JSON");
    }

    #[test]
    fn test_malformed_json_missing_fields() {
        let malformed_json = r#"{"name": "test"}"#; // missing 'count'
        let result: Result<TestStruct, _> = serde_json::from_str(malformed_json);
        assert!(result.is_err(), "Should fail on missing required fields");
    }

    #[test]
    fn test_empty_json_object() {
        let empty_json = "{}";
        let result: Result<TestStruct, _> = serde_json::from_str(empty_json);
        assert!(
            result.is_err(),
            "Should fail on empty object with required fields"
        );
    }

    #[test]
    fn test_null_in_required_fields() {
        let null_field = r#"{"name": null, "count": 1}"#;
        let result: Result<TestStruct, _> = serde_json::from_str(null_field);
        assert!(result.is_err(), "Should fail when required field is null");
    }

    #[test]
    fn test_unicode_in_strings() {
        let unicode_json = r#"{"name": "测试🚀", "count": 42}"#;
        let result: Result<TestStruct, _> = serde_json::from_str(unicode_json);
        assert!(result.is_ok(), "Should handle Unicode characters");
    }

    #[test]
    fn test_array_instead_of_object() {
        let array_json = r#"["not", "an", "object"]"#;
        let result: Result<TestStruct, _> = serde_json::from_str(array_json);
        assert!(
            result.is_err(),
            "Should fail when array provided instead of object"
        );
    }

    #[test]
    fn test_empty_arrays() {
        let json_with_empty_array = r#"{"items": []}"#;
        let result: Result<HashMap<String, Vec<String>>, _> =
            serde_json::from_str(json_with_empty_array);
        assert!(result.is_ok(), "Should handle empty arrays");
    }

    #[test]
    fn test_missing_optional_fields() {
        let minimal_json = r#"{"required": "value"}"#;
        let result: Result<TestWithOptional, _> = serde_json::from_str(minimal_json);
        assert!(
            result.is_ok(),
            "Optional fields should be handled gracefully"
        );
        if let Ok(data) = result {
            assert_eq!(data.required, "value");
            assert!(data.optional.is_none());
        }
    }

    #[test]
    fn test_extra_unknown_fields() {
        let extra_fields = r#"{
            "name": "test",
            "count": 42,
            "unknown_field_1": "value1",
            "unknown_field_2": 123
        }"#;
        let result: Result<TestStruct, _> = serde_json::from_str(extra_fields);
        assert!(result.is_ok(), "Should ignore unknown fields");
    }

    #[test]
    fn test_type_mismatch_string_as_number() {
        let type_mismatch = r#"{"name": "test", "count": "not_a_number"}"#;
        let result: Result<TestStruct, _> = serde_json::from_str(type_mismatch);
        assert!(result.is_err(), "Should fail on type mismatch");
    }

    #[test]
    fn test_special_characters_in_keys() {
        let special_chars = r#"{"key-with-dash": "value", "key.with.dot": "value"}"#;
        let result: Result<HashMap<String, String>, _> = serde_json::from_str(special_chars);
        assert!(result.is_ok(), "Should handle special characters in keys");
    }

    #[test]
    fn test_boolean_string_coercion() {
        let bool_strings = r#"{"enabled": "true"}"#;
        let result: Result<HashMap<String, bool>, _> = serde_json::from_str(bool_strings);
        assert!(result.is_err(), "Should not coerce string to boolean");
    }

    #[test]
    fn test_empty_string_values() {
        let empty_strings = r#"{"name": "", "count": 0}"#;
        let result: Result<TestStruct, _> = serde_json::from_str(empty_strings);
        assert!(result.is_ok(), "Empty strings should be valid");
    }

    #[test]
    fn test_floating_point_precision() {
        let precise_float = r#"{"value": 1.234567890123456789}"#;
        let result: Result<HashMap<String, f64>, _> = serde_json::from_str(precise_float);
        assert!(result.is_ok(), "Should handle floating point precision");
    }

    #[test]
    fn test_large_numbers() {
        let large_num = r#"{"count": 9223372036854775807}"#; // i64::MAX
        let result: Result<HashMap<String, i64>, _> = serde_json::from_str(large_num);
        assert!(result.is_ok(), "Should handle large numbers");
    }

    #[test]
    fn test_negative_numbers() {
        let negative = r#"{"name": "test", "count": -42}"#;
        let result: Result<TestStruct, _> = serde_json::from_str(negative);
        assert!(result.is_ok(), "Should handle negative numbers");
    }

    #[test]
    fn test_whitespace_handling() {
        let whitespace = r#"  {  "name"  :  "test"  ,  "count"  :  42  }  "#;
        let result: Result<TestStruct, _> = serde_json::from_str(whitespace);
        assert!(result.is_ok(), "Should handle extra whitespace");
    }

    #[test]
    fn test_escaped_characters() {
        let escaped = r#"{"name": "test\"with\"quotes", "count": 42}"#;
        let result: Result<TestStruct, _> = serde_json::from_str(escaped);
        assert!(result.is_ok(), "Should handle escaped characters");
    }

    #[test]
    fn test_newlines_in_strings() {
        let newlines = r#"{"name": "test\nwith\nnewlines", "count": 42}"#;
        let result: Result<TestStruct, _> = serde_json::from_str(newlines);
        assert!(result.is_ok(), "Should handle newlines in strings");
    }
}
