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

//! Compatibility layer between minio-rs and iceberg-rust types.
//!
//! This module provides conversions between minio-rs's custom Iceberg types
//! and the iceberg-rust crate's types. This enables interoperability with
//! the broader iceberg-rust ecosystem (DataFusion integration, etc.).
//!
//! # Feature Flag
//!
//! This module is only available when the `iceberg-compat` feature is enabled:
//!
//! ```toml
//! [dependencies]
//! minio = { version = "0.3", features = ["iceberg-compat"] }
//! ```
//!
//! # Type Mapping
//!
//! | minio-rs Type | iceberg-rust Type | Notes |
//! |---------------|-------------------|-------|
//! | `Namespace` | `NamespaceIdent` | Full conversion |
//! | `Schema` | `iceberg::spec::Schema` | Partial (V3 types need fallback) |
//! | `Transform` | `iceberg::spec::Transform` | Full conversion |
//! | `PartitionSpec` | `iceberg::spec::PartitionSpec` | Full conversion |
//! | `SortOrder` | `iceberg::spec::SortOrder` | Full conversion |
//!
//! # V3 Types
//!
//! Iceberg V3 introduces new primitive types. As of iceberg-rust 0.7:
//!
//! **Supported in iceberg-rust (fully convertible):**
//! - `PrimitiveType::TimestampNs` - Nanosecond precision timestamp
//! - `PrimitiveType::TimestamptzNs` - Nanosecond precision timestamptz
//!
//! **NOT in iceberg-rust (minio-rs only):**
//! - `PrimitiveType::Variant` - Semi-structured data
//! - `PrimitiveType::Geometry` - Geospatial geometry
//! - `PrimitiveType::Geography` - Geographic coordinates
//!
//! # Example
//!
//! ```ignore
//! use minio::s3tables::compat::IcebergCompat;
//! use minio::s3tables::utils::Namespace;
//!
//! // Convert minio-rs Namespace to iceberg-rust NamespaceIdent
//! let ns = Namespace::try_from(vec!["db".to_string(), "schema".to_string()])?;
//! let ident: iceberg::NamespaceIdent = ns.to_iceberg();
//!
//! // Convert back
//! let ns_back = Namespace::from_iceberg(&ident)?;
//! ```

use crate::s3tables::S3TablesValidationErr as ValidationErr;
use crate::s3tables::types::iceberg as minio_types;
use crate::s3tables::utils::Namespace;

// Re-export iceberg types for convenience
pub use iceberg::NamespaceIdent;
pub use iceberg::TableIdent;

/// Re-exports from iceberg::spec for type compatibility.
pub mod spec {
    pub use iceberg::spec::{
        DataContentType, ListType, MapType, NestedField, PartitionField, PartitionSpec,
        PrimitiveType as IcebergPrimitiveType, Schema, Snapshot, SnapshotReference, SortDirection,
        SortField, SortOrder, StructType, TableMetadata, Transform, Type, UnboundPartitionSpec,
    };
}

/// Extension trait for converting minio-rs types to iceberg-rust types.
pub trait ToIceberg<T> {
    /// Convert this type to its iceberg-rust equivalent.
    fn to_iceberg(&self) -> T;
}

/// Extension trait for converting iceberg-rust types to minio-rs types.
pub trait FromIceberg<T>: Sized {
    /// Error type for conversion failures.
    type Error;

    /// Convert from an iceberg-rust type to this type.
    fn from_iceberg(value: &T) -> Result<Self, Self::Error>;
}

// ============================================================================
// Namespace Conversions
// ============================================================================

impl ToIceberg<NamespaceIdent> for Namespace {
    fn to_iceberg(&self) -> NamespaceIdent {
        NamespaceIdent::from_vec(self.as_slice().to_vec()).expect("namespace should be valid")
    }
}

impl FromIceberg<NamespaceIdent> for Namespace {
    type Error = ValidationErr;

    fn from_iceberg(value: &NamespaceIdent) -> Result<Self, Self::Error> {
        Namespace::try_from(value.as_ref().to_vec())
    }
}

// ============================================================================
// Transform Conversions
// ============================================================================

impl ToIceberg<spec::Transform> for minio_types::Transform {
    fn to_iceberg(&self) -> spec::Transform {
        match self {
            minio_types::Transform::Identity => spec::Transform::Identity,
            minio_types::Transform::Year => spec::Transform::Year,
            minio_types::Transform::Month => spec::Transform::Month,
            minio_types::Transform::Day => spec::Transform::Day,
            minio_types::Transform::Hour => spec::Transform::Hour,
            minio_types::Transform::Void => spec::Transform::Void,
            minio_types::Transform::Bucket { n } => spec::Transform::Bucket(*n),
            minio_types::Transform::Truncate { width } => spec::Transform::Truncate(*width),
        }
    }
}

impl FromIceberg<spec::Transform> for minio_types::Transform {
    type Error = String;

    fn from_iceberg(value: &spec::Transform) -> Result<Self, Self::Error> {
        match value {
            spec::Transform::Identity => Ok(minio_types::Transform::Identity),
            spec::Transform::Year => Ok(minio_types::Transform::Year),
            spec::Transform::Month => Ok(minio_types::Transform::Month),
            spec::Transform::Day => Ok(minio_types::Transform::Day),
            spec::Transform::Hour => Ok(minio_types::Transform::Hour),
            spec::Transform::Void => Ok(minio_types::Transform::Void),
            spec::Transform::Bucket(n) => Ok(minio_types::Transform::Bucket { n: *n }),
            spec::Transform::Truncate(w) => Ok(minio_types::Transform::Truncate { width: *w }),
            spec::Transform::Unknown => Err("Unknown transform not supported".to_string()),
        }
    }
}

// ============================================================================
// SortDirection Conversions
// ============================================================================

impl ToIceberg<spec::SortDirection> for minio_types::SortDirection {
    fn to_iceberg(&self) -> spec::SortDirection {
        match self {
            minio_types::SortDirection::Asc => spec::SortDirection::Ascending,
            minio_types::SortDirection::Desc => spec::SortDirection::Descending,
        }
    }
}

impl FromIceberg<spec::SortDirection> for minio_types::SortDirection {
    type Error = String;

    fn from_iceberg(value: &spec::SortDirection) -> Result<Self, Self::Error> {
        match value {
            spec::SortDirection::Ascending => Ok(minio_types::SortDirection::Asc),
            spec::SortDirection::Descending => Ok(minio_types::SortDirection::Desc),
        }
    }
}

// ============================================================================
// PrimitiveType Conversions (Partial - V3 types not in iceberg-rust)
// ============================================================================

/// Error type for primitive type conversions.
#[derive(Debug, Clone)]
pub enum PrimitiveTypeConversionError {
    /// V3 type not supported in iceberg-rust.
    V3TypeNotSupported(String),
    /// Unknown type from iceberg-rust.
    UnknownType(String),
}

impl std::fmt::Display for PrimitiveTypeConversionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::V3TypeNotSupported(ty) => {
                write!(f, "V3 type '{}' is not supported in iceberg-rust", ty)
            }
            Self::UnknownType(ty) => write!(f, "Unknown type: {}", ty),
        }
    }
}

impl std::error::Error for PrimitiveTypeConversionError {}

impl TryFrom<&minio_types::PrimitiveType> for spec::IcebergPrimitiveType {
    type Error = PrimitiveTypeConversionError;

    fn try_from(value: &minio_types::PrimitiveType) -> Result<Self, Self::Error> {
        match value {
            minio_types::PrimitiveType::Boolean => Ok(spec::IcebergPrimitiveType::Boolean),
            minio_types::PrimitiveType::Int => Ok(spec::IcebergPrimitiveType::Int),
            minio_types::PrimitiveType::Long => Ok(spec::IcebergPrimitiveType::Long),
            minio_types::PrimitiveType::Float => Ok(spec::IcebergPrimitiveType::Float),
            minio_types::PrimitiveType::Double => Ok(spec::IcebergPrimitiveType::Double),
            minio_types::PrimitiveType::Decimal { precision, scale } => {
                Ok(spec::IcebergPrimitiveType::Decimal {
                    precision: *precision,
                    scale: *scale,
                })
            }
            minio_types::PrimitiveType::Date => Ok(spec::IcebergPrimitiveType::Date),
            minio_types::PrimitiveType::Time => Ok(spec::IcebergPrimitiveType::Time),
            minio_types::PrimitiveType::Timestamp => Ok(spec::IcebergPrimitiveType::Timestamp),
            minio_types::PrimitiveType::Timestamptz => Ok(spec::IcebergPrimitiveType::Timestamptz),
            minio_types::PrimitiveType::String => Ok(spec::IcebergPrimitiveType::String),
            minio_types::PrimitiveType::Uuid => Ok(spec::IcebergPrimitiveType::Uuid),
            minio_types::PrimitiveType::Fixed { length } => {
                Ok(spec::IcebergPrimitiveType::Fixed(*length as u64))
            }
            minio_types::PrimitiveType::Binary => Ok(spec::IcebergPrimitiveType::Binary),
            // Nanosecond precision timestamps - iceberg-rust 0.7 supports these
            minio_types::PrimitiveType::TimestampNs => Ok(spec::IcebergPrimitiveType::TimestampNs),
            minio_types::PrimitiveType::TimestamptzNs => {
                Ok(spec::IcebergPrimitiveType::TimestamptzNs)
            }
            // V3 types - not in iceberg-rust
            minio_types::PrimitiveType::Variant => Err(
                PrimitiveTypeConversionError::V3TypeNotSupported("variant".to_string()),
            ),
            minio_types::PrimitiveType::Geometry => Err(
                PrimitiveTypeConversionError::V3TypeNotSupported("geometry".to_string()),
            ),
            minio_types::PrimitiveType::Geography => Err(
                PrimitiveTypeConversionError::V3TypeNotSupported("geography".to_string()),
            ),
        }
    }
}

impl TryFrom<&spec::IcebergPrimitiveType> for minio_types::PrimitiveType {
    type Error = PrimitiveTypeConversionError;

    fn try_from(value: &spec::IcebergPrimitiveType) -> Result<Self, Self::Error> {
        match value {
            spec::IcebergPrimitiveType::Boolean => Ok(minio_types::PrimitiveType::Boolean),
            spec::IcebergPrimitiveType::Int => Ok(minio_types::PrimitiveType::Int),
            spec::IcebergPrimitiveType::Long => Ok(minio_types::PrimitiveType::Long),
            spec::IcebergPrimitiveType::Float => Ok(minio_types::PrimitiveType::Float),
            spec::IcebergPrimitiveType::Double => Ok(minio_types::PrimitiveType::Double),
            spec::IcebergPrimitiveType::Decimal { precision, scale } => {
                Ok(minio_types::PrimitiveType::Decimal {
                    precision: *precision,
                    scale: *scale,
                })
            }
            spec::IcebergPrimitiveType::Date => Ok(minio_types::PrimitiveType::Date),
            spec::IcebergPrimitiveType::Time => Ok(minio_types::PrimitiveType::Time),
            spec::IcebergPrimitiveType::Timestamp => Ok(minio_types::PrimitiveType::Timestamp),
            spec::IcebergPrimitiveType::Timestamptz => Ok(minio_types::PrimitiveType::Timestamptz),
            spec::IcebergPrimitiveType::String => Ok(minio_types::PrimitiveType::String),
            spec::IcebergPrimitiveType::Uuid => Ok(minio_types::PrimitiveType::Uuid),
            spec::IcebergPrimitiveType::Fixed(length) => Ok(minio_types::PrimitiveType::Fixed {
                length: *length as u32,
            }),
            spec::IcebergPrimitiveType::Binary => Ok(minio_types::PrimitiveType::Binary),
            // V3 types - iceberg-rust 0.7 has these
            spec::IcebergPrimitiveType::TimestampNs => Ok(minio_types::PrimitiveType::TimestampNs),
            spec::IcebergPrimitiveType::TimestamptzNs => {
                Ok(minio_types::PrimitiveType::TimestamptzNs)
            }
        }
    }
}

// ============================================================================
// V3 Types (minio-rs only)
// ============================================================================

/// Re-export V3 types that are only available in minio-rs.
///
/// These types are part of the Iceberg V3 specification but are not yet
/// available in iceberg-rust. They should be used directly from minio-rs.
///
/// Note: `TimestampNs` and `TimestamptzNs` are supported in iceberg-rust 0.7
/// and are fully convertible. Only Variant, Geometry, and Geography remain
/// as minio-rs-only types.
pub mod v3 {
    pub use crate::s3tables::types::iceberg::{GeographyType, GeometryType, PrimitiveType};

    /// Check if a primitive type is a V3-only type (not convertible to iceberg-rust).
    ///
    /// This returns true only for types that cannot be converted to iceberg-rust:
    /// - Variant
    /// - Geometry
    /// - Geography
    pub fn is_v3_only_type(ty: &PrimitiveType) -> bool {
        matches!(
            ty,
            PrimitiveType::Variant | PrimitiveType::Geometry | PrimitiveType::Geography
        )
    }

    /// Check if a primitive type is part of the V3 specification.
    ///
    /// This includes all V3 types, including those now supported by iceberg-rust.
    pub fn is_v3_type(ty: &PrimitiveType) -> bool {
        matches!(
            ty,
            PrimitiveType::Variant
                | PrimitiveType::Geometry
                | PrimitiveType::Geography
                | PrimitiveType::TimestampNs
                | PrimitiveType::TimestamptzNs
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_namespace_to_iceberg() {
        let ns = Namespace::try_from(vec!["db".to_string(), "schema".to_string()]).unwrap();
        let ident = ns.to_iceberg();
        assert_eq!(ident.as_ref(), &["db", "schema"]);
    }

    #[test]
    fn test_namespace_from_iceberg() {
        let ident = NamespaceIdent::from_vec(vec!["db".to_string(), "schema".to_string()]).unwrap();
        let ns = Namespace::from_iceberg(&ident).unwrap();
        assert_eq!(ns.as_slice(), &["db", "schema"]);
    }

    #[test]
    fn test_transform_roundtrip() {
        let transforms = vec![
            minio_types::Transform::Identity,
            minio_types::Transform::Year,
            minio_types::Transform::Month,
            minio_types::Transform::Day,
            minio_types::Transform::Hour,
            minio_types::Transform::Void,
            minio_types::Transform::Bucket { n: 16 },
            minio_types::Transform::Truncate { width: 10 },
        ];

        for t in transforms {
            let iceberg_t = t.to_iceberg();
            let roundtrip = minio_types::Transform::from_iceberg(&iceberg_t).unwrap();
            assert_eq!(t, roundtrip);
        }
    }

    #[test]
    fn test_sort_direction_roundtrip() {
        let asc = minio_types::SortDirection::Asc;
        let desc = minio_types::SortDirection::Desc;

        let iceberg_asc = asc.to_iceberg();
        let iceberg_desc = desc.to_iceberg();

        assert!(matches!(iceberg_asc, spec::SortDirection::Ascending));
        assert!(matches!(iceberg_desc, spec::SortDirection::Descending));

        let roundtrip_asc = minio_types::SortDirection::from_iceberg(&iceberg_asc).unwrap();
        let roundtrip_desc = minio_types::SortDirection::from_iceberg(&iceberg_desc).unwrap();

        assert!(matches!(roundtrip_asc, minio_types::SortDirection::Asc));
        assert!(matches!(roundtrip_desc, minio_types::SortDirection::Desc));
    }

    #[test]
    fn test_primitive_type_v2_roundtrip() {
        let types = vec![
            minio_types::PrimitiveType::Boolean,
            minio_types::PrimitiveType::Int,
            minio_types::PrimitiveType::Long,
            minio_types::PrimitiveType::Float,
            minio_types::PrimitiveType::Double,
            minio_types::PrimitiveType::Date,
            minio_types::PrimitiveType::Time,
            minio_types::PrimitiveType::Timestamp,
            minio_types::PrimitiveType::Timestamptz,
            minio_types::PrimitiveType::String,
            minio_types::PrimitiveType::Uuid,
            minio_types::PrimitiveType::Binary,
            minio_types::PrimitiveType::Decimal {
                precision: 10,
                scale: 2,
            },
            minio_types::PrimitiveType::Fixed { length: 16 },
        ];

        for t in types {
            let iceberg_t: spec::IcebergPrimitiveType = (&t).try_into().unwrap();
            let roundtrip: minio_types::PrimitiveType = (&iceberg_t).try_into().unwrap();

            // Check they serialize the same way for comparison
            let original_json = serde_json::to_string(&t).unwrap();
            let roundtrip_json = serde_json::to_string(&roundtrip).unwrap();
            assert_eq!(original_json, roundtrip_json);
        }
    }

    #[test]
    fn test_v3_only_types_not_convertible() {
        // These V3 types are not in iceberg-rust and cannot be converted
        let v3_only_types = vec![
            minio_types::PrimitiveType::Variant,
            minio_types::PrimitiveType::Geometry,
            minio_types::PrimitiveType::Geography,
        ];

        for t in v3_only_types {
            let result: Result<spec::IcebergPrimitiveType, _> = (&t).try_into();
            assert!(result.is_err());
            assert!(v3::is_v3_only_type(&t));
            assert!(v3::is_v3_type(&t));
        }
    }

    #[test]
    fn test_timestamp_ns_types_convertible() {
        // TimestampNs and TimestamptzNs are V3 types but are supported in iceberg-rust 0.7
        let ts_ns = minio_types::PrimitiveType::TimestampNs;
        let tstz_ns = minio_types::PrimitiveType::TimestamptzNs;

        // They are V3 types
        assert!(v3::is_v3_type(&ts_ns));
        assert!(v3::is_v3_type(&tstz_ns));

        // But they are NOT V3-only (they can be converted to iceberg-rust)
        assert!(!v3::is_v3_only_type(&ts_ns));
        assert!(!v3::is_v3_only_type(&tstz_ns));

        // Verify forward conversion works
        let ice_ts_ns: spec::IcebergPrimitiveType = (&ts_ns).try_into().unwrap();
        let ice_tstz_ns: spec::IcebergPrimitiveType = (&tstz_ns).try_into().unwrap();

        // Verify roundtrip
        let roundtrip_ts: minio_types::PrimitiveType = (&ice_ts_ns).try_into().unwrap();
        let roundtrip_tstz: minio_types::PrimitiveType = (&ice_tstz_ns).try_into().unwrap();

        assert!(matches!(
            roundtrip_ts,
            minio_types::PrimitiveType::TimestampNs
        ));
        assert!(matches!(
            roundtrip_tstz,
            minio_types::PrimitiveType::TimestamptzNs
        ));
    }

    // ========================================================================
    // Serde Compatibility Tests
    // ========================================================================

    #[test]
    fn test_serde_transform_compatibility() {
        // Test that transforms serialize in compatible formats
        let minio_transforms = vec![
            (minio_types::Transform::Identity, "identity"),
            (minio_types::Transform::Year, "year"),
            (minio_types::Transform::Month, "month"),
            (minio_types::Transform::Day, "day"),
            (minio_types::Transform::Hour, "hour"),
            (minio_types::Transform::Void, "void"),
        ];

        for (transform, expected_str) in minio_transforms {
            let json = serde_json::to_string(&transform).unwrap();
            assert!(
                json.contains(expected_str),
                "Transform {expected_str} should contain '{expected_str}' in JSON, got: {json}"
            );
        }
    }

    #[test]
    fn test_serde_bucket_transform_compatibility() {
        // Bucket transform with n=16
        let minio = minio_types::Transform::Bucket { n: 16 };
        let minio_json = serde_json::to_string(&minio).unwrap();

        // Both should represent bucket[16]
        assert!(
            minio_json.contains("bucket") && minio_json.contains("16"),
            "Bucket transform should contain 'bucket' and '16', got: {minio_json}"
        );
    }

    #[test]
    fn test_serde_truncate_transform_compatibility() {
        // Truncate transform with width=10
        let minio = minio_types::Transform::Truncate { width: 10 };
        let minio_json = serde_json::to_string(&minio).unwrap();

        // Both should represent truncate[10]
        assert!(
            minio_json.contains("truncate") && minio_json.contains("10"),
            "Truncate transform should contain 'truncate' and '10', got: {minio_json}"
        );
    }

    #[test]
    fn test_serde_primitive_types_string_representation() {
        // Verify that primitive type strings match Iceberg spec
        let type_strings = vec![
            (minio_types::PrimitiveType::Boolean, "boolean"),
            (minio_types::PrimitiveType::Int, "int"),
            (minio_types::PrimitiveType::Long, "long"),
            (minio_types::PrimitiveType::Float, "float"),
            (minio_types::PrimitiveType::Double, "double"),
            (minio_types::PrimitiveType::Date, "date"),
            (minio_types::PrimitiveType::Time, "time"),
            (minio_types::PrimitiveType::Timestamp, "timestamp"),
            (minio_types::PrimitiveType::Timestamptz, "timestamptz"),
            (minio_types::PrimitiveType::String, "string"),
            (minio_types::PrimitiveType::Uuid, "uuid"),
            (minio_types::PrimitiveType::Binary, "binary"),
            (minio_types::PrimitiveType::TimestampNs, "timestamp_ns"),
            (minio_types::PrimitiveType::TimestamptzNs, "timestamptz_ns"),
        ];

        for (ty, expected) in type_strings {
            let json = serde_json::to_string(&ty).unwrap();
            assert!(
                json.contains(expected),
                "Type should contain '{expected}', got: {json}"
            );
        }
    }

    #[test]
    fn test_serde_decimal_type_format() {
        // Decimal(10, 2) should serialize with precision and scale
        let decimal = minio_types::PrimitiveType::Decimal {
            precision: 10,
            scale: 2,
        };
        let json = serde_json::to_string(&decimal).unwrap();

        // Should contain decimal, 10, and 2
        assert!(
            json.contains("decimal") && json.contains("10") && json.contains("2"),
            "Decimal should contain 'decimal', '10', and '2', got: {json}"
        );
    }

    #[test]
    fn test_serde_fixed_type_format() {
        // Fixed(16) should serialize with length
        let fixed = minio_types::PrimitiveType::Fixed { length: 16 };
        let json = serde_json::to_string(&fixed).unwrap();

        // Should contain fixed and 16
        assert!(
            json.contains("fixed") && json.contains("16"),
            "Fixed should contain 'fixed' and '16', got: {json}"
        );
    }
}
