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

//! Iceberg V3 Variant type support
//!
//! This module provides encoding and decoding for the Iceberg V3 Variant type,
//! which stores semi-structured data similar to JSON but with typed values.
//!
//! The Variant type was introduced in Iceberg V3 to support semi-structured
//! data without requiring schema definition upfront. Values can be primitives,
//! arrays, or objects with string keys.
//!
//! # Supported Value Types
//!
//! | Type | Description |
//! |------|-------------|
//! | Null | Null value |
//! | Boolean | True/false |
//! | Int8/16/32/64 | Signed integers |
//! | Float/Double | IEEE 754 floating point |
//! | Decimal | Arbitrary precision decimal |
//! | Date | Days since Unix epoch |
//! | Timestamp | Microseconds since Unix epoch |
//! | String | UTF-8 string |
//! | Binary | Byte array |
//! | Array | Ordered sequence of variants |
//! | Object | String-keyed map of variants |
//!
//! # Example
//!
//! ```
//! use minio::s3tables::variant::{Variant, VariantValue};
//!
//! // Create a variant from JSON-like structure
//! let variant = Variant::object([
//!     ("name", Variant::string("Alice")),
//!     ("age", Variant::int(30)),
//!     ("active", Variant::boolean(true)),
//! ]);
//!
//! // Access values
//! assert_eq!(variant.get("name").unwrap().as_str(), Some("Alice"));
//! ```
//!
//! # References
//!
//! - [Iceberg V3 Spec - Variant Type](https://iceberg.apache.org/spec/#variant)
//! - [Iceberg Table Spec](https://iceberg.apache.org/spec/)
//! - [Parquet Variant Shredding](https://github.com/apache/parquet-format/blob/master/VariantShredding.md)

use std::collections::HashMap;

/// Variant parsing/encoding error
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum VariantError {
    /// Unexpected end of input
    UnexpectedEof,
    /// Invalid type tag
    InvalidTypeTag(u8),
    /// Invalid UTF-8 string
    InvalidUtf8,
    /// Invalid structure
    InvalidStructure(String),
    /// Type mismatch during access
    TypeMismatch {
        expected: &'static str,
        actual: &'static str,
    },
}

impl std::fmt::Display for VariantError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            VariantError::UnexpectedEof => write!(f, "Unexpected end of variant data"),
            VariantError::InvalidTypeTag(t) => write!(f, "Invalid variant type tag: {t}"),
            VariantError::InvalidUtf8 => write!(f, "Invalid UTF-8 in variant string"),
            VariantError::InvalidStructure(msg) => write!(f, "Invalid variant structure: {msg}"),
            VariantError::TypeMismatch { expected, actual } => {
                write!(f, "Type mismatch: expected {expected}, got {actual}")
            }
        }
    }
}

impl std::error::Error for VariantError {}

/// Type tags for variant binary encoding
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum VariantTypeTag {
    Null = 0,
    Boolean = 1,
    Int8 = 2,
    Int16 = 3,
    Int32 = 4,
    Int64 = 5,
    Float = 6,
    Double = 7,
    Decimal = 8,
    Date = 9,
    Timestamp = 10,
    TimestampNtz = 11,
    Binary = 12,
    String = 13,
    Array = 14,
    Object = 15,
}

impl TryFrom<u8> for VariantTypeTag {
    type Error = VariantError;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(VariantTypeTag::Null),
            1 => Ok(VariantTypeTag::Boolean),
            2 => Ok(VariantTypeTag::Int8),
            3 => Ok(VariantTypeTag::Int16),
            4 => Ok(VariantTypeTag::Int32),
            5 => Ok(VariantTypeTag::Int64),
            6 => Ok(VariantTypeTag::Float),
            7 => Ok(VariantTypeTag::Double),
            8 => Ok(VariantTypeTag::Decimal),
            9 => Ok(VariantTypeTag::Date),
            10 => Ok(VariantTypeTag::Timestamp),
            11 => Ok(VariantTypeTag::TimestampNtz),
            12 => Ok(VariantTypeTag::Binary),
            13 => Ok(VariantTypeTag::String),
            14 => Ok(VariantTypeTag::Array),
            15 => Ok(VariantTypeTag::Object),
            _ => Err(VariantError::InvalidTypeTag(value)),
        }
    }
}

/// A variant value representing semi-structured data
#[derive(Debug, Clone, PartialEq)]
pub enum Variant {
    Null,
    Boolean(bool),
    Int8(i8),
    Int16(i16),
    Int32(i32),
    Int64(i64),
    Float(f32),
    Double(f64),
    /// Stored as string to preserve arbitrary precision
    Decimal(String),
    /// Days since Unix epoch
    Date(i32),
    /// Microseconds since Unix epoch (with timezone)
    Timestamp(i64),
    /// Microseconds since Unix epoch (without timezone)
    TimestampNtz(i64),
    Binary(Vec<u8>),
    String(String),
    Array(Vec<Variant>),
    Object(HashMap<String, Variant>),
}

impl Variant {
    pub fn null() -> Self {
        Variant::Null
    }

    pub fn boolean(v: bool) -> Self {
        Variant::Boolean(v)
    }

    /// Selects the smallest integer type that fits the value
    pub fn int(v: i64) -> Self {
        if v >= i8::MIN as i64 && v <= i8::MAX as i64 {
            Variant::Int8(v as i8)
        } else if v >= i16::MIN as i64 && v <= i16::MAX as i64 {
            Variant::Int16(v as i16)
        } else if v >= i32::MIN as i64 && v <= i32::MAX as i64 {
            Variant::Int32(v as i32)
        } else {
            Variant::Int64(v)
        }
    }

    pub fn int32(v: i32) -> Self {
        Variant::Int32(v)
    }

    pub fn int64(v: i64) -> Self {
        Variant::Int64(v)
    }

    pub fn float(v: f32) -> Self {
        Variant::Float(v)
    }

    pub fn double(v: f64) -> Self {
        Variant::Double(v)
    }

    pub fn decimal(v: impl Into<String>) -> Self {
        Variant::Decimal(v.into())
    }

    pub fn date(days: i32) -> Self {
        Variant::Date(days)
    }

    pub fn timestamp(micros: i64) -> Self {
        Variant::Timestamp(micros)
    }

    pub fn timestamp_ntz(micros: i64) -> Self {
        Variant::TimestampNtz(micros)
    }

    pub fn binary(v: impl Into<Vec<u8>>) -> Self {
        Variant::Binary(v.into())
    }

    pub fn string(v: impl Into<String>) -> Self {
        Variant::String(v.into())
    }

    pub fn array(v: impl IntoIterator<Item = Variant>) -> Self {
        Variant::Array(v.into_iter().collect())
    }

    pub fn object<K, V>(pairs: impl IntoIterator<Item = (K, V)>) -> Self
    where
        K: Into<String>,
        V: Into<Variant>,
    {
        Variant::Object(
            pairs
                .into_iter()
                .map(|(k, v)| (k.into(), v.into()))
                .collect(),
        )
    }

    /// Returns the Iceberg type name for this variant
    pub fn type_name(&self) -> &'static str {
        match self {
            Variant::Null => "null",
            Variant::Boolean(_) => "boolean",
            Variant::Int8(_) => "int8",
            Variant::Int16(_) => "int16",
            Variant::Int32(_) => "int32",
            Variant::Int64(_) => "int64",
            Variant::Float(_) => "float",
            Variant::Double(_) => "double",
            Variant::Decimal(_) => "decimal",
            Variant::Date(_) => "date",
            Variant::Timestamp(_) => "timestamp",
            Variant::TimestampNtz(_) => "timestamp_ntz",
            Variant::Binary(_) => "binary",
            Variant::String(_) => "string",
            Variant::Array(_) => "array",
            Variant::Object(_) => "object",
        }
    }

    /// Check if this is a null value
    pub fn is_null(&self) -> bool {
        matches!(self, Variant::Null)
    }

    pub fn is_boolean(&self) -> bool {
        matches!(self, Variant::Boolean(_))
    }

    pub fn is_integer(&self) -> bool {
        matches!(
            self,
            Variant::Int8(_) | Variant::Int16(_) | Variant::Int32(_) | Variant::Int64(_)
        )
    }

    pub fn is_float(&self) -> bool {
        matches!(self, Variant::Float(_) | Variant::Double(_))
    }

    pub fn is_string(&self) -> bool {
        matches!(self, Variant::String(_))
    }

    pub fn is_array(&self) -> bool {
        matches!(self, Variant::Array(_))
    }

    pub fn is_object(&self) -> bool {
        matches!(self, Variant::Object(_))
    }

    pub fn as_bool(&self) -> Option<bool> {
        match self {
            Variant::Boolean(v) => Some(*v),
            _ => None,
        }
    }

    /// Converts any integer variant to i64
    pub fn as_i64(&self) -> Option<i64> {
        match self {
            Variant::Int8(v) => Some(*v as i64),
            Variant::Int16(v) => Some(*v as i64),
            Variant::Int32(v) => Some(*v as i64),
            Variant::Int64(v) => Some(*v),
            _ => None,
        }
    }

    /// Converts Float or Double to f64
    pub fn as_f64(&self) -> Option<f64> {
        match self {
            Variant::Float(v) => Some(*v as f64),
            Variant::Double(v) => Some(*v),
            _ => None,
        }
    }

    pub fn as_str(&self) -> Option<&str> {
        match self {
            Variant::String(v) => Some(v),
            _ => None,
        }
    }

    pub fn as_bytes(&self) -> Option<&[u8]> {
        match self {
            Variant::Binary(v) => Some(v),
            _ => None,
        }
    }

    pub fn as_array(&self) -> Option<&[Variant]> {
        match self {
            Variant::Array(v) => Some(v),
            _ => None,
        }
    }

    pub fn as_object(&self) -> Option<&HashMap<String, Variant>> {
        match self {
            Variant::Object(v) => Some(v),
            _ => None,
        }
    }

    pub fn get(&self, key: &str) -> Option<&Variant> {
        match self {
            Variant::Object(map) => map.get(key),
            _ => None,
        }
    }

    pub fn get_index(&self, index: usize) -> Option<&Variant> {
        match self {
            Variant::Array(arr) => arr.get(index),
            _ => None,
        }
    }

    /// Encode this variant to binary format
    pub fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        self.encode_to(&mut buf);
        buf
    }

    /// Encode this variant to an existing buffer
    pub fn encode_to(&self, buf: &mut Vec<u8>) {
        match self {
            Variant::Null => {
                buf.push(VariantTypeTag::Null as u8);
            }
            Variant::Boolean(v) => {
                buf.push(VariantTypeTag::Boolean as u8);
                buf.push(if *v { 1 } else { 0 });
            }
            Variant::Int8(v) => {
                buf.push(VariantTypeTag::Int8 as u8);
                buf.extend_from_slice(&v.to_le_bytes());
            }
            Variant::Int16(v) => {
                buf.push(VariantTypeTag::Int16 as u8);
                buf.extend_from_slice(&v.to_le_bytes());
            }
            Variant::Int32(v) => {
                buf.push(VariantTypeTag::Int32 as u8);
                buf.extend_from_slice(&v.to_le_bytes());
            }
            Variant::Int64(v) => {
                buf.push(VariantTypeTag::Int64 as u8);
                buf.extend_from_slice(&v.to_le_bytes());
            }
            Variant::Float(v) => {
                buf.push(VariantTypeTag::Float as u8);
                buf.extend_from_slice(&v.to_le_bytes());
            }
            Variant::Double(v) => {
                buf.push(VariantTypeTag::Double as u8);
                buf.extend_from_slice(&v.to_le_bytes());
            }
            Variant::Decimal(v) => {
                buf.push(VariantTypeTag::Decimal as u8);
                let bytes = v.as_bytes();
                buf.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
                buf.extend_from_slice(bytes);
            }
            Variant::Date(v) => {
                buf.push(VariantTypeTag::Date as u8);
                buf.extend_from_slice(&v.to_le_bytes());
            }
            Variant::Timestamp(v) => {
                buf.push(VariantTypeTag::Timestamp as u8);
                buf.extend_from_slice(&v.to_le_bytes());
            }
            Variant::TimestampNtz(v) => {
                buf.push(VariantTypeTag::TimestampNtz as u8);
                buf.extend_from_slice(&v.to_le_bytes());
            }
            Variant::Binary(v) => {
                buf.push(VariantTypeTag::Binary as u8);
                buf.extend_from_slice(&(v.len() as u32).to_le_bytes());
                buf.extend_from_slice(v);
            }
            Variant::String(v) => {
                buf.push(VariantTypeTag::String as u8);
                let bytes = v.as_bytes();
                buf.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
                buf.extend_from_slice(bytes);
            }
            Variant::Array(arr) => {
                buf.push(VariantTypeTag::Array as u8);
                buf.extend_from_slice(&(arr.len() as u32).to_le_bytes());
                for item in arr {
                    item.encode_to(buf);
                }
            }
            Variant::Object(obj) => {
                buf.push(VariantTypeTag::Object as u8);
                buf.extend_from_slice(&(obj.len() as u32).to_le_bytes());
                for (key, value) in obj {
                    let key_bytes = key.as_bytes();
                    buf.extend_from_slice(&(key_bytes.len() as u32).to_le_bytes());
                    buf.extend_from_slice(key_bytes);
                    value.encode_to(buf);
                }
            }
        }
    }

    /// Decode a variant from binary format
    pub fn decode(data: &[u8]) -> Result<Self, VariantError> {
        let mut cursor = 0;
        Self::decode_from(data, &mut cursor)
    }

    /// Decode a variant from binary format at a specific cursor position
    fn decode_from(data: &[u8], cursor: &mut usize) -> Result<Self, VariantError> {
        let tag = *data.get(*cursor).ok_or(VariantError::UnexpectedEof)?;
        *cursor += 1;
        let tag = VariantTypeTag::try_from(tag)?;

        match tag {
            VariantTypeTag::Null => Ok(Variant::Null),
            VariantTypeTag::Boolean => {
                let v = *data.get(*cursor).ok_or(VariantError::UnexpectedEof)?;
                *cursor += 1;
                Ok(Variant::Boolean(v != 0))
            }
            VariantTypeTag::Int8 => {
                let v = *data.get(*cursor).ok_or(VariantError::UnexpectedEof)? as i8;
                *cursor += 1;
                Ok(Variant::Int8(v))
            }
            VariantTypeTag::Int16 => {
                let bytes: [u8; 2] = data
                    .get(*cursor..*cursor + 2)
                    .ok_or(VariantError::UnexpectedEof)?
                    .try_into()
                    .unwrap();
                *cursor += 2;
                Ok(Variant::Int16(i16::from_le_bytes(bytes)))
            }
            VariantTypeTag::Int32 => {
                let bytes: [u8; 4] = data
                    .get(*cursor..*cursor + 4)
                    .ok_or(VariantError::UnexpectedEof)?
                    .try_into()
                    .unwrap();
                *cursor += 4;
                Ok(Variant::Int32(i32::from_le_bytes(bytes)))
            }
            VariantTypeTag::Int64 => {
                let bytes: [u8; 8] = data
                    .get(*cursor..*cursor + 8)
                    .ok_or(VariantError::UnexpectedEof)?
                    .try_into()
                    .unwrap();
                *cursor += 8;
                Ok(Variant::Int64(i64::from_le_bytes(bytes)))
            }
            VariantTypeTag::Float => {
                let bytes: [u8; 4] = data
                    .get(*cursor..*cursor + 4)
                    .ok_or(VariantError::UnexpectedEof)?
                    .try_into()
                    .unwrap();
                *cursor += 4;
                Ok(Variant::Float(f32::from_le_bytes(bytes)))
            }
            VariantTypeTag::Double => {
                let bytes: [u8; 8] = data
                    .get(*cursor..*cursor + 8)
                    .ok_or(VariantError::UnexpectedEof)?
                    .try_into()
                    .unwrap();
                *cursor += 8;
                Ok(Variant::Double(f64::from_le_bytes(bytes)))
            }
            VariantTypeTag::Decimal => {
                let len = Self::read_u32(data, cursor)? as usize;
                let s = Self::read_string(data, cursor, len)?;
                Ok(Variant::Decimal(s))
            }
            VariantTypeTag::Date => {
                let bytes: [u8; 4] = data
                    .get(*cursor..*cursor + 4)
                    .ok_or(VariantError::UnexpectedEof)?
                    .try_into()
                    .unwrap();
                *cursor += 4;
                Ok(Variant::Date(i32::from_le_bytes(bytes)))
            }
            VariantTypeTag::Timestamp => {
                let bytes: [u8; 8] = data
                    .get(*cursor..*cursor + 8)
                    .ok_or(VariantError::UnexpectedEof)?
                    .try_into()
                    .unwrap();
                *cursor += 8;
                Ok(Variant::Timestamp(i64::from_le_bytes(bytes)))
            }
            VariantTypeTag::TimestampNtz => {
                let bytes: [u8; 8] = data
                    .get(*cursor..*cursor + 8)
                    .ok_or(VariantError::UnexpectedEof)?
                    .try_into()
                    .unwrap();
                *cursor += 8;
                Ok(Variant::TimestampNtz(i64::from_le_bytes(bytes)))
            }
            VariantTypeTag::Binary => {
                let len = Self::read_u32(data, cursor)? as usize;
                let bytes = data
                    .get(*cursor..*cursor + len)
                    .ok_or(VariantError::UnexpectedEof)?
                    .to_vec();
                *cursor += len;
                Ok(Variant::Binary(bytes))
            }
            VariantTypeTag::String => {
                let len = Self::read_u32(data, cursor)? as usize;
                let s = Self::read_string(data, cursor, len)?;
                Ok(Variant::String(s))
            }
            VariantTypeTag::Array => {
                let count = Self::read_u32(data, cursor)? as usize;
                let mut arr = Vec::with_capacity(count);
                for _ in 0..count {
                    arr.push(Self::decode_from(data, cursor)?);
                }
                Ok(Variant::Array(arr))
            }
            VariantTypeTag::Object => {
                let count = Self::read_u32(data, cursor)? as usize;
                let mut obj = HashMap::with_capacity(count);
                for _ in 0..count {
                    let key_len = Self::read_u32(data, cursor)? as usize;
                    let key = Self::read_string(data, cursor, key_len)?;
                    let value = Self::decode_from(data, cursor)?;
                    obj.insert(key, value);
                }
                Ok(Variant::Object(obj))
            }
        }
    }

    fn read_u32(data: &[u8], cursor: &mut usize) -> Result<u32, VariantError> {
        let bytes: [u8; 4] = data
            .get(*cursor..*cursor + 4)
            .ok_or(VariantError::UnexpectedEof)?
            .try_into()
            .unwrap();
        *cursor += 4;
        Ok(u32::from_le_bytes(bytes))
    }

    fn read_string(data: &[u8], cursor: &mut usize, len: usize) -> Result<String, VariantError> {
        let bytes = data
            .get(*cursor..*cursor + len)
            .ok_or(VariantError::UnexpectedEof)?;
        *cursor += len;
        String::from_utf8(bytes.to_vec()).map_err(|_| VariantError::InvalidUtf8)
    }

    pub fn to_json(&self) -> serde_json::Value {
        match self {
            Variant::Null => serde_json::Value::Null,
            Variant::Boolean(v) => serde_json::Value::Bool(*v),
            Variant::Int8(v) => serde_json::json!(*v),
            Variant::Int16(v) => serde_json::json!(*v),
            Variant::Int32(v) => serde_json::json!(*v),
            Variant::Int64(v) => serde_json::json!(*v),
            Variant::Float(v) => serde_json::json!(*v),
            Variant::Double(v) => serde_json::json!(*v),
            Variant::Decimal(v) => serde_json::Value::String(v.clone()),
            Variant::Date(v) => serde_json::json!(*v),
            Variant::Timestamp(v) => serde_json::json!(*v),
            Variant::TimestampNtz(v) => serde_json::json!(*v),
            Variant::Binary(v) => {
                use base64::Engine;
                serde_json::Value::String(base64::engine::general_purpose::STANDARD.encode(v))
            }
            Variant::String(v) => serde_json::Value::String(v.clone()),
            Variant::Array(arr) => {
                serde_json::Value::Array(arr.iter().map(|v| v.to_json()).collect())
            }
            Variant::Object(obj) => serde_json::Value::Object(
                obj.iter().map(|(k, v)| (k.clone(), v.to_json())).collect(),
            ),
        }
    }

    /// Create a variant from a JSON value
    pub fn from_json(value: &serde_json::Value) -> Self {
        match value {
            serde_json::Value::Null => Variant::Null,
            serde_json::Value::Bool(v) => Variant::Boolean(*v),
            serde_json::Value::Number(n) => {
                if let Some(i) = n.as_i64() {
                    Variant::int(i)
                } else if let Some(f) = n.as_f64() {
                    Variant::Double(f)
                } else {
                    Variant::String(n.to_string())
                }
            }
            serde_json::Value::String(s) => Variant::String(s.clone()),
            serde_json::Value::Array(arr) => {
                Variant::Array(arr.iter().map(Variant::from_json).collect())
            }
            serde_json::Value::Object(obj) => Variant::Object(
                obj.iter()
                    .map(|(k, v)| (k.clone(), Variant::from_json(v)))
                    .collect(),
            ),
        }
    }

    /// Serialized size when encoded
    pub fn size_bytes(&self) -> usize {
        self.encode().len()
    }

    /// Count distinct types in this variant (for statistics)
    pub fn count_types(&self) -> HashMap<&'static str, usize> {
        let mut counts = HashMap::new();
        self.count_types_recursive(&mut counts);
        counts
    }

    fn count_types_recursive(&self, counts: &mut HashMap<&'static str, usize>) {
        *counts.entry(self.type_name()).or_insert(0) += 1;
        match self {
            Variant::Array(arr) => {
                for item in arr {
                    item.count_types_recursive(counts);
                }
            }
            Variant::Object(obj) => {
                for value in obj.values() {
                    value.count_types_recursive(counts);
                }
            }
            _ => {}
        }
    }
}

impl From<bool> for Variant {
    fn from(v: bool) -> Self {
        Variant::Boolean(v)
    }
}

impl From<i32> for Variant {
    fn from(v: i32) -> Self {
        Variant::Int32(v)
    }
}

impl From<i64> for Variant {
    fn from(v: i64) -> Self {
        Variant::Int64(v)
    }
}

impl From<f64> for Variant {
    fn from(v: f64) -> Self {
        Variant::Double(v)
    }
}

impl From<String> for Variant {
    fn from(v: String) -> Self {
        Variant::String(v)
    }
}

impl From<&str> for Variant {
    fn from(v: &str) -> Self {
        Variant::String(v.to_string())
    }
}

impl From<Vec<Variant>> for Variant {
    fn from(v: Vec<Variant>) -> Self {
        Variant::Array(v)
    }
}

/// Trait for values that can be converted into Variant
pub trait VariantValue: Into<Variant> {}

impl<T: Into<Variant>> VariantValue for T {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_null() {
        let v = Variant::null();
        assert!(v.is_null());
        assert_eq!(v.type_name(), "null");
    }

    #[test]
    fn test_boolean() {
        let v = Variant::boolean(true);
        assert!(v.is_boolean());
        assert_eq!(v.as_bool(), Some(true));

        let v = Variant::boolean(false);
        assert_eq!(v.as_bool(), Some(false));
    }

    #[test]
    fn test_integers() {
        // Small values use smaller types
        let v = Variant::int(42);
        assert!(matches!(v, Variant::Int8(42)));
        assert_eq!(v.as_i64(), Some(42));

        let v = Variant::int(1000);
        assert!(matches!(v, Variant::Int16(1000)));

        let v = Variant::int(100000);
        assert!(matches!(v, Variant::Int32(100000)));

        let v = Variant::int(10_000_000_000);
        assert!(matches!(v, Variant::Int64(10_000_000_000)));
    }

    #[test]
    fn test_floats() {
        let v = Variant::float(2.5);
        assert!(v.is_float());

        let v = Variant::double(1.23456789);
        assert_eq!(v.as_f64(), Some(1.23456789));
    }

    #[test]
    fn test_string() {
        let v = Variant::string("hello");
        assert!(v.is_string());
        assert_eq!(v.as_str(), Some("hello"));
    }

    #[test]
    fn test_array() {
        let v = Variant::array([Variant::int(1), Variant::int(2), Variant::int(3)]);
        assert!(v.is_array());
        assert_eq!(v.as_array().unwrap().len(), 3);
        assert_eq!(v.get_index(1).unwrap().as_i64(), Some(2));
    }

    #[test]
    fn test_object() {
        let v = Variant::object([
            ("name", Variant::string("Alice")),
            ("age", Variant::int(30)),
        ]);
        assert!(v.is_object());
        assert_eq!(v.get("name").unwrap().as_str(), Some("Alice"));
        assert_eq!(v.get("age").unwrap().as_i64(), Some(30));
        assert!(v.get("missing").is_none());
    }

    #[test]
    fn test_encode_decode_null() {
        let original = Variant::null();
        let encoded = original.encode();
        let decoded = Variant::decode(&encoded).unwrap();
        assert_eq!(original, decoded);
    }

    #[test]
    fn test_encode_decode_boolean() {
        for v in [true, false] {
            let original = Variant::boolean(v);
            let encoded = original.encode();
            let decoded = Variant::decode(&encoded).unwrap();
            assert_eq!(original, decoded);
        }
    }

    #[test]
    fn test_encode_decode_integers() {
        for v in [
            0i64,
            1,
            -1,
            127,
            128,
            32767,
            32768,
            i32::MAX as i64,
            i64::MAX,
        ] {
            let original = Variant::int(v);
            let encoded = original.encode();
            let decoded = Variant::decode(&encoded).unwrap();
            assert_eq!(original.as_i64(), decoded.as_i64());
        }
    }

    #[test]
    fn test_encode_decode_string() {
        let original = Variant::string("Hello, World!");
        let encoded = original.encode();
        let decoded = Variant::decode(&encoded).unwrap();
        assert_eq!(original, decoded);
    }

    #[test]
    fn test_encode_decode_array() {
        let original = Variant::array([
            Variant::int(1),
            Variant::string("two"),
            Variant::boolean(true),
        ]);
        let encoded = original.encode();
        let decoded = Variant::decode(&encoded).unwrap();
        assert_eq!(original, decoded);
    }

    #[test]
    fn test_encode_decode_object() {
        let original = Variant::object([
            ("name", Variant::string("Test")),
            ("value", Variant::int(42)),
            ("nested", Variant::object([("inner", Variant::null())])),
        ]);
        let encoded = original.encode();
        let decoded = Variant::decode(&encoded).unwrap();
        // Objects may not preserve order, so check individual fields
        assert_eq!(decoded.get("name").unwrap().as_str(), Some("Test"));
        assert_eq!(decoded.get("value").unwrap().as_i64(), Some(42));
        assert!(
            decoded
                .get("nested")
                .unwrap()
                .get("inner")
                .unwrap()
                .is_null()
        );
    }

    #[test]
    fn test_json_roundtrip() {
        let original = Variant::object([
            ("string", Variant::string("hello")),
            ("number", Variant::int(42)),
            ("float", Variant::double(2.5)),
            ("bool", Variant::boolean(true)),
            ("null", Variant::null()),
            ("array", Variant::array([Variant::int(1), Variant::int(2)])),
        ]);

        let json = original.to_json();
        let back = Variant::from_json(&json);

        assert_eq!(back.get("string").unwrap().as_str(), Some("hello"));
        assert_eq!(back.get("bool").unwrap().as_bool(), Some(true));
        assert!(back.get("null").unwrap().is_null());
    }

    #[test]
    fn test_count_types() {
        let v = Variant::object([
            ("name", Variant::string("test")),
            (
                "values",
                Variant::array([Variant::int(1), Variant::int(2), Variant::string("three")]),
            ),
        ]);

        let counts = v.count_types();
        assert_eq!(counts.get("object"), Some(&1));
        assert_eq!(counts.get("array"), Some(&1));
        assert_eq!(counts.get("string"), Some(&2)); // "test" and "three"
        assert_eq!(counts.get("int8"), Some(&2)); // 1 and 2
    }

    #[test]
    fn test_size_bytes() {
        let v = Variant::string("hello");
        let size = v.size_bytes();
        // 1 (tag) + 4 (length) + 5 (bytes) = 10
        assert_eq!(size, 10);
    }

    #[test]
    fn test_from_impls() {
        let _: Variant = true.into();
        let _: Variant = 42i32.into();
        let _: Variant = 42i64.into();
        let _: Variant = 2.5f64.into();
        let _: Variant = "hello".into();
        let _: Variant = String::from("hello").into();
    }
}
