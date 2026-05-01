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

//! Statistics collection for Iceberg V3 types
//!
//! This module provides collectors to compute column statistics for the new
//! Iceberg V3 types: Geometry, Geography, and Variant. These statistics are
//! stored in manifest files and used for query optimization (predicate pushdown,
//! partition pruning).
//!
//! # Spatial Statistics
//!
//! For Geometry and Geography columns, statistics include:
//! - **Bounding box**: Min/max coordinates enclosing all geometries
//! - **CRS**: Coordinate Reference System identifier
//! - **Value/null counts**: For cardinality estimation
//!
//! # Variant Statistics
//!
//! For Variant columns, statistics include:
//! - **Type distribution**: Count of each top-level type
//! - **Size metrics**: Total serialized size
//! - **Value/null counts**: For cardinality estimation
//!
//! # Example
//!
//! ```
//! use minio::s3tables::statistics::{SpatialStatsCollector, VariantStatsCollector};
//! use minio::s3tables::variant::Variant;
//!
//! // Collect spatial statistics from WKB data
//! let mut spatial = SpatialStatsCollector::new();
//! // spatial.add_wkb(&wkb_bytes);
//! // spatial.add_null();
//! // let stats = spatial.finish();
//!
//! // Collect variant statistics
//! let mut variant = VariantStatsCollector::new();
//! variant.add_value(&Variant::string("hello"));
//! variant.add_value(&Variant::int(42));
//! let stats = variant.finish();
//! ```
//!
//! # References
//!
//! - [Iceberg V3 Spec](https://iceberg.apache.org/spec/#version-3)
//! - [Iceberg Manifest Files](https://iceberg.apache.org/spec/#manifests)
//! - [Iceberg Column Statistics](https://iceberg.apache.org/spec/#column-statistics)

use std::collections::HashMap;

use crate::s3tables::types::iceberg::{BoundingBox, SpatialStatistics, VariantStatistics};
use crate::s3tables::variant::Variant;
use crate::s3tables::wkb::{WkbError, bounding_box_from_wkb};

/// Collector for spatial (Geometry/Geography) column statistics
///
/// Tracks bounding boxes and value counts for spatial columns.
#[derive(Debug, Clone)]
pub struct SpatialStatsCollector {
    /// Current bounding box (expanded as values are added)
    bbox: Option<BoundingBox>,
    /// Coordinate reference system
    crs: Option<String>,
    /// Number of non-null values
    value_count: i64,
    /// Number of null values
    null_count: i64,
    /// Total size of all geometry values in bytes
    total_size_bytes: i64,
}

impl Default for SpatialStatsCollector {
    fn default() -> Self {
        Self::new()
    }
}

impl SpatialStatsCollector {
    /// Create a new spatial statistics collector
    pub fn new() -> Self {
        Self {
            bbox: None,
            crs: None,
            value_count: 0,
            null_count: 0,
            total_size_bytes: 0,
        }
    }

    /// Create a new collector with a specific CRS
    pub fn with_crs(crs: impl Into<String>) -> Self {
        Self {
            crs: Some(crs.into()),
            ..Self::new()
        }
    }

    /// Add a WKB-encoded geometry value
    ///
    /// Returns an error if the WKB is invalid.
    pub fn add_wkb(&mut self, wkb: &[u8]) -> Result<(), WkbError> {
        self.total_size_bytes += wkb.len() as i64;
        self.value_count += 1;

        if let Some(geom_bbox) = bounding_box_from_wkb(wkb)? {
            self.expand_bbox(&geom_bbox);
        }

        Ok(())
    }

    /// Add a null value
    pub fn add_null(&mut self) {
        self.null_count += 1;
    }

    /// Add a pre-computed bounding box
    ///
    /// Useful when the bounding box is already known (e.g., from Parquet statistics).
    pub fn add_bbox(&mut self, bbox: &BoundingBox, size_bytes: i64) {
        self.value_count += 1;
        self.total_size_bytes += size_bytes;
        self.expand_bbox(bbox);
    }

    /// Merge another collector's statistics into this one
    pub fn merge(&mut self, other: &SpatialStatsCollector) {
        self.value_count += other.value_count;
        self.null_count += other.null_count;
        self.total_size_bytes += other.total_size_bytes;

        if let Some(other_bbox) = &other.bbox {
            self.expand_bbox(other_bbox);
        }

        // Keep CRS if we don't have one
        if self.crs.is_none() {
            self.crs.clone_from(&other.crs);
        }
    }

    /// Expand the current bounding box to include another
    fn expand_bbox(&mut self, other: &BoundingBox) {
        match &mut self.bbox {
            Some(bbox) => {
                bbox.x_min = bbox.x_min.min(other.x_min);
                bbox.x_max = bbox.x_max.max(other.x_max);
                bbox.y_min = bbox.y_min.min(other.y_min);
                bbox.y_max = bbox.y_max.max(other.y_max);
                // Handle Z coordinates
                match (bbox.z_min, bbox.z_max, other.z_min, other.z_max) {
                    (Some(z_min), Some(z_max), Some(other_z_min), Some(other_z_max)) => {
                        bbox.z_min = Some(z_min.min(other_z_min));
                        bbox.z_max = Some(z_max.max(other_z_max));
                    }
                    (None, None, Some(z_min), Some(z_max)) => {
                        bbox.z_min = Some(z_min);
                        bbox.z_max = Some(z_max);
                    }
                    _ => {}
                }
            }
            None => {
                self.bbox = Some(other.clone());
            }
        }
    }

    /// Finish collecting and return the statistics
    pub fn finish(self) -> SpatialStatistics {
        SpatialStatistics {
            bounding_box: self.bbox,
            crs: self.crs,
            value_count: if self.value_count > 0 {
                Some(self.value_count)
            } else {
                None
            },
            null_count: if self.null_count > 0 {
                Some(self.null_count)
            } else {
                None
            },
            total_size_bytes: if self.total_size_bytes > 0 {
                Some(self.total_size_bytes)
            } else {
                None
            },
        }
    }

    /// Get the current bounding box
    pub fn bounding_box(&self) -> Option<&BoundingBox> {
        self.bbox.as_ref()
    }

    /// Get the current value count
    pub fn value_count(&self) -> i64 {
        self.value_count
    }

    /// Get the current null count
    pub fn null_count(&self) -> i64 {
        self.null_count
    }
}

/// Collector for Variant column statistics
///
/// Tracks value counts, sizes, and type distribution for variant columns.
#[derive(Debug, Clone)]
pub struct VariantStatsCollector {
    /// Number of non-null values
    value_count: i64,
    /// Number of null values
    null_count: i64,
    /// Total serialized size of all variant values
    total_size_bytes: i64,
    /// Count of each top-level type encountered
    type_counts: HashMap<String, i64>,
}

impl Default for VariantStatsCollector {
    fn default() -> Self {
        Self::new()
    }
}

impl VariantStatsCollector {
    /// Create a new variant statistics collector
    pub fn new() -> Self {
        Self {
            value_count: 0,
            null_count: 0,
            total_size_bytes: 0,
            type_counts: HashMap::new(),
        }
    }

    /// Add a variant value
    pub fn add_value(&mut self, value: &Variant) {
        if value.is_null() {
            self.null_count += 1;
            return;
        }

        self.value_count += 1;
        self.total_size_bytes += value.size_bytes() as i64;

        // Track top-level type
        let type_name = value.type_name().to_string();
        *self.type_counts.entry(type_name).or_insert(0) += 1;
    }

    /// Add a null value
    pub fn add_null(&mut self) {
        self.null_count += 1;
    }

    /// Add a pre-encoded variant (binary data)
    ///
    /// Returns an error if decoding fails.
    pub fn add_encoded(
        &mut self,
        data: &[u8],
    ) -> Result<(), crate::s3tables::variant::VariantError> {
        let variant = Variant::decode(data)?;
        self.add_value(&variant);
        Ok(())
    }

    /// Merge another collector's statistics into this one
    pub fn merge(&mut self, other: &VariantStatsCollector) {
        self.value_count += other.value_count;
        self.null_count += other.null_count;
        self.total_size_bytes += other.total_size_bytes;

        for (type_name, count) in &other.type_counts {
            *self.type_counts.entry(type_name.clone()).or_insert(0) += count;
        }
    }

    /// Finish collecting and return the statistics
    pub fn finish(self) -> VariantStatistics {
        // Get distinct type count
        let distinct_type_count = self.type_counts.len() as i64;

        // Get most common types (sorted by count, descending)
        let mut types_vec: Vec<_> = self.type_counts.into_iter().collect();
        types_vec.sort_by(|a, b| b.1.cmp(&a.1));
        let common_types: Vec<String> = types_vec.into_iter().map(|(name, _)| name).collect();

        VariantStatistics {
            value_count: if self.value_count > 0 {
                Some(self.value_count)
            } else {
                None
            },
            null_count: if self.null_count > 0 {
                Some(self.null_count)
            } else {
                None
            },
            total_size_bytes: if self.total_size_bytes > 0 {
                Some(self.total_size_bytes)
            } else {
                None
            },
            distinct_type_count: if distinct_type_count > 0 {
                Some(distinct_type_count)
            } else {
                None
            },
            common_types: if !common_types.is_empty() {
                Some(common_types)
            } else {
                None
            },
        }
    }

    /// Get the current value count
    pub fn value_count(&self) -> i64 {
        self.value_count
    }

    /// Get the current null count
    pub fn null_count(&self) -> i64 {
        self.null_count
    }

    /// Get the number of distinct types seen
    pub fn distinct_type_count(&self) -> usize {
        self.type_counts.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_spatial_collector_empty() {
        let collector = SpatialStatsCollector::new();
        let stats = collector.finish();

        assert!(stats.bounding_box.is_none());
        assert!(stats.value_count.is_none());
        assert!(stats.null_count.is_none());
    }

    #[test]
    fn test_spatial_collector_with_crs() {
        let collector = SpatialStatsCollector::with_crs("EPSG:4326");
        let stats = collector.finish();

        assert_eq!(stats.crs, Some("EPSG:4326".to_string()));
    }

    #[test]
    fn test_spatial_collector_nulls() {
        let mut collector = SpatialStatsCollector::new();
        collector.add_null();
        collector.add_null();

        let stats = collector.finish();
        assert_eq!(stats.null_count, Some(2));
        assert!(stats.value_count.is_none());
    }

    #[test]
    fn test_spatial_collector_single_point() {
        // Create WKB for POINT(10.0, 20.0)
        let mut wkb = vec![
            0x01, // little-endian
            0x01, 0x00, 0x00, 0x00, // Point type
        ];
        wkb.extend_from_slice(&10.0_f64.to_le_bytes());
        wkb.extend_from_slice(&20.0_f64.to_le_bytes());

        let mut collector = SpatialStatsCollector::new();
        collector.add_wkb(&wkb).unwrap();

        let stats = collector.finish();
        let bbox = stats.bounding_box.unwrap();
        assert_eq!(bbox.x_min, 10.0);
        assert_eq!(bbox.x_max, 10.0);
        assert_eq!(bbox.y_min, 20.0);
        assert_eq!(bbox.y_max, 20.0);
        assert_eq!(stats.value_count, Some(1));
    }

    #[test]
    fn test_spatial_collector_multiple_points() {
        // Point 1: (0, 0)
        let mut wkb1 = vec![0x01, 0x01, 0x00, 0x00, 0x00];
        wkb1.extend_from_slice(&0.0_f64.to_le_bytes());
        wkb1.extend_from_slice(&0.0_f64.to_le_bytes());

        // Point 2: (10, 20)
        let mut wkb2 = vec![0x01, 0x01, 0x00, 0x00, 0x00];
        wkb2.extend_from_slice(&10.0_f64.to_le_bytes());
        wkb2.extend_from_slice(&20.0_f64.to_le_bytes());

        // Point 3: (-5, 15)
        let mut wkb3 = vec![0x01, 0x01, 0x00, 0x00, 0x00];
        wkb3.extend_from_slice(&(-5.0_f64).to_le_bytes());
        wkb3.extend_from_slice(&15.0_f64.to_le_bytes());

        let mut collector = SpatialStatsCollector::new();
        collector.add_wkb(&wkb1).unwrap();
        collector.add_wkb(&wkb2).unwrap();
        collector.add_wkb(&wkb3).unwrap();
        collector.add_null();

        let stats = collector.finish();
        let bbox = stats.bounding_box.unwrap();
        assert_eq!(bbox.x_min, -5.0);
        assert_eq!(bbox.x_max, 10.0);
        assert_eq!(bbox.y_min, 0.0);
        assert_eq!(bbox.y_max, 20.0);
        assert_eq!(stats.value_count, Some(3));
        assert_eq!(stats.null_count, Some(1));
    }

    #[test]
    fn test_spatial_collector_merge() {
        // Point 1: (0, 0)
        let mut wkb1 = vec![0x01, 0x01, 0x00, 0x00, 0x00];
        wkb1.extend_from_slice(&0.0_f64.to_le_bytes());
        wkb1.extend_from_slice(&0.0_f64.to_le_bytes());

        // Point 2: (100, 100)
        let mut wkb2 = vec![0x01, 0x01, 0x00, 0x00, 0x00];
        wkb2.extend_from_slice(&100.0_f64.to_le_bytes());
        wkb2.extend_from_slice(&100.0_f64.to_le_bytes());

        let mut collector1 = SpatialStatsCollector::new();
        collector1.add_wkb(&wkb1).unwrap();

        let mut collector2 = SpatialStatsCollector::with_crs("EPSG:4326");
        collector2.add_wkb(&wkb2).unwrap();
        collector2.add_null();

        collector1.merge(&collector2);
        let stats = collector1.finish();

        let bbox = stats.bounding_box.unwrap();
        assert_eq!(bbox.x_min, 0.0);
        assert_eq!(bbox.x_max, 100.0);
        assert_eq!(stats.value_count, Some(2));
        assert_eq!(stats.null_count, Some(1));
        assert_eq!(stats.crs, Some("EPSG:4326".to_string()));
    }

    #[test]
    fn test_spatial_collector_add_bbox() {
        let mut collector = SpatialStatsCollector::new();

        let bbox1 = BoundingBox::new_2d(0.0, 10.0, 0.0, 10.0);
        collector.add_bbox(&bbox1, 100);

        let bbox2 = BoundingBox::new_2d(5.0, 20.0, 5.0, 20.0);
        collector.add_bbox(&bbox2, 150);

        let stats = collector.finish();
        let bbox = stats.bounding_box.unwrap();
        assert_eq!(bbox.x_min, 0.0);
        assert_eq!(bbox.x_max, 20.0);
        assert_eq!(bbox.y_min, 0.0);
        assert_eq!(bbox.y_max, 20.0);
        assert_eq!(stats.total_size_bytes, Some(250));
    }

    #[test]
    fn test_variant_collector_empty() {
        let collector = VariantStatsCollector::new();
        let stats = collector.finish();

        assert!(stats.value_count.is_none());
        assert!(stats.null_count.is_none());
        assert!(stats.distinct_type_count.is_none());
    }

    #[test]
    fn test_variant_collector_nulls() {
        let mut collector = VariantStatsCollector::new();
        collector.add_null();
        collector.add_value(&Variant::null());
        collector.add_null();

        let stats = collector.finish();
        assert_eq!(stats.null_count, Some(3));
        assert!(stats.value_count.is_none());
    }

    #[test]
    fn test_variant_collector_mixed_types() {
        let mut collector = VariantStatsCollector::new();
        collector.add_value(&Variant::string("hello"));
        collector.add_value(&Variant::string("world"));
        collector.add_value(&Variant::int(42));
        collector.add_value(&Variant::boolean(true));
        collector.add_value(&Variant::object([("key", Variant::string("value"))]));

        let stats = collector.finish();
        assert_eq!(stats.value_count, Some(5));
        assert_eq!(stats.distinct_type_count, Some(4)); // string, int8, boolean, object

        let common = stats.common_types.unwrap();
        assert_eq!(common[0], "string"); // Most common (2 occurrences)
    }

    #[test]
    fn test_variant_collector_size_tracking() {
        let mut collector = VariantStatsCollector::new();

        let v1 = Variant::string("hello"); // 1 + 4 + 5 = 10 bytes
        let v2 = Variant::int(42); // 1 + 1 = 2 bytes (int8)

        collector.add_value(&v1);
        collector.add_value(&v2);

        let stats = collector.finish();
        assert_eq!(stats.total_size_bytes, Some(12));
    }

    #[test]
    fn test_variant_collector_merge() {
        let mut collector1 = VariantStatsCollector::new();
        collector1.add_value(&Variant::string("a"));
        collector1.add_value(&Variant::string("b"));

        let mut collector2 = VariantStatsCollector::new();
        collector2.add_value(&Variant::int(1));
        collector2.add_null();

        collector1.merge(&collector2);
        let stats = collector1.finish();

        assert_eq!(stats.value_count, Some(3));
        assert_eq!(stats.null_count, Some(1));
        assert_eq!(stats.distinct_type_count, Some(2)); // string, int8
    }

    #[test]
    fn test_variant_collector_add_encoded() {
        let original = Variant::string("test");
        let encoded = original.encode();

        let mut collector = VariantStatsCollector::new();
        collector.add_encoded(&encoded).unwrap();

        let stats = collector.finish();
        assert_eq!(stats.value_count, Some(1));

        let types = stats.common_types.unwrap();
        assert_eq!(types[0], "string");
    }

    #[test]
    fn test_variant_collector_type_distribution() {
        let mut collector = VariantStatsCollector::new();

        // Add 5 strings, 3 ints, 2 booleans
        for _ in 0..5 {
            collector.add_value(&Variant::string("x"));
        }
        for _ in 0..3 {
            collector.add_value(&Variant::int(1));
        }
        for _ in 0..2 {
            collector.add_value(&Variant::boolean(true));
        }

        let stats = collector.finish();
        let common = stats.common_types.unwrap();

        // Should be sorted by frequency
        assert_eq!(common[0], "string");
        assert_eq!(common[1], "int8");
        assert_eq!(common[2], "boolean");
    }
}
