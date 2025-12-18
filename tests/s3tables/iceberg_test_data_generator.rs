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

//! Multi-file Iceberg table test data generator
//!
//! Generates realistic, deterministic test data for S3 Tables benchmarking.
//! Creates multiple parquet files with controlled characteristics:
//! - Configurable total size (multiple of MB)
//! - Configurable number of files
//! - Deterministic random data (seeded RNG)
//! - Mixed data types with realistic distributions
//! - Column statistics for filter pushdown testing
//! - Optional partition key for file-level filtering
//! - Tracking of selectivity metrics for each filter

use rand::rngs::StdRng;

/// Selectivity information for a filter expression
#[derive(Debug, Clone)]
pub struct FilterSelectivity {
    /// Filter description (e.g., "status = 'active'")
    pub filter: String,
    /// Percentage of rows that match this filter (0-100)
    pub selectivity_pct: f64,
    /// Approximate rows matching this filter
    pub matching_rows: u64,
}

/// Metadata about generated test data
#[derive(Debug, Clone)]
pub struct TestDataMetadata {
    /// Total number of rows across all files
    pub total_rows: u64,
    /// Number of files created
    pub file_count: u32,
    /// Column definitions and their value ranges
    pub columns: Vec<ColumnMetadata>,
    /// Known filter selectivity metrics
    pub filter_selectivity: Vec<FilterSelectivity>,
    /// Partition key column (if any)
    pub partition_key: Option<String>,
}

/// Metadata about a single column
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct ColumnMetadata {
    /// Column name
    pub name: String,
    /// Column type (int, string, timestamp)
    pub data_type: String,
    /// Minimum value seen (for numeric/timestamp)
    pub min_value: Option<String>,
    /// Maximum value seen (for numeric/timestamp)
    pub max_value: Option<String>,
}

/// Configuration for test data generation
#[derive(Debug, Clone)]
pub struct TestDataConfig {
    /// Random seed for deterministic data generation
    pub seed: u64,
    /// Total data size in MB
    pub total_mb: u32,
    /// Number of files to create
    pub file_count: u32,
    /// Number of columns to generate (mix of types)
    pub column_count: u32,
    /// Optional column name to use as partition key (determines file placement)
    pub partition_key: Option<String>,
}

impl TestDataConfig {
    /// Create a new test data configuration
    pub fn new(total_mb: u32, file_count: u32, column_count: u32) -> Self {
        Self {
            seed: 42,
            total_mb,
            file_count,
            column_count,
            partition_key: None,
        }
    }

    /// Set the random seed for reproducibility
    pub fn with_seed(mut self, seed: u64) -> Self {
        self.seed = seed;
        self
    }

    /// Set a partition key column
    pub fn with_partition_key(mut self, key: String) -> Self {
        self.partition_key = Some(key);
        self
    }

    /// Calculate rows per file
    pub fn rows_per_file(&self) -> u64 {
        let bytes_per_file = (self.total_mb as u64) * 1024 * 1024;
        // Approximate: ~100 bytes per row average (varies by column count)
        let bytes_per_row = 80 + (self.column_count as u64 * 8);
        bytes_per_file / bytes_per_row
    }

    /// Calculate total rows
    pub fn total_rows(&self) -> u64 {
        self.rows_per_file() * (self.file_count as u64)
    }
}

/// Test data generator for Iceberg tables
pub struct IcebergTestDataGenerator {
    config: TestDataConfig,
}

impl IcebergTestDataGenerator {
    /// Create a new test data generator
    pub fn new(config: TestDataConfig) -> Self {
        Self { config }
    }

    /// Generate metadata describing the test data that would be created
    ///
    /// This is useful for understanding selectivity and data characteristics
    /// without actually generating all the parquet files.
    pub fn generate_metadata(&self) -> TestDataMetadata {
        let _seed = self.config.seed;

        // Generate column definitions
        let mut columns = Vec::new();
        for i in 0..self.config.column_count {
            let col_type = match i % 4 {
                0 => "id".to_string(),        // Long (partition-like)
                1 => "status".to_string(),    // String with skewed distribution
                2 => "timestamp".to_string(), // Timestamp
                _ => format!("value_{}", i),  // Int
            };

            columns.push(ColumnMetadata {
                name: col_type.clone(),
                data_type: match i % 4 {
                    0 => "Long".to_string(),
                    1 => "String".to_string(),
                    2 => "Timestamp".to_string(),
                    _ => "Int".to_string(),
                },
                min_value: Some("0".to_string()),
                max_value: Some(format!("{}", self.config.total_rows())),
            });
        }

        // Calculate known filter selectivity metrics
        let total_rows = self.config.total_rows();
        let filter_selectivity = vec![
            // Filter 1: status = 'active' (80% of rows - skewed distribution)
            FilterSelectivity {
                filter: "status = 'active'".to_string(),
                selectivity_pct: 80.0,
                matching_rows: (total_rows as f64 * 0.80) as u64,
            },
            // Filter 2: status = 'pending' (15% of rows)
            FilterSelectivity {
                filter: "status = 'pending'".to_string(),
                selectivity_pct: 15.0,
                matching_rows: (total_rows as f64 * 0.15) as u64,
            },
            // Filter 3: status = 'archived' (5% of rows)
            FilterSelectivity {
                filter: "status = 'archived'".to_string(),
                selectivity_pct: 5.0,
                matching_rows: (total_rows as f64 * 0.05) as u64,
            },
            // Filter 4: id > 50% (time-based-like filter)
            FilterSelectivity {
                filter: "id > (total_rows / 2)".to_string(),
                selectivity_pct: 50.0,
                matching_rows: total_rows / 2,
            },
            // Filter 5: id > 90% (tail filtering)
            FilterSelectivity {
                filter: "id > (0.9 * total_rows)".to_string(),
                selectivity_pct: 10.0,
                matching_rows: (total_rows as f64 * 0.10) as u64,
            },
        ];

        TestDataMetadata {
            total_rows: self.config.total_rows(),
            file_count: self.config.file_count,
            columns,
            filter_selectivity,
            partition_key: self.config.partition_key.clone(),
        }
    }

    /// Generate a deterministic string value for a row
    #[allow(dead_code)]
    fn generate_string_value(&self, rng: &mut StdRng, row_idx: u64, col_idx: u32) -> String {
        use rand::Rng;

        match col_idx % 4 {
            1 => {
                // status column: skewed distribution
                let rand_val: u32 = rng.random_range(0_u32..100_u32);
                if rand_val < 80 {
                    "active".to_string()
                } else if rand_val < 95 {
                    "pending".to_string()
                } else {
                    "archived".to_string()
                }
            }
            _ => {
                // Generic string with deterministic content
                format!("str_{}_{}", row_idx, col_idx)
            }
        }
    }

    /// Generate a deterministic int value for a row
    #[allow(dead_code)]
    fn generate_int_value(&self, row_idx: u64, col_idx: u32) -> i64 {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        let mut hasher = DefaultHasher::new();
        row_idx.hash(&mut hasher);
        col_idx.hash(&mut hasher);
        hasher.finish() as i64
    }

    /// Generate a timestamp value deterministically
    #[allow(dead_code)]
    fn generate_timestamp_value(&self, row_idx: u64, col_idx: u32) -> i64 {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        let mut hasher = DefaultHasher::new();
        row_idx.hash(&mut hasher);
        col_idx.hash(&mut hasher);
        // Spread timestamps across a realistic range (year 2024-2025)
        let base = 1704067200; // 2024-01-01
        let range = 365 * 24 * 3600; // One year in seconds
        base + (hasher.finish() % (range as u64)) as i64
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_config_calculations() {
        let config = TestDataConfig::new(100, 10, 4);
        assert_eq!(config.file_count, 10);
        assert_eq!(config.total_mb, 100);
        assert_eq!(config.column_count, 4);

        let rows_per_file = config.rows_per_file();
        let total_rows = config.total_rows();
        assert!(total_rows > 0);
        assert_eq!(total_rows, rows_per_file * 10);
    }

    #[test]
    fn test_metadata_generation() {
        let config = TestDataConfig::new(10, 2, 4);
        let generator = IcebergTestDataGenerator::new(config);
        let metadata = generator.generate_metadata();

        assert_eq!(metadata.file_count, 2);
        assert!(metadata.total_rows > 0);
        assert_eq!(metadata.columns.len(), 4);
        assert!(!metadata.filter_selectivity.is_empty());
    }

    #[test]
    fn test_filter_selectivity_sums_reasonably() {
        let config = TestDataConfig::new(100, 5, 4);
        let generator = IcebergTestDataGenerator::new(config);
        let metadata = generator.generate_metadata();

        let status_filters: Vec<_> = metadata
            .filter_selectivity
            .iter()
            .filter(|f| f.filter.contains("status"))
            .collect();

        // Should have status filters
        assert!(!status_filters.is_empty());

        // Status filters should sum to ~100%
        let total_selectivity: f64 = status_filters.iter().map(|f| f.selectivity_pct).sum();
        assert!((total_selectivity - 100.0).abs() < 0.1);
    }

    #[test]
    fn test_partition_key_configuration() {
        let config = TestDataConfig::new(100, 10, 4).with_partition_key("id".to_string());

        assert!(config.partition_key.is_some());
        assert_eq!(config.partition_key.unwrap(), "id");
    }

    #[test]
    fn test_seed_reproducibility() {
        let config1 = TestDataConfig::new(10, 2, 4).with_seed(12345);
        let config2 = TestDataConfig::new(10, 2, 4).with_seed(12345);

        let gen1 = IcebergTestDataGenerator::new(config1);
        let gen2 = IcebergTestDataGenerator::new(config2);

        let meta1 = gen1.generate_metadata();
        let meta2 = gen2.generate_metadata();

        // Same seed should produce identical metadata
        assert_eq!(meta1.total_rows, meta2.total_rows);
        assert_eq!(meta1.columns.len(), meta2.columns.len());
        assert_eq!(
            meta1.filter_selectivity.len(),
            meta2.filter_selectivity.len()
        );
    }
}
