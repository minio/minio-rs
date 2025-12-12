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

//! Roaring bitmap support for Iceberg V3 deletion vectors
//!
//! Roaring bitmaps are an efficient compressed bitmap format used by Iceberg V3
//! for deletion vectors. They provide excellent compression for sparse sets of
//! integers (like deleted row positions) while maintaining fast operations.
//!
//! # Format Overview
//!
//! Roaring bitmaps partition the 32-bit integer space into 2^16 chunks of 2^16
//! integers each. Each chunk is stored using one of three container types:
//!
//! - **Array Container**: Sorted array of 16-bit integers (for sparse chunks)
//! - **Bitmap Container**: 2^16 bits = 8KB bitmap (for dense chunks)
//! - **Run Container**: Run-length encoded ranges (for clustered data)
//!
//! # Serialization Format
//!
//! The portable serialization format used by Iceberg:
//!
//! ```text
//! +-------------------+
//! | Cookie (4 bytes)  | 0x3B30 (no runs) or 0x3B31 (with runs)
//! +-------------------+
//! | Container count   | 4 bytes (if cookie indicates runs)
//! +-------------------+
//! | Key/card pairs    | 4 bytes per container
//! +-------------------+
//! | Run flag bitset   | (if runs, ceil(n/8) bytes)
//! +-------------------+
//! | Container data    | variable
//! +-------------------+
//! ```
//!
//! # References
//!
//! - [Roaring Bitmap Paper](https://arxiv.org/abs/1603.06549)
//! - [Roaring Format Spec](https://github.com/RoaringBitmap/RoaringFormatSpec)

use std::collections::BTreeSet;
use std::io::{self, Read, Write};

/// Cookie value for roaring bitmap without run containers
pub const COOKIE_NO_RUNS: u32 = 12346;

/// Cookie value for roaring bitmap with run containers
pub const COOKIE_WITH_RUNS: u32 = 12347;

/// Serial cookie (indicates run-length encoding presence)
pub const SERIAL_COOKIE_NO_RUNS: u32 = 12346;
pub const SERIAL_COOKIE: u32 = 12347;

/// Maximum value in a roaring bitmap container (16-bit)
pub const CONTAINER_MAX: u16 = u16::MAX;

/// Threshold for switching from array to bitmap container
pub const ARRAY_TO_BITMAP_THRESHOLD: usize = 4096;

/// A simple roaring bitmap implementation for deletion vectors
///
/// This implementation focuses on reading deletion vectors from Iceberg.
/// For full roaring bitmap functionality, consider using the `roaring` crate.
#[derive(Debug, Clone, Default)]
pub struct RoaringBitmap {
    /// Set of values in the bitmap
    values: BTreeSet<u32>,
}

impl RoaringBitmap {
    /// Create an empty roaring bitmap
    pub fn new() -> Self {
        Self {
            values: BTreeSet::new(),
        }
    }

    /// Create a roaring bitmap from a collection of values
    pub fn from_values(values: impl IntoIterator<Item = u32>) -> Self {
        Self {
            values: values.into_iter().collect(),
        }
    }

    /// Add a value to the bitmap
    pub fn add(&mut self, value: u32) -> bool {
        self.values.insert(value)
    }

    /// Remove a value from the bitmap
    pub fn remove(&mut self, value: u32) -> bool {
        self.values.remove(&value)
    }

    /// Check if a value is in the bitmap
    pub fn contains(&self, value: u32) -> bool {
        self.values.contains(&value)
    }

    /// Get the cardinality (number of set bits)
    pub fn cardinality(&self) -> u64 {
        self.values.len() as u64
    }

    /// Check if the bitmap is empty
    pub fn is_empty(&self) -> bool {
        self.values.is_empty()
    }

    /// Iterate over all values in the bitmap
    pub fn iter(&self) -> impl Iterator<Item = u32> + '_ {
        self.values.iter().copied()
    }

    /// Get all values as a vector
    pub fn to_vec(&self) -> Vec<u32> {
        self.values.iter().copied().collect()
    }

    /// Deserialize a roaring bitmap from the portable format
    ///
    /// This reads the format used by Iceberg deletion vectors.
    pub fn deserialize<R: Read>(mut reader: R) -> io::Result<Self> {
        // Read cookie
        let mut cookie_bytes = [0u8; 4];
        reader.read_exact(&mut cookie_bytes)?;
        let cookie = u32::from_le_bytes(cookie_bytes);

        let (container_count, has_runs) = if cookie == SERIAL_COOKIE_NO_RUNS {
            // No run containers, next 4 bytes are container count - 1
            let mut count_bytes = [0u8; 4];
            reader.read_exact(&mut count_bytes)?;
            let count = u32::from_le_bytes(count_bytes) as usize + 1;
            (count, false)
        } else if (cookie & 0xFFFF) == SERIAL_COOKIE {
            // Has run containers, count is in upper 16 bits
            let count = ((cookie >> 16) + 1) as usize;
            (count, true)
        } else {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("Invalid roaring bitmap cookie: 0x{:08X}", cookie),
            ));
        };

        // Read key/cardinality pairs
        let mut keys = Vec::with_capacity(container_count);
        let mut cardinalities = Vec::with_capacity(container_count);

        for _ in 0..container_count {
            let mut pair_bytes = [0u8; 4];
            reader.read_exact(&mut pair_bytes)?;
            let key = u16::from_le_bytes([pair_bytes[0], pair_bytes[1]]);
            let card = u16::from_le_bytes([pair_bytes[2], pair_bytes[3]]) as usize + 1;
            keys.push(key);
            cardinalities.push(card);
        }

        // Read run flag bitset if present
        let run_flags = if has_runs {
            let flag_bytes = container_count.div_ceil(8);
            let mut flags = vec![0u8; flag_bytes];
            reader.read_exact(&mut flags)?;
            flags
        } else {
            vec![]
        };

        // Read containers
        let mut bitmap = RoaringBitmap::new();

        for i in 0..container_count {
            let key = keys[i] as u32;
            let base = key << 16;
            let cardinality = cardinalities[i];

            let is_run = has_runs && (run_flags[i / 8] & (1 << (i % 8))) != 0;

            if is_run {
                // Run container: pairs of (start, length-1)
                let _num_runs = cardinality; // In run containers, this is the run count
                let mut run_bytes = [0u8; 4];

                // Read number of runs
                reader.read_exact(&mut run_bytes[..2])?;
                let actual_runs = u16::from_le_bytes([run_bytes[0], run_bytes[1]]) as usize;

                for _ in 0..actual_runs {
                    reader.read_exact(&mut run_bytes)?;
                    let start = u16::from_le_bytes([run_bytes[0], run_bytes[1]]) as u32;
                    let length = u16::from_le_bytes([run_bytes[2], run_bytes[3]]) as u32 + 1;

                    for offset in 0..length {
                        bitmap.add(base + start + offset);
                    }
                }
            } else if cardinality <= ARRAY_TO_BITMAP_THRESHOLD {
                // Array container
                for _ in 0..cardinality {
                    let mut value_bytes = [0u8; 2];
                    reader.read_exact(&mut value_bytes)?;
                    let value = u16::from_le_bytes(value_bytes) as u32;
                    bitmap.add(base + value);
                }
            } else {
                // Bitmap container (8KB)
                let mut bitmap_data = vec![0u8; 8192];
                reader.read_exact(&mut bitmap_data)?;

                for (word_idx, chunk) in bitmap_data.chunks(8).enumerate() {
                    let word = u64::from_le_bytes(chunk.try_into().unwrap());
                    for bit in 0..64 {
                        if word & (1u64 << bit) != 0 {
                            let value = (word_idx * 64 + bit) as u32;
                            bitmap.add(base + value);
                        }
                    }
                }
            }
        }

        Ok(bitmap)
    }

    /// Serialize the roaring bitmap to the portable format
    ///
    /// This is a simplified serialization that only uses array containers.
    pub fn serialize<W: Write>(&self, mut writer: W) -> io::Result<()> {
        if self.values.is_empty() {
            // Empty bitmap: just write cookie and count of 0
            writer.write_all(&SERIAL_COOKIE_NO_RUNS.to_le_bytes())?;
            writer.write_all(&0u32.to_le_bytes())?; // 0 means 1 container, but we'll handle empty
            return Ok(());
        }

        // Group values by high 16 bits
        let mut containers: std::collections::BTreeMap<u16, Vec<u16>> =
            std::collections::BTreeMap::new();

        for &value in &self.values {
            let key = (value >> 16) as u16;
            let low = value as u16;
            containers.entry(key).or_default().push(low);
        }

        // Write cookie (no runs)
        writer.write_all(&SERIAL_COOKIE_NO_RUNS.to_le_bytes())?;

        // Write container count - 1
        let count_minus_one = (containers.len() - 1) as u32;
        writer.write_all(&count_minus_one.to_le_bytes())?;

        // Write key/cardinality pairs
        for (&key, values) in &containers {
            let card_minus_one = (values.len() - 1) as u16;
            writer.write_all(&key.to_le_bytes())?;
            writer.write_all(&card_minus_one.to_le_bytes())?;
        }

        // Write container data (all as array containers for simplicity)
        for values in containers.values() {
            if values.len() <= ARRAY_TO_BITMAP_THRESHOLD {
                // Array container
                for &value in values {
                    writer.write_all(&value.to_le_bytes())?;
                }
            } else {
                // Bitmap container
                let mut bitmap = vec![0u64; 1024]; // 1024 * 64 = 65536 bits
                for &value in values {
                    let word_idx = value as usize / 64;
                    let bit_idx = value as usize % 64;
                    bitmap[word_idx] |= 1u64 << bit_idx;
                }
                for word in bitmap {
                    writer.write_all(&word.to_le_bytes())?;
                }
            }
        }

        Ok(())
    }
}

/// Parse a deletion vector from raw bytes
///
/// Deletion vectors in Iceberg V3 use roaring bitmaps to track deleted rows.
pub fn parse_deletion_vector(data: &[u8]) -> io::Result<RoaringBitmap> {
    RoaringBitmap::deserialize(std::io::Cursor::new(data))
}

/// Check if a row position is deleted according to the deletion vector
pub fn is_row_deleted(deletion_vector: &RoaringBitmap, row_position: u32) -> bool {
    deletion_vector.contains(row_position)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    #[test]
    fn test_roaring_basic_operations() {
        let mut bitmap = RoaringBitmap::new();

        assert!(bitmap.is_empty());
        assert_eq!(bitmap.cardinality(), 0);

        bitmap.add(1);
        bitmap.add(100);
        bitmap.add(1000);

        assert!(!bitmap.is_empty());
        assert_eq!(bitmap.cardinality(), 3);

        assert!(bitmap.contains(1));
        assert!(bitmap.contains(100));
        assert!(bitmap.contains(1000));
        assert!(!bitmap.contains(2));

        bitmap.remove(100);
        assert!(!bitmap.contains(100));
        assert_eq!(bitmap.cardinality(), 2);
    }

    #[test]
    fn test_roaring_from_values() {
        let bitmap = RoaringBitmap::from_values([1, 2, 3, 100, 1000]);

        assert_eq!(bitmap.cardinality(), 5);
        assert!(bitmap.contains(1));
        assert!(bitmap.contains(100));
    }

    #[test]
    fn test_roaring_serialization_roundtrip() {
        let original = RoaringBitmap::from_values([1, 10, 100, 1000, 10000]);

        let mut buffer = Vec::new();
        original.serialize(&mut buffer).unwrap();

        let deserialized = RoaringBitmap::deserialize(Cursor::new(&buffer)).unwrap();

        assert_eq!(original.cardinality(), deserialized.cardinality());
        for value in original.iter() {
            assert!(deserialized.contains(value));
        }
    }

    #[test]
    fn test_roaring_large_values() {
        let bitmap = RoaringBitmap::from_values([0, 65535, 65536, 100000, u32::MAX - 1]);

        assert_eq!(bitmap.cardinality(), 5);
        assert!(bitmap.contains(0));
        assert!(bitmap.contains(65535));
        assert!(bitmap.contains(65536));
        assert!(bitmap.contains(100000));
        assert!(bitmap.contains(u32::MAX - 1));
    }

    #[test]
    fn test_is_row_deleted() {
        let bitmap = RoaringBitmap::from_values([5, 10, 15, 20]);

        assert!(!is_row_deleted(&bitmap, 0));
        assert!(is_row_deleted(&bitmap, 5));
        assert!(!is_row_deleted(&bitmap, 6));
        assert!(is_row_deleted(&bitmap, 10));
        assert!(is_row_deleted(&bitmap, 20));
        assert!(!is_row_deleted(&bitmap, 21));
    }
}
