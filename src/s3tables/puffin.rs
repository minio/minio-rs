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

//! Puffin file format for Iceberg V3
//!
//! Puffin is a container file format used by Apache Iceberg for storing
//! auxiliary data blobs such as:
//! - Column statistics
//! - Deletion vectors (V3)
//! - Bloom filters
//!
//! # File Structure
//!
//! ```text
//! +------------------+
//! | Magic "PUF1"     | 4 bytes
//! +------------------+
//! | Blob 1           | variable
//! +------------------+
//! | Blob 2           | variable
//! +------------------+
//! | ...              |
//! +------------------+
//! | Footer Payload   | variable (JSON)
//! +------------------+
//! | Footer Length    | 4 bytes (little-endian)
//! +------------------+
//! | Flags            | 4 bytes
//! +------------------+
//! | Magic "PUF1"     | 4 bytes
//! +------------------+
//! ```
//!
//! # Compression Support
//!
//! Enable the `puffin-compression` feature to support LZ4 and Zstd compression:
//!
//! ```toml
//! minio = { version = "0.3", features = ["puffin-compression"] }
//! ```
//!
//! Without this feature, compressed blobs will return an `Unsupported` error.
//!
//! # References
//!
//! - [Puffin Spec](https://iceberg.apache.org/puffin-spec/)

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::io::{self, Read, Seek, SeekFrom, Write};

// ============================================================================
// Compression Module
// ============================================================================

/// Compression/decompression support for Puffin blobs
pub mod compression {
    use std::io;

    /// Decompress LZ4 data
    ///
    /// Requires the `puffin-compression` feature.
    #[cfg(feature = "puffin-compression")]
    pub fn decompress_lz4(data: &[u8]) -> io::Result<Vec<u8>> {
        lz4_flex::decompress_size_prepended(data).map_err(|e| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("LZ4 decompression failed: {}", e),
            )
        })
    }

    /// Decompress LZ4 data (stub when feature not enabled)
    #[cfg(not(feature = "puffin-compression"))]
    pub fn decompress_lz4(_data: &[u8]) -> io::Result<Vec<u8>> {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "LZ4 decompression requires the 'puffin-compression' feature",
        ))
    }

    /// Decompress Zstd data
    ///
    /// Requires the `puffin-compression` feature.
    #[cfg(feature = "puffin-compression")]
    pub fn decompress_zstd(data: &[u8]) -> io::Result<Vec<u8>> {
        zstd::decode_all(std::io::Cursor::new(data)).map_err(|e| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("Zstd decompression failed: {}", e),
            )
        })
    }

    /// Decompress Zstd data (stub when feature not enabled)
    #[cfg(not(feature = "puffin-compression"))]
    pub fn decompress_zstd(_data: &[u8]) -> io::Result<Vec<u8>> {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "Zstd decompression requires the 'puffin-compression' feature",
        ))
    }

    /// Compress data with LZ4
    ///
    /// Requires the `puffin-compression` feature.
    #[cfg(feature = "puffin-compression")]
    pub fn compress_lz4(data: &[u8]) -> io::Result<Vec<u8>> {
        Ok(lz4_flex::compress_prepend_size(data))
    }

    /// Compress data with LZ4 (stub when feature not enabled)
    #[cfg(not(feature = "puffin-compression"))]
    pub fn compress_lz4(_data: &[u8]) -> io::Result<Vec<u8>> {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "LZ4 compression requires the 'puffin-compression' feature",
        ))
    }

    /// Compress data with Zstd
    ///
    /// Requires the `puffin-compression` feature.
    /// Uses compression level 3 (default, good balance of speed and ratio).
    #[cfg(feature = "puffin-compression")]
    pub fn compress_zstd(data: &[u8]) -> io::Result<Vec<u8>> {
        compress_zstd_with_level(data, 3)
    }

    /// Compress data with Zstd at a specific compression level
    ///
    /// Requires the `puffin-compression` feature.
    /// Level ranges from 1 (fastest) to 22 (best compression).
    #[cfg(feature = "puffin-compression")]
    pub fn compress_zstd_with_level(data: &[u8], level: i32) -> io::Result<Vec<u8>> {
        zstd::encode_all(std::io::Cursor::new(data), level).map_err(|e| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("Zstd compression failed: {}", e),
            )
        })
    }

    /// Compress data with Zstd (stub when feature not enabled)
    #[cfg(not(feature = "puffin-compression"))]
    pub fn compress_zstd(_data: &[u8]) -> io::Result<Vec<u8>> {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "Zstd compression requires the 'puffin-compression' feature",
        ))
    }

    /// Compress data with Zstd at a specific level (stub when feature not enabled)
    #[cfg(not(feature = "puffin-compression"))]
    pub fn compress_zstd_with_level(_data: &[u8], _level: i32) -> io::Result<Vec<u8>> {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "Zstd compression requires the 'puffin-compression' feature",
        ))
    }

    /// Check if compression support is available
    pub fn is_compression_available() -> bool {
        cfg!(feature = "puffin-compression")
    }
}

/// Puffin file magic bytes
pub const PUFFIN_MAGIC: &[u8; 4] = b"PUF1";

/// Puffin file header size (magic bytes)
pub const PUFFIN_HEADER_SIZE: usize = 4;

/// Puffin file footer overhead (length + flags + magic)
pub const PUFFIN_FOOTER_OVERHEAD: usize = 12;

/// Flag indicating footer is compressed with LZ4
pub const FLAG_FOOTER_COMPRESSED_LZ4: u32 = 0x01;

/// Flag indicating footer is compressed with Zstd
pub const FLAG_FOOTER_COMPRESSED_ZSTD: u32 = 0x02;

/// Blob type for deletion vectors
pub const BLOB_TYPE_DELETION_VECTOR: &str = "deletion-vector-v1";

/// Blob type for Apache DataSketches theta sketch
pub const BLOB_TYPE_THETA_SKETCH: &str = "apache-datasketches-theta-v1";

/// Compression codec used for blob data
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum CompressionCodec {
    /// No compression
    None,
    /// LZ4 compression
    Lz4,
    /// Zstandard compression
    Zstd,
}

impl Default for CompressionCodec {
    fn default() -> Self {
        Self::None
    }
}

/// Metadata for a single blob in a Puffin file
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BlobMetadata {
    /// Type of blob (e.g., "deletion-vector-v1", "apache-datasketches-theta-v1")
    #[serde(rename = "type")]
    pub blob_type: String,

    /// Fields that this blob is associated with (by field ID)
    pub fields: Vec<i32>,

    /// Snapshot ID that this blob is associated with
    #[serde(rename = "snapshot-id")]
    pub snapshot_id: i64,

    /// Sequence number that this blob is associated with
    #[serde(rename = "sequence-number")]
    pub sequence_number: i64,

    /// Byte offset of the blob data in the file
    pub offset: i64,

    /// Length of the blob data in bytes
    pub length: i64,

    /// Compression codec used for the blob data
    #[serde(rename = "compression-codec", default)]
    pub compression_codec: CompressionCodec,

    /// Additional properties for the blob
    #[serde(default)]
    pub properties: HashMap<String, String>,
}

/// Puffin file footer containing metadata about all blobs
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PuffinFooter {
    /// List of blob metadata
    pub blobs: Vec<BlobMetadata>,

    /// Additional properties for the file
    #[serde(default)]
    pub properties: HashMap<String, String>,
}

/// A blob read from a Puffin file
#[derive(Debug, Clone)]
pub struct PuffinBlob {
    /// Blob metadata
    pub metadata: BlobMetadata,
    /// Blob data (decompressed)
    pub data: Vec<u8>,
}

/// Puffin file reader
///
/// Reads and parses Puffin files, providing access to blob metadata and data.
#[derive(Debug)]
pub struct PuffinReader<R> {
    reader: R,
    footer: PuffinFooter,
    file_length: u64,
}

impl<R: Read + Seek> PuffinReader<R> {
    /// Open a Puffin file and read its footer
    pub fn open(mut reader: R) -> io::Result<Self> {
        // Get file length
        let file_length = reader.seek(SeekFrom::End(0))?;

        // Validate minimum file size
        if file_length < (PUFFIN_HEADER_SIZE + PUFFIN_FOOTER_OVERHEAD) as u64 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "File too small to be a valid Puffin file",
            ));
        }

        // Read and validate header magic
        reader.seek(SeekFrom::Start(0))?;
        let mut header_magic = [0u8; 4];
        reader.read_exact(&mut header_magic)?;
        if &header_magic != PUFFIN_MAGIC {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Invalid Puffin file: wrong header magic",
            ));
        }

        // Read footer magic (last 4 bytes)
        reader.seek(SeekFrom::End(-4))?;
        let mut footer_magic = [0u8; 4];
        reader.read_exact(&mut footer_magic)?;
        if &footer_magic != PUFFIN_MAGIC {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Invalid Puffin file: wrong footer magic",
            ));
        }

        // Read flags (4 bytes before footer magic)
        reader.seek(SeekFrom::End(-8))?;
        let mut flags_bytes = [0u8; 4];
        reader.read_exact(&mut flags_bytes)?;
        let flags = u32::from_le_bytes(flags_bytes);

        // Read footer length (4 bytes before flags)
        reader.seek(SeekFrom::End(-12))?;
        let mut length_bytes = [0u8; 4];
        reader.read_exact(&mut length_bytes)?;
        let footer_length = u32::from_le_bytes(length_bytes) as usize;

        // Calculate footer payload position
        let footer_start = file_length as usize - PUFFIN_FOOTER_OVERHEAD - footer_length;

        // Read footer payload
        reader.seek(SeekFrom::Start(footer_start as u64))?;
        let mut footer_data = vec![0u8; footer_length];
        reader.read_exact(&mut footer_data)?;

        // Decompress footer if needed
        let footer_json = if flags & FLAG_FOOTER_COMPRESSED_LZ4 != 0 {
            compression::decompress_lz4(&footer_data)?
        } else if flags & FLAG_FOOTER_COMPRESSED_ZSTD != 0 {
            compression::decompress_zstd(&footer_data)?
        } else {
            footer_data
        };

        // Parse footer JSON
        let footer: PuffinFooter = serde_json::from_slice(&footer_json).map_err(|e| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("Invalid Puffin footer JSON: {}", e),
            )
        })?;

        Ok(Self {
            reader,
            footer,
            file_length,
        })
    }

    /// Get the footer metadata
    pub fn footer(&self) -> &PuffinFooter {
        &self.footer
    }

    /// Get the file length in bytes
    pub fn file_length(&self) -> u64 {
        self.file_length
    }

    /// Get the number of blobs in the file
    pub fn blob_count(&self) -> usize {
        self.footer.blobs.len()
    }

    /// Get metadata for a specific blob by index
    pub fn blob_metadata(&self, index: usize) -> Option<&BlobMetadata> {
        self.footer.blobs.get(index)
    }

    /// Read a blob by index
    pub fn read_blob(&mut self, index: usize) -> io::Result<PuffinBlob> {
        let metadata = self
            .footer
            .blobs
            .get(index)
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "Blob index out of range"))?
            .clone();

        // Seek to blob position
        self.reader.seek(SeekFrom::Start(metadata.offset as u64))?;

        // Read blob data
        let mut data = vec![0u8; metadata.length as usize];
        self.reader.read_exact(&mut data)?;

        // Decompress if needed
        let decompressed = match metadata.compression_codec {
            CompressionCodec::None => data,
            CompressionCodec::Lz4 => compression::decompress_lz4(&data)?,
            CompressionCodec::Zstd => compression::decompress_zstd(&data)?,
        };

        Ok(PuffinBlob {
            metadata,
            data: decompressed,
        })
    }

    /// Find blobs by type
    pub fn find_blobs_by_type(&self, blob_type: &str) -> Vec<&BlobMetadata> {
        self.footer
            .blobs
            .iter()
            .filter(|b| b.blob_type == blob_type)
            .collect()
    }

    /// Find deletion vector blobs
    pub fn find_deletion_vectors(&self) -> Vec<&BlobMetadata> {
        self.find_blobs_by_type(BLOB_TYPE_DELETION_VECTOR)
    }
}

/// Puffin file writer
///
/// Creates Puffin files with blob data.
#[derive(Debug)]
pub struct PuffinWriter<W> {
    writer: W,
    blobs: Vec<BlobMetadata>,
    current_offset: i64,
    properties: HashMap<String, String>,
}

impl<W: Write + Seek> PuffinWriter<W> {
    /// Create a new Puffin file writer
    pub fn new(mut writer: W) -> io::Result<Self> {
        // Write header magic
        writer.write_all(PUFFIN_MAGIC)?;

        Ok(Self {
            writer,
            blobs: Vec::new(),
            current_offset: PUFFIN_HEADER_SIZE as i64,
            properties: HashMap::new(),
        })
    }

    /// Add a property to the file
    pub fn add_property(&mut self, key: impl Into<String>, value: impl Into<String>) {
        self.properties.insert(key.into(), value.into());
    }

    /// Write a blob to the file (uncompressed)
    pub fn write_blob(
        &mut self,
        blob_type: impl Into<String>,
        data: &[u8],
        fields: Vec<i32>,
        snapshot_id: i64,
        sequence_number: i64,
        properties: HashMap<String, String>,
    ) -> io::Result<usize> {
        self.write_blob_with_compression(
            blob_type,
            data,
            fields,
            snapshot_id,
            sequence_number,
            properties,
            CompressionCodec::None,
        )
    }

    /// Write a blob to the file with optional compression
    ///
    /// For LZ4 or Zstd compression, the `puffin-compression` feature must be enabled.
    pub fn write_blob_with_compression(
        &mut self,
        blob_type: impl Into<String>,
        data: &[u8],
        fields: Vec<i32>,
        snapshot_id: i64,
        sequence_number: i64,
        properties: HashMap<String, String>,
        codec: CompressionCodec,
    ) -> io::Result<usize> {
        let blob_index = self.blobs.len();

        // Compress data if needed
        let (written_data, actual_codec) = match codec {
            CompressionCodec::None => (data.to_vec(), CompressionCodec::None),
            CompressionCodec::Lz4 => {
                let compressed = compression::compress_lz4(data)?;
                (compressed, CompressionCodec::Lz4)
            }
            CompressionCodec::Zstd => {
                let compressed = compression::compress_zstd(data)?;
                (compressed, CompressionCodec::Zstd)
            }
        };

        // Write blob data
        self.writer.write_all(&written_data)?;

        // Create metadata
        let metadata = BlobMetadata {
            blob_type: blob_type.into(),
            fields,
            snapshot_id,
            sequence_number,
            offset: self.current_offset,
            length: written_data.len() as i64,
            compression_codec: actual_codec,
            properties,
        };

        self.current_offset += written_data.len() as i64;
        self.blobs.push(metadata);

        Ok(blob_index)
    }

    /// Finish writing the Puffin file (uncompressed footer)
    pub fn finish(self) -> io::Result<W> {
        self.finish_with_footer_compression(CompressionCodec::None)
    }

    /// Finish writing the Puffin file with optional footer compression
    ///
    /// For LZ4 or Zstd compression, the `puffin-compression` feature must be enabled.
    pub fn finish_with_footer_compression(
        mut self,
        footer_codec: CompressionCodec,
    ) -> io::Result<W> {
        // Create footer
        let footer = PuffinFooter {
            blobs: self.blobs,
            properties: self.properties,
        };

        // Serialize footer to JSON
        let footer_json = serde_json::to_vec(&footer).map_err(|e| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("Failed to serialize footer: {}", e),
            )
        })?;

        // Compress footer if needed
        let (footer_data, flags) = match footer_codec {
            CompressionCodec::None => (footer_json, 0u32),
            CompressionCodec::Lz4 => {
                let compressed = compression::compress_lz4(&footer_json)?;
                (compressed, FLAG_FOOTER_COMPRESSED_LZ4)
            }
            CompressionCodec::Zstd => {
                let compressed = compression::compress_zstd(&footer_json)?;
                (compressed, FLAG_FOOTER_COMPRESSED_ZSTD)
            }
        };

        // Write footer payload
        self.writer.write_all(&footer_data)?;

        // Write footer length
        let footer_length = footer_data.len() as u32;
        self.writer.write_all(&footer_length.to_le_bytes())?;

        // Write flags
        self.writer.write_all(&flags.to_le_bytes())?;

        // Write footer magic
        self.writer.write_all(PUFFIN_MAGIC)?;

        Ok(self.writer)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    #[test]
    fn test_puffin_roundtrip() {
        // Create a Puffin file
        let mut buffer = Cursor::new(Vec::new());

        let mut writer = PuffinWriter::new(&mut buffer).unwrap();
        writer.add_property("created-by", "minio-rs-test");

        let blob_data = b"test deletion vector data";
        writer
            .write_blob(
                BLOB_TYPE_DELETION_VECTOR,
                blob_data,
                vec![1, 2, 3],
                12345,
                1,
                HashMap::new(),
            )
            .unwrap();

        writer.finish().unwrap();

        // Read it back
        buffer.set_position(0);
        let mut reader = PuffinReader::open(buffer).unwrap();

        assert_eq!(reader.blob_count(), 1);

        let metadata = reader.blob_metadata(0).unwrap();
        assert_eq!(metadata.blob_type, BLOB_TYPE_DELETION_VECTOR);
        assert_eq!(metadata.fields, vec![1, 2, 3]);
        assert_eq!(metadata.snapshot_id, 12345);

        let blob = reader.read_blob(0).unwrap();
        assert_eq!(blob.data, blob_data);
    }

    #[test]
    fn test_puffin_multiple_blobs() {
        let mut buffer = Cursor::new(Vec::new());

        let mut writer = PuffinWriter::new(&mut buffer).unwrap();

        writer
            .write_blob(
                BLOB_TYPE_DELETION_VECTOR,
                b"blob1",
                vec![1],
                100,
                1,
                HashMap::new(),
            )
            .unwrap();

        writer
            .write_blob(
                BLOB_TYPE_THETA_SKETCH,
                b"blob2data",
                vec![2],
                100,
                1,
                HashMap::new(),
            )
            .unwrap();

        writer.finish().unwrap();

        buffer.set_position(0);
        let reader = PuffinReader::open(buffer).unwrap();

        assert_eq!(reader.blob_count(), 2);

        let dv_blobs = reader.find_deletion_vectors();
        assert_eq!(dv_blobs.len(), 1);
        assert_eq!(dv_blobs[0].fields, vec![1]);
    }

    #[test]
    fn test_puffin_invalid_magic() {
        let buffer = Cursor::new(b"NOTPUF1somedata".to_vec());
        let result = PuffinReader::open(buffer);
        assert!(result.is_err());
    }

    #[test]
    fn test_compression_availability() {
        let available = compression::is_compression_available();
        #[cfg(feature = "puffin-compression")]
        assert!(available);
        #[cfg(not(feature = "puffin-compression"))]
        assert!(!available);
    }

    #[test]
    #[cfg(feature = "puffin-compression")]
    fn test_puffin_lz4_blob_roundtrip() {
        let mut buffer = Cursor::new(Vec::new());

        let mut writer = PuffinWriter::new(&mut buffer).unwrap();

        let blob_data = b"test data for LZ4 compression roundtrip test";
        writer
            .write_blob_with_compression(
                BLOB_TYPE_DELETION_VECTOR,
                blob_data,
                vec![1],
                100,
                1,
                HashMap::new(),
                CompressionCodec::Lz4,
            )
            .unwrap();

        writer.finish().unwrap();

        buffer.set_position(0);
        let mut reader = PuffinReader::open(buffer).unwrap();

        let metadata = reader.blob_metadata(0).unwrap();
        assert_eq!(metadata.compression_codec, CompressionCodec::Lz4);

        let blob = reader.read_blob(0).unwrap();
        assert_eq!(blob.data, blob_data);
    }

    #[test]
    #[cfg(feature = "puffin-compression")]
    fn test_puffin_zstd_blob_roundtrip() {
        let mut buffer = Cursor::new(Vec::new());

        let mut writer = PuffinWriter::new(&mut buffer).unwrap();

        let blob_data = b"test data for Zstd compression roundtrip test";
        writer
            .write_blob_with_compression(
                BLOB_TYPE_DELETION_VECTOR,
                blob_data,
                vec![1],
                100,
                1,
                HashMap::new(),
                CompressionCodec::Zstd,
            )
            .unwrap();

        writer.finish().unwrap();

        buffer.set_position(0);
        let mut reader = PuffinReader::open(buffer).unwrap();

        let metadata = reader.blob_metadata(0).unwrap();
        assert_eq!(metadata.compression_codec, CompressionCodec::Zstd);

        let blob = reader.read_blob(0).unwrap();
        assert_eq!(blob.data, blob_data);
    }

    #[test]
    #[cfg(feature = "puffin-compression")]
    fn test_puffin_lz4_footer_compression() {
        let mut buffer = Cursor::new(Vec::new());

        let mut writer = PuffinWriter::new(&mut buffer).unwrap();
        writer.add_property("test-property", "test-value");

        let blob_data = b"blob data";
        writer
            .write_blob(
                BLOB_TYPE_DELETION_VECTOR,
                blob_data,
                vec![1],
                100,
                1,
                HashMap::new(),
            )
            .unwrap();

        writer
            .finish_with_footer_compression(CompressionCodec::Lz4)
            .unwrap();

        buffer.set_position(0);
        let mut reader = PuffinReader::open(buffer).unwrap();

        assert_eq!(reader.blob_count(), 1);
        assert_eq!(
            reader.footer().properties.get("test-property"),
            Some(&"test-value".to_string())
        );

        let blob = reader.read_blob(0).unwrap();
        assert_eq!(blob.data, blob_data);
    }

    #[test]
    #[cfg(feature = "puffin-compression")]
    fn test_puffin_zstd_footer_compression() {
        let mut buffer = Cursor::new(Vec::new());

        let mut writer = PuffinWriter::new(&mut buffer).unwrap();
        writer.add_property("test-property", "test-value");

        let blob_data = b"blob data";
        writer
            .write_blob(
                BLOB_TYPE_DELETION_VECTOR,
                blob_data,
                vec![1],
                100,
                1,
                HashMap::new(),
            )
            .unwrap();

        writer
            .finish_with_footer_compression(CompressionCodec::Zstd)
            .unwrap();

        buffer.set_position(0);
        let mut reader = PuffinReader::open(buffer).unwrap();

        assert_eq!(reader.blob_count(), 1);
        assert_eq!(
            reader.footer().properties.get("test-property"),
            Some(&"test-value".to_string())
        );

        let blob = reader.read_blob(0).unwrap();
        assert_eq!(blob.data, blob_data);
    }

    #[test]
    #[cfg(feature = "puffin-compression")]
    fn test_puffin_mixed_compression() {
        let mut buffer = Cursor::new(Vec::new());

        let mut writer = PuffinWriter::new(&mut buffer).unwrap();

        // Write blobs with different compression codecs
        let blob1_data = b"uncompressed blob data";
        writer
            .write_blob_with_compression(
                BLOB_TYPE_DELETION_VECTOR,
                blob1_data,
                vec![1],
                100,
                1,
                HashMap::new(),
                CompressionCodec::None,
            )
            .unwrap();

        let blob2_data = b"LZ4 compressed blob data for testing";
        writer
            .write_blob_with_compression(
                BLOB_TYPE_DELETION_VECTOR,
                blob2_data,
                vec![2],
                100,
                2,
                HashMap::new(),
                CompressionCodec::Lz4,
            )
            .unwrap();

        let blob3_data = b"Zstd compressed blob data for testing";
        writer
            .write_blob_with_compression(
                BLOB_TYPE_THETA_SKETCH,
                blob3_data,
                vec![3],
                100,
                3,
                HashMap::new(),
                CompressionCodec::Zstd,
            )
            .unwrap();

        writer
            .finish_with_footer_compression(CompressionCodec::Zstd)
            .unwrap();

        buffer.set_position(0);
        let mut reader = PuffinReader::open(buffer).unwrap();

        assert_eq!(reader.blob_count(), 3);

        // Verify each blob is decompressed correctly
        let blob1 = reader.read_blob(0).unwrap();
        assert_eq!(blob1.metadata.compression_codec, CompressionCodec::None);
        assert_eq!(blob1.data, blob1_data);

        let blob2 = reader.read_blob(1).unwrap();
        assert_eq!(blob2.metadata.compression_codec, CompressionCodec::Lz4);
        assert_eq!(blob2.data, blob2_data);

        let blob3 = reader.read_blob(2).unwrap();
        assert_eq!(blob3.metadata.compression_codec, CompressionCodec::Zstd);
        assert_eq!(blob3.data, blob3_data);
    }

    #[test]
    #[cfg(feature = "puffin-compression")]
    fn test_compression_reduces_size() {
        // Generate highly compressible data (repeated pattern)
        let compressible_data: Vec<u8> = (0..1000).flat_map(|_| b"AAAA".to_vec()).collect();

        // Test LZ4
        let lz4_compressed = compression::compress_lz4(&compressible_data).unwrap();
        assert!(
            lz4_compressed.len() < compressible_data.len(),
            "LZ4 should reduce size for compressible data"
        );

        let lz4_decompressed = compression::decompress_lz4(&lz4_compressed).unwrap();
        assert_eq!(lz4_decompressed, compressible_data);

        // Test Zstd
        let zstd_compressed = compression::compress_zstd(&compressible_data).unwrap();
        assert!(
            zstd_compressed.len() < compressible_data.len(),
            "Zstd should reduce size for compressible data"
        );

        let zstd_decompressed = compression::decompress_zstd(&zstd_compressed).unwrap();
        assert_eq!(zstd_decompressed, compressible_data);
    }

    #[test]
    #[cfg(not(feature = "puffin-compression"))]
    fn test_compression_disabled() {
        let data = b"test data";

        // Compression should fail when feature is not enabled
        assert!(compression::compress_lz4(data).is_err());
        assert!(compression::compress_zstd(data).is_err());
        assert!(compression::decompress_lz4(data).is_err());
        assert!(compression::decompress_zstd(data).is_err());
    }
}
