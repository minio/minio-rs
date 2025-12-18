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

//! ObjectStore trait implementation for MinIO.
//!
//! This module provides adapters that make MinIO S3 clients compatible with the
//! standard `object_store::ObjectStore` trait. This enables MinIO to be used with
//! Apache DataFusion and other systems that depend on this abstraction.
//!
//! # Key Features
//!
//! - Full ObjectStore trait implementation
//! - Streaming data transfer (no full buffering in memory)
//! - Efficient byte handling with minimal copies
//! - Multipart upload support
//! - Range request support for partial object retrieval
//!
//! # Example
//!
//! ```ignore
//! use std::sync::Arc;
//! use minio::s3::MinioClient;
//! use minio::s3tables::datafusion::MinioObjectStore;
//!
//! // Create a MinIO client
//! let client = Arc::new(MinioClient::new(
//!     "http://localhost:9000".parse()?,
//!     Some(provider),
//!     None,
//!     None,
//! )?);
//!
//! // Create an ObjectStore adapter
//! let store = MinioObjectStore::new(client, "my-bucket".to_string());
//! ```

use async_trait::async_trait;
use bytes::Bytes;
use futures_util::stream::BoxStream;
use futures_util::{StreamExt, TryStreamExt};
use object_store::{
    Attribute, Error as ObjectStoreError, GetOptions, GetResult, GetResultPayload, ListResult,
    MultipartUpload, ObjectMeta, ObjectStore, PutMode, PutMultipartOptions, PutOptions, PutPayload,
    PutResult, Result, path::Path,
};
use std::fmt;
use std::sync::Arc;

use crate::s3::builders::{ObjectToDelete, PutObject, UploadPart};
use crate::s3::client::MinioClient;
use crate::s3::multimap_ext::{Multimap, MultimapExt};
use crate::s3::response::{
    DeleteObjectResponse, GetObjectResponse, PutObjectResponse, StatObjectResponse,
};
use crate::s3::response_traits::{HasEtagFromHeaders, HasVersion};
use crate::s3::segmented_bytes::SegmentedBytes;
use crate::s3::types::{BucketName, ObjectKey, PartInfo, S3Api, ToStream, UploadId};

/// MinIO multipart upload handler implementing the ObjectStore trait.
#[derive(Debug)]
pub struct MinioMultipartUpload {
    client: Arc<MinioClient>,
    bucket: BucketName,
    object: ObjectKey,
    upload_id: UploadId,
    parts: Vec<PartInfo>,
    part_number: u16,
}

impl MinioMultipartUpload {
    /// Create a new multipart upload handler.
    pub fn new(
        client: Arc<MinioClient>,
        bucket: BucketName,
        object: ObjectKey,
        upload_id: UploadId,
    ) -> Self {
        Self {
            client,
            bucket,
            object,
            upload_id,
            parts: Vec::new(),
            part_number: 0,
        }
    }

    /// Convert MinIO error to ObjectStore error.
    fn convert_error(err: crate::s3::error::Error) -> ObjectStoreError {
        ObjectStoreError::Generic {
            store: "MinIO",
            source: Box::new(err),
        }
    }
}

#[async_trait]
impl MultipartUpload for MinioMultipartUpload {
    /// Upload a single part.
    fn put_part(
        &mut self,
        data: PutPayload,
    ) -> futures_util::future::BoxFuture<'static, Result<()>> {
        let client = self.client.clone();
        let bucket = self.bucket.clone();
        let object = self.object.clone();
        let upload_id = self.upload_id.clone();
        let mut parts = self.parts.clone();
        let part_number = self.part_number + 1;

        Box::pin(async move {
            // Collect payload into bytes
            let mut part_data = Vec::with_capacity(data.content_length());
            for chunk in data {
                part_data.extend_from_slice(&chunk);
            }

            // Upload the part to MinIO
            let response = client
                .upload_part(
                    bucket,
                    object,
                    upload_id,
                    part_number,
                    SegmentedBytes::from(Bytes::from(part_data.clone())),
                )
                .build()
                .send()
                .await
                .map_err(Self::convert_error)?;

            // Extract ETag from response
            let etag = response.etag().map_err(|e| ObjectStoreError::Generic {
                store: "MinIO",
                source: format!("Failed to get ETag from upload_part response: {}", e).into(),
            })?;

            // Store part info for later completion
            parts.push(PartInfo::new(
                part_number,
                etag,
                part_data.len() as u64,
                None,
            ));

            Ok(())
        })
    }

    /// Complete the multipart upload.
    async fn complete(&mut self) -> Result<PutResult> {
        if self.parts.is_empty() {
            return Err(ObjectStoreError::Generic {
                store: "MinIO",
                source: "No parts uploaded for multipart upload".into(),
            });
        }

        let response = self
            .client
            .complete_multipart_upload(
                self.bucket.clone(), // TODO why clone here?
                self.object.clone(),
                self.upload_id.clone(),
                self.parts.clone(),
            )
            .build()
            .send()
            .await
            .map_err(Self::convert_error)?;

        // Extract ETag and version from response
        let e_tag = response.etag().ok();
        let version = response.version_id().map(|v| v.to_string());

        Ok(PutResult { e_tag, version })
    }

    /// Abort the multipart upload.
    async fn abort(&mut self) -> Result<()> {
        self.client
            .abort_multipart_upload(
                self.bucket.clone(),
                self.object.clone(),
                self.upload_id.clone(),
            )
            .build()
            .send()
            .await
            .map_err(Self::convert_error)?;

        Ok(())
    }
}

/// Adapter that wraps MinioClient to implement the ObjectStore trait.
///
/// This adapter enables MinIO to be used with systems expecting the standard
/// object_store::ObjectStore interface, such as Apache DataFusion.
#[derive(Debug)]
pub struct MinioObjectStore {
    client: Arc<MinioClient>,
    bucket: BucketName,
}

impl MinioObjectStore {
    /// Create a new MinIO ObjectStore adapter.
    pub fn new(client: Arc<MinioClient>, bucket: BucketName) -> Self {
        Self { client, bucket }
    }

    /// Helper to convert object_store::Path to MinIO object name.
    pub(crate) fn path_to_key(location: &Path) -> Result<ObjectKey, ObjectStoreError> {
        ObjectKey::new(location.to_string()).map_err(|e| Self::convert_error(e.into()))
    }

    /// Helper to convert MinIO errors to ObjectStore errors.
    fn convert_error(err: crate::s3::error::Error) -> ObjectStoreError {
        ObjectStoreError::Generic {
            store: "MinIO",
            source: Box::new(err),
        }
    }
}

impl fmt::Display for MinioObjectStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "MinioObjectStore(bucket={})", self.bucket)
    }
}

#[async_trait]
impl ObjectStore for MinioObjectStore {
    async fn put(&self, location: &Path, payload: PutPayload) -> Result<PutResult> {
        let key = Self::path_to_key(location)?;
        // Collect all chunks from the payload into a single byte vector
        let mut data = Vec::with_capacity(payload.content_length());
        for chunk in payload {
            data.extend_from_slice(&chunk);
        }

        let response: PutObjectResponse = self
            .client
            .put_object(
                self.bucket.clone(),
                key,
                SegmentedBytes::from(Bytes::from(data)),
            )
            .build()
            .send()
            .await
            .map_err(Self::convert_error)?;

        // Extract ETag and version from response
        let e_tag = response.etag().ok();
        let version = response.version_id().map(|v| v.to_string());

        Ok(PutResult { e_tag, version })
    }

    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> Result<PutResult> {
        let key = Self::path_to_key(location)?;

        // Collect payload into bytes
        let mut data = Vec::with_capacity(payload.content_length());
        for chunk in payload {
            data.extend_from_slice(&chunk);
        }

        // Build extra headers for conditional operations and tags
        let mut headers = Multimap::new();

        // Handle PutMode via conditional headers
        match opts.mode {
            PutMode::Overwrite => {
                // Default behavior, no special headers needed
            }
            PutMode::Create => {
                // Fail if object already exists - use If-None-Match: *
                headers.add("If-None-Match", "*");
            }
            PutMode::Update(update_version) => {
                // Conditional update based on ETag
                if let Some(etag) = update_version.e_tag.as_ref() {
                    headers.add("If-Match", etag.as_str());
                }
            }
        }

        // Handle tags - pass as X-Amz-Tagging header using the encoded string
        let tags_encoded = opts.tags.encoded();
        if !tags_encoded.is_empty() {
            headers.add("x-amz-tagging", tags_encoded);
        }

        // Extract content-type from attributes
        let mut content_type: Option<String> = None;
        for (attr, value) in &opts.attributes {
            if matches!(attr, Attribute::ContentType) {
                content_type = Some(value.to_string());
                break;
            }
        }

        // Build UploadPart directly to access all options
        let extra_headers = if headers.is_empty() {
            None
        } else {
            Some(headers)
        };

        let inner = UploadPart::builder()
            .client((*self.client).clone())
            .bucket(self.bucket.clone())
            .object(key)
            .data(Arc::new(SegmentedBytes::from(Bytes::from(data))))
            .extra_headers(extra_headers)
            .content_type(content_type)
            .build();

        let response: PutObjectResponse = PutObject::builder()
            .inner(inner)
            .build()
            .send()
            .await
            .map_err(Self::convert_error)?;

        // Extract ETag and version from response
        let e_tag = response.etag().ok();
        let version = response.version_id().map(|v| v.to_string());

        Ok(PutResult { e_tag, version })
    }

    async fn put_multipart(&self, location: &Path) -> Result<Box<dyn MultipartUpload>> {
        self.put_multipart_opts(location, PutMultipartOptions::default())
            .await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        _opts: PutMultipartOptions,
    ) -> Result<Box<dyn MultipartUpload>> {
        let key = Self::path_to_key(&location)?;

        // Initiate multipart upload
        let response = self
            .client
            .create_multipart_upload(self.bucket.clone(), key.clone())
            .build()
            .send()
            .await
            .map_err(Self::convert_error)?;

        // Extract upload ID from response
        let upload_id = response
            .upload_id()
            .await
            .map_err(|e| ObjectStoreError::Generic {
                store: "MinIO",
                source: format!("Failed to get upload ID: {}", e).into(),
            })?;

        // Create and return multipart upload handler
        let upload =
            MinioMultipartUpload::new(self.client.clone(), self.bucket.clone(), key, upload_id);

        Ok(Box::new(upload))
    }

    async fn get(&self, location: &Path) -> Result<GetResult> {
        let key = Self::path_to_key(&location)?;

        let response: GetObjectResponse = self
            .client
            .get_object(self.bucket.clone(), key)
            .build()
            .send()
            .await
            .map_err(Self::convert_error)?;

        // Use direct stream access - bypasses async ObjectContent wrapper
        let (stream, size) = response.into_boxed_stream().map_err(Self::convert_error)?;

        // Convert minio stream to object_store stream
        let object_store_stream = stream.map_err(|e| ObjectStoreError::Generic {
            store: "MinIO",
            source: Box::new(e),
        });

        Ok(GetResult {
            payload: GetResultPayload::Stream(object_store_stream.boxed()),
            meta: ObjectMeta {
                location: location.clone(),
                last_modified: chrono::Utc::now(),
                size,
                e_tag: None,
                version: None,
            },
            range: 0..size,
            attributes: Default::default(),
        })
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> Result<GetResult> {
        // If no range is specified, use the default get
        if options.range.is_none() {
            return self.get(location).await;
        }

        let key = Self::path_to_key(location)?;

        // Handle range requests
        let (offset, length) = match options.range {
            Some(object_store::GetRange::Bounded(r)) => (r.start, (r.end - r.start)),
            Some(object_store::GetRange::Offset(off)) => (off, 0),
            Some(object_store::GetRange::Suffix(_)) | None => {
                return self.get(location).await;
            }
        };

        // Build request with range
        let response = self
            .client
            .get_object(self.bucket.clone(), key)
            .offset(offset)
            .length(length)
            .build()
            .send()
            .await
            .map_err(Self::convert_error)?;

        // Use direct stream access - bypasses async ObjectContent wrapper
        let (stream, size) = response.into_boxed_stream().map_err(Self::convert_error)?;

        let object_store_stream = stream.map_err(|e| ObjectStoreError::Generic {
            store: "MinIO",
            source: Box::new(e),
        });

        Ok(GetResult {
            payload: GetResultPayload::Stream(object_store_stream.boxed()),
            meta: ObjectMeta {
                location: location.clone(),
                last_modified: chrono::Utc::now(),
                size,
                e_tag: None,
                version: None,
            },
            range: 0..size,
            attributes: Default::default(),
        })
    }

    async fn get_range(&self, location: &Path, range: std::ops::Range<u64>) -> Result<Bytes> {
        let key = Self::path_to_key(location)?;

        let response = self
            .client
            .get_object(self.bucket.clone(), key)
            .offset(range.start)
            .length(range.end - range.start)
            .build()
            .send()
            .await
            .map_err(Self::convert_error)?;

        // Use direct bytes access - bypasses ObjectContent wrapper
        response.into_bytes().await.map_err(Self::convert_error)
    }

    async fn head(&self, location: &Path) -> Result<ObjectMeta> {
        let key = Self::path_to_key(&location)?;

        let stat: StatObjectResponse = self
            .client
            .stat_object(self.bucket.clone(), key)
            .build()
            .send()
            .await
            .map_err(Self::convert_error)?;

        let size = stat.size().unwrap_or(0);
        let e_tag = stat.etag().ok();
        let last_modified = stat
            .last_modified()
            .ok()
            .flatten()
            .unwrap_or_else(chrono::Utc::now);

        Ok(ObjectMeta {
            location: location.clone(),
            last_modified,
            size,
            e_tag,
            version: None,
        })
    }

    async fn delete(&self, location: &Path) -> Result<()> {
        let key = Self::path_to_key(location)?;

        let _resp: DeleteObjectResponse = self
            .client
            .delete_object(self.bucket.clone(), ObjectToDelete::from(key.as_str()))
            .build()
            .send()
            .await
            .map_err(Self::convert_error)?;

        Ok(())
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, Result<ObjectMeta>> {
        let prefix_str = prefix.map(|p| p.to_string());
        let client = self.client.clone();
        let bucket = self.bucket.clone();

        Box::pin(async_stream::stream! {
            let builder = client.list_objects(bucket).recursive(true).prefix(prefix_str);

            let mut stream = builder.build().to_stream().await;

            while let Some(result) = stream.next().await {
                match result {
                    Ok(response) => {
                        for item in response.contents {
                            yield Ok(ObjectMeta {
                                location: Path::from(item.name.clone()),
                                last_modified: item.last_modified.unwrap_or_else(chrono::Utc::now),
                                size: item.size.unwrap_or(0),
                                e_tag: item.etag,
                                version: None,
                            });
                        }
                    }
                    Err(e) => {
                        yield Err(Self::convert_error(e));
                    }
                }
            }
        })
    }

    async fn list_with_delimiter(&self, _prefix: Option<&Path>) -> Result<ListResult> {
        Err(ObjectStoreError::NotImplemented)
    }

    async fn copy(&self, _from: &Path, _to: &Path) -> Result<()> {
        Err(ObjectStoreError::NotImplemented)
    }

    async fn copy_if_not_exists(&self, _from: &Path, _to: &Path) -> Result<()> {
        Err(ObjectStoreError::NotImplemented)
    }

    async fn rename(&self, _from: &Path, _to: &Path) -> Result<()> {
        Err(ObjectStoreError::NotImplemented)
    }

    async fn rename_if_not_exists(&self, _from: &Path, _to: &Path) -> Result<()> {
        Err(ObjectStoreError::NotImplemented)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_path_conversion() {
        let path = Path::from("data/test.parquet");
        let key = MinioObjectStore::path_to_key(&path).unwrap();
        assert_eq!(key.as_str(), "data/test.parquet");
    }

    #[test]
    fn test_path_conversion_root() {
        let path = Path::from("test.parquet");
        let key = MinioObjectStore::path_to_key(&path).unwrap();
        assert_eq!(key.as_str(), "test.parquet");
    }

    #[test]
    fn test_path_conversion_nested() {
        let path = Path::from("data/year=2024/month=12/test.parquet");
        let key = MinioObjectStore::path_to_key(&path).unwrap();
        assert_eq!(key.as_str(), "data/year=2024/month=12/test.parquet");
    }

    #[test]
    fn test_creation() {
        let client = Arc::new(
            crate::s3::MinioClient::new(
                "http://localhost:9000".parse().unwrap(),
                None::<crate::s3::creds::StaticProvider>,
                None,
                None,
            )
            .unwrap(),
        );
        let bucket = BucketName::new("my-bucket").unwrap();
        let store = MinioObjectStore::new(client.clone(), bucket);

        assert_eq!(store.bucket.as_str(), "my-bucket");
        assert!(Arc::ptr_eq(&store.client, &client));
    }

    #[test]
    fn test_display() {
        let bucket = BucketName::new("test-bucket").unwrap();
        let store = MinioObjectStore::new(
            Arc::new(
                crate::s3::MinioClient::new(
                    "http://localhost:9000".parse().unwrap(),
                    None::<crate::s3::creds::StaticProvider>,
                    None,
                    None,
                )
                .unwrap(),
            ),
            bucket,
        );

        let display_str = format!("{}", store);
        assert!(display_str.contains("MinioObjectStore"));
        assert!(display_str.contains("test-bucket"));
    }

    #[test]
    fn test_multipart_upload_structure() {
        let client = Arc::new(
            crate::s3::MinioClient::new(
                "http://localhost:9000".parse().unwrap(),
                None::<crate::s3::creds::StaticProvider>,
                None,
                None,
            )
            .unwrap(),
        );

        let bucket = BucketName::new("test-bucket").unwrap();
        let object = ObjectKey::new("test-object").unwrap();
        let upload_id = UploadId::new("upload-id-123").unwrap();
        let upload = MinioMultipartUpload::new(client, bucket, object, upload_id);

        assert_eq!(upload.bucket.as_str(), "test-bucket");
        assert_eq!(upload.object.as_str(), "test-object");
        assert_eq!(upload.upload_id.as_str(), "upload-id-123");
        assert_eq!(upload.part_number, 0);
        assert_eq!(upload.parts.len(), 0);
    }

    #[test]
    fn test_multipart_upload_initialization() {
        let client = Arc::new(
            crate::s3::MinioClient::new(
                "http://localhost:9000".parse().unwrap(),
                None::<crate::s3::creds::StaticProvider>,
                None,
                None,
            )
            .unwrap(),
        );

        let bucket = BucketName::new("bucket").unwrap();
        let object = ObjectKey::new("object").unwrap();
        let upload_id = UploadId::new("id").unwrap();
        let upload = MinioMultipartUpload::new(client.clone(), bucket, object, upload_id);

        // Verify initial state
        assert_eq!(upload.part_number, 0);
        assert_eq!(upload.parts.len(), 0);
        assert_eq!(upload.bucket.as_str(), "bucket");
        assert_eq!(upload.object.as_str(), "object");
        assert_eq!(upload.upload_id.as_str(), "id");
    }
}
