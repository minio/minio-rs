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

//! Extended ObjectStore adapter with query pushdown context.
//!
//! This module provides `PushdownMinioObjectStore`, which wraps the base `MinioObjectStore`
//! and adds support for carrying filter context information through the query execution pipeline.
//! This enables integration with DataFusion optimizer rules that can inject filters before scan execution.

use async_trait::async_trait;
use bytes::Bytes;
use futures_util::stream::BoxStream;
use futures_util::{StreamExt, TryStreamExt};
use object_store::{
    GetOptions, GetResult, GetResultPayload, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMode, PutMultipartOptions, PutOptions, PutPayload, PutResult, Result, path::Path,
};
use std::fmt;
use std::sync::Arc;

use crate::s3::builders::{ObjectToDelete, UploadPart};
use crate::s3::client::MinioClient;
use crate::s3::multimap_ext::{Multimap, MultimapExt};
use crate::s3::response::CreateMultipartUploadResponse;
use crate::s3::response_traits::{HasEtagFromHeaders, HasVersion};
use crate::s3::segmented_bytes::SegmentedBytes;
use crate::s3::types::{BucketName, ObjectKey, S3Api, ToStream};

// Re-export the base types from object_store module
pub use super::object_store::{MinioMultipartUpload, MinioObjectStore};

/// Extended MinIO ObjectStore adapter with query pushdown support.
///
/// This adapter extends `MinioObjectStore` to optionally track filter information
/// that can be passed to `plan_table_scan()` for server-side filtering.
///
/// When filters are not set, it behaves identically to the base `MinioObjectStore`.
/// When filters are set, they can be used by query planners to optimize data retrieval
/// from MinIO S3 Tables.
///
/// # Example
///
/// ```ignore
/// use std::sync::Arc;
/// use minio::s3::client::MinioClient;
/// use minio::s3tables::datafusion::PushdownMinioObjectStore;
///
/// let client = Arc::new(MinioClient::new(...)?);
/// let store = PushdownMinioObjectStore::new(client, "my-bucket".to_string());
/// let store_with_filter = store.with_filter(serde_json::json!({
///     "op": ">",
///     "term": "age",
///     "value": 18
/// }));
/// ```
#[derive(Debug)]
pub struct PushdownMinioObjectStore {
    client: Arc<MinioClient>,
    bucket: BucketName,
    /// Optional filter context for pushdown (set via query optimizer)
    /// In production, this would be populated by a DataFusion optimizer rule that
    /// converts logical plan predicates to Iceberg filters before scan execution.
    pub filter_context: Option<serde_json::Value>,
}

impl PushdownMinioObjectStore {
    /// Create a new MinIO ObjectStore adapter with pushdown support.
    pub fn new(client: Arc<MinioClient>, bucket: BucketName) -> Self {
        Self {
            client,
            bucket,
            filter_context: None,
        }
    }

    /// Set filter context for the next query.
    ///
    /// In production, this would be managed by a DataFusion optimizer rule that
    /// converts logical plan predicates to Iceberg filters before scan execution.
    ///
    /// # Arguments
    ///
    /// * `filter` - Iceberg filter JSON to be used for query optimization
    ///
    /// # Returns
    ///
    /// Self for method chaining
    pub fn with_filter(mut self, filter: serde_json::Value) -> Self {
        self.filter_context = Some(filter);
        self
    }

    /// Get the current filter context, if any.
    pub fn get_filter_context(&self) -> Option<&serde_json::Value> {
        self.filter_context.as_ref()
    }

    /// Helper to convert MinIO errors to ObjectStore errors.
    fn convert_error(err: crate::s3::error::Error) -> object_store::Error {
        object_store::Error::Generic {
            store: "MinIO",
            source: Box::new(err),
        }
    }
}

impl fmt::Display for PushdownMinioObjectStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "PushdownMinioObjectStore(bucket={})", self.bucket)
    }
}

#[async_trait]
impl ObjectStore for PushdownMinioObjectStore {
    async fn put(&self, location: &Path, payload: PutPayload) -> Result<PutResult> {
        let key: ObjectKey = MinioObjectStore::path_to_key(location)?;
        let mut data = Vec::with_capacity(payload.content_length());
        for chunk in payload {
            data.extend_from_slice(&chunk);
        }

        let response = self
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
        let key: ObjectKey = MinioObjectStore::path_to_key(location)?;

        let mut data = Vec::with_capacity(payload.content_length());
        for chunk in payload {
            data.extend_from_slice(&chunk);
        }

        let mut headers = Multimap::new();

        match opts.mode {
            PutMode::Overwrite => {}
            PutMode::Create => {
                headers.add("If-None-Match", "*");
            }
            PutMode::Update(update_version) => {
                if let Some(etag) = update_version.e_tag.as_ref() {
                    headers.add("If-Match", etag.as_str());
                }
            }
        }

        let tags_encoded = opts.tags.encoded();
        if !tags_encoded.is_empty() {
            headers.add("x-amz-tagging", tags_encoded);
        }

        let mut content_type: Option<String> = None;
        for (attr, value) in &opts.attributes {
            if matches!(attr, object_store::Attribute::ContentType) {
                content_type = Some(value.to_string());
                break;
            }
        }

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

        let response = crate::s3::builders::PutObject::builder()
            .inner(inner)
            .build()
            .send()
            .await
            .map_err(Self::convert_error)?;

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
        let key: ObjectKey = MinioObjectStore::path_to_key(location)?;

        // Initiate multipart upload
        let response: CreateMultipartUploadResponse = self
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
            .map_err(|e| object_store::Error::Generic {
                store: "MinIO",
                source: format!("Failed to get upload ID: {}", e).into(),
            })?;

        // Create and return multipart upload handler
        let upload =
            MinioMultipartUpload::new(self.client.clone(), self.bucket.clone(), key, upload_id);

        Ok(Box::new(upload))
    }

    async fn get(&self, location: &Path) -> Result<GetResult> {
        let key: ObjectKey = MinioObjectStore::path_to_key(location)?;

        let response = self
            .client
            .get_object(self.bucket.clone(), key)
            .build()
            .send()
            .await
            .map_err(Self::convert_error)?;

        let (stream, size) = response.into_boxed_stream().map_err(Self::convert_error)?;

        let object_store_stream = stream.map_err(|e| object_store::Error::Generic {
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
        if options.range.is_none() {
            return self.get(location).await;
        }

        let key: ObjectKey = MinioObjectStore::path_to_key(location)?;

        let (offset, length) = match options.range {
            Some(object_store::GetRange::Bounded(r)) => (r.start, (r.end - r.start)),
            Some(object_store::GetRange::Offset(off)) => (off, 0),
            Some(object_store::GetRange::Suffix(_)) | None => {
                return self.get(location).await;
            }
        };

        let response = self
            .client
            .get_object(self.bucket.clone(), key)
            .offset(offset)
            .length(length)
            .build()
            .send()
            .await
            .map_err(Self::convert_error)?;

        let (stream, size) = response.into_boxed_stream().map_err(Self::convert_error)?;

        let object_store_stream = stream.map_err(|e| object_store::Error::Generic {
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
        let key = MinioObjectStore::path_to_key(location)?;

        let response = self
            .client
            .get_object(self.bucket.clone(), key)
            .offset(range.start)
            .length(range.end - range.start)
            .build()
            .send()
            .await
            .map_err(Self::convert_error)?;

        response.into_bytes().await.map_err(Self::convert_error)
    }

    async fn head(&self, location: &Path) -> Result<ObjectMeta> {
        let key = MinioObjectStore::path_to_key(location)?;

        let stat = self
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
        let key = MinioObjectStore::path_to_key(location)?;

        self.client
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
        Err(object_store::Error::NotImplemented)
    }

    async fn copy(&self, _from: &Path, _to: &Path) -> Result<()> {
        Err(object_store::Error::NotImplemented)
    }

    async fn copy_if_not_exists(&self, _from: &Path, _to: &Path) -> Result<()> {
        Err(object_store::Error::NotImplemented)
    }

    async fn rename(&self, _from: &Path, _to: &Path) -> Result<()> {
        Err(object_store::Error::NotImplemented)
    }

    async fn rename_if_not_exists(&self, _from: &Path, _to: &Path) -> Result<()> {
        Err(object_store::Error::NotImplemented)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::s3::types::UploadId;

    #[test]
    fn test_with_filter() {
        let client = Arc::new(
            MinioClient::new(
                "http://localhost:9000".parse().unwrap(),
                None::<crate::s3::creds::StaticProvider>,
                None,
                None,
            )
            .unwrap(),
        );
        let bucket_name = BucketName::new("test-bucket").unwrap();
        let store = PushdownMinioObjectStore::new(client, bucket_name);

        assert!(store.filter_context.is_none());

        let filter = serde_json::json!({
            "type": "and",
            "left": { "type": "unbound", "op": ">=", "term": "age" },
            "right": { "type": "literal", "value": 18 }
        });

        let store_with_filter = store.with_filter(filter.clone());
        assert!(store_with_filter.filter_context.is_some());
        assert_eq!(store_with_filter.filter_context, Some(filter));
    }

    #[test]
    fn test_creation() {
        let client = Arc::new(
            MinioClient::new(
                "http://localhost:9000".parse().unwrap(),
                None::<crate::s3::creds::StaticProvider>,
                None,
                None,
            )
            .unwrap(),
        );
        let bucket_name = BucketName::new("test-bucket").unwrap();
        let store = PushdownMinioObjectStore::new(client, bucket_name);

        assert_eq!(store.bucket.as_str(), "test-bucket");
        assert!(store.filter_context.is_none());
    }

    #[test]
    fn test_display() {
        let client = Arc::new(
            MinioClient::new(
                "http://localhost:9000".parse().unwrap(),
                None::<crate::s3::creds::StaticProvider>,
                None,
                None,
            )
            .unwrap(),
        );
        let bucket_name = BucketName::new("test-bucket").unwrap();
        let store = PushdownMinioObjectStore::new(client, bucket_name);

        let display_str = format!("{}", store);
        assert!(display_str.contains("PushdownMinioObjectStore"));
        assert!(display_str.contains("test-bucket"));
    }

    #[test]
    fn test_get_filter_context_none() {
        let client = Arc::new(
            MinioClient::new(
                "http://localhost:9000".parse().unwrap(),
                None::<crate::s3::creds::StaticProvider>,
                None,
                None,
            )
            .unwrap(),
        );
        let bucket_name = BucketName::new("test-bucket").unwrap();
        let store = PushdownMinioObjectStore::new(client, bucket_name);

        assert!(store.get_filter_context().is_none());
    }

    #[test]
    fn test_get_filter_context_some() {
        let client = Arc::new(
            MinioClient::new(
                "http://localhost:9000".parse().unwrap(),
                None::<crate::s3::creds::StaticProvider>,
                None,
                None,
            )
            .unwrap(),
        );
        let bucket_name = BucketName::new("test-bucket").unwrap();
        let store = PushdownMinioObjectStore::new(client, bucket_name);

        let filter = serde_json::json!({
            "type": "comparison",
            "column": "age",
            "op": ">",
            "value": 18
        });

        let store_with_filter = store.with_filter(filter.clone());
        assert!(store_with_filter.get_filter_context().is_some());
        assert_eq!(store_with_filter.get_filter_context().unwrap(), &filter);
    }

    #[test]
    fn test_chained_with_filter() {
        let client = Arc::new(
            MinioClient::new(
                "http://localhost:9000".parse().unwrap(),
                None::<crate::s3::creds::StaticProvider>,
                None,
                None,
            )
            .unwrap(),
        );
        let bucket_name = BucketName::new("test-bucket").unwrap();
        let store = PushdownMinioObjectStore::new(client, bucket_name);

        let filter1 = serde_json::json!({"type": "comparison", "op": "="});
        let filter2 = serde_json::json!({"type": "comparison", "op": ">"});

        let store1 = store.with_filter(filter1.clone());
        assert_eq!(store1.filter_context, Some(filter1));

        // Overwrite filter
        let store2 = store1.with_filter(filter2.clone());
        assert_eq!(store2.filter_context, Some(filter2));
    }

    #[test]
    fn test_multipart_upload_creation() {
        let client = Arc::new(
            MinioClient::new(
                "http://localhost:9000".parse().unwrap(),
                None::<crate::s3::creds::StaticProvider>,
                None,
                None,
            )
            .unwrap(),
        );

        let bucket_name = BucketName::new("test-bucket").unwrap();
        let object_key = ObjectKey::new("test-object").unwrap();
        let upload_id = UploadId::new("upload-id-123").unwrap();

        // Just verify it can be created without panicking
        let _upload = MinioMultipartUpload::new(client, bucket_name, object_key, upload_id);
        // The struct is successfully created if we reach here
    }
}
