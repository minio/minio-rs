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

use crate::s3::Client;
use crate::s3::client::MAX_MULTIPART_COUNT;
use crate::s3::error::{Error, ValidationErr};
use crate::s3::header_constants::*;
use crate::s3::multimap_ext::{Multimap, MultimapExt};
use crate::s3::response::{DeleteError, DeleteObjectResponse, DeleteObjectsResponse};
use crate::s3::types::{ListEntry, S3Api, S3Request, ToS3Request, ToStream};
use crate::s3::utils::{check_bucket_name, check_object_name, insert, md5sum_hash};
use async_trait::async_trait;
use bytes::Bytes;
use futures_util::stream::iter;
use futures_util::{Stream, StreamExt, stream as futures_stream};
use http::Method;
use std::pin::Pin;
// region: object-to-delete

pub trait ValidKey: Into<String> {}
impl ValidKey for String {}
impl ValidKey for &str {}
impl ValidKey for &String {}

/// Specify an object to be deleted. The object can be specified by key or by
/// key and version_id via the From trait.
#[derive(Debug, Clone, Default)]
pub struct ObjectToDelete {
    key: String,
    version_id: Option<String>,
}

/// A key can be converted into a DeleteObject. The version_id is set to None.
impl<K: ValidKey> From<K> for ObjectToDelete {
    fn from(key: K) -> Self {
        Self {
            key: key.into(),
            version_id: None,
        }
    }
}

/// A tuple of key and version_id can be converted into a DeleteObject.
impl<K: ValidKey> From<(K, &str)> for ObjectToDelete {
    fn from((key, version_id): (K, &str)) -> Self {
        Self {
            key: key.into(),
            version_id: Some(version_id.to_string()),
        }
    }
}

/// A tuple of key and option version_id can be converted into a DeleteObject.
impl<K: ValidKey> From<(K, Option<&str>)> for ObjectToDelete {
    fn from((key, version_id): (K, Option<&str>)) -> Self {
        Self {
            key: key.into(),
            version_id: version_id.map(|v| v.to_string()),
        }
    }
}

impl From<ListEntry> for ObjectToDelete {
    fn from(entry: ListEntry) -> Self {
        Self {
            key: entry.name,
            version_id: entry.version_id,
        }
    }
}

impl From<DeleteError> for ObjectToDelete {
    fn from(entry: DeleteError) -> Self {
        Self {
            key: entry.object_name,
            version_id: entry.version_id,
        }
    }
}

// endregion: object-to-delete

// region: delete-object

/// Argument builder for the [`RemoveObject`](https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeleteObject.html) S3 API operation.
///
/// This struct constructs the parameters required for the [`Client::remove_object`](crate::s3::client::Client::delete_object) method.
#[derive(Debug, Clone, Default)]
pub struct DeleteObject {
    client: Client,

    extra_headers: Option<Multimap>,
    extra_query_params: Option<Multimap>,
    region: Option<String>,
    bucket: String,

    object: ObjectToDelete,
    bypass_governance_mode: bool,
}

impl DeleteObject {
    pub fn new(client: Client, bucket: String, object: impl Into<ObjectToDelete>) -> Self {
        Self {
            client,
            bucket,
            object: object.into(),
            ..Default::default()
        }
    }

    pub fn bypass_governance_mode(mut self, bypass_governance_mode: bool) -> Self {
        self.bypass_governance_mode = bypass_governance_mode;
        self
    }

    pub fn extra_headers(mut self, extra_headers: Option<Multimap>) -> Self {
        self.extra_headers = extra_headers;
        self
    }

    pub fn extra_query_params(mut self, extra_query_params: Option<Multimap>) -> Self {
        self.extra_query_params = extra_query_params;
        self
    }

    /// Sets the region for the request
    pub fn region(mut self, region: Option<String>) -> Self {
        self.region = region;
        self
    }
}

impl S3Api for DeleteObject {
    type S3Response = DeleteObjectResponse;
}

impl ToS3Request for DeleteObject {
    fn to_s3request(self) -> Result<S3Request, ValidationErr> {
        check_bucket_name(&self.bucket, true)?;
        check_object_name(&self.object.key)?;

        let mut query_params: Multimap = self.extra_query_params.unwrap_or_default();
        query_params.add_version(self.object.version_id);

        let mut headers: Multimap = self.extra_headers.unwrap_or_default();
        if self.bypass_governance_mode {
            headers.add(X_AMZ_BYPASS_GOVERNANCE_RETENTION, "true");
        }

        Ok(S3Request::new(self.client, Method::DELETE)
            .region(self.region)
            .bucket(Some(self.bucket))
            .object(Some(self.object.key))
            .query_params(query_params)
            .headers(headers))
    }
}

// endregion: delete-object

// region: delete-objects
#[derive(Debug, Clone, Default)]
pub struct DeleteObjects {
    client: Client,

    bucket: String,
    objects: Vec<ObjectToDelete>,

    bypass_governance_mode: bool,
    verbose_mode: bool,

    extra_headers: Option<Multimap>,
    extra_query_params: Option<Multimap>,
    region: Option<String>,
}

impl DeleteObjects {
    pub fn new(client: Client, bucket: String, objects: Vec<ObjectToDelete>) -> Self {
        DeleteObjects {
            client,
            bucket,
            objects,
            ..Default::default()
        }
    }

    pub fn bypass_governance_mode(mut self, bypass_governance_mode: bool) -> Self {
        self.bypass_governance_mode = bypass_governance_mode;
        self
    }

    /// Enable verbose mode (defaults to false). If enabled, the response will
    /// include the keys of objects that were successfully deleted. Otherwise,
    /// only objects that encountered an error are returned.
    pub fn verbose_mode(mut self, verbose_mode: bool) -> Self {
        self.verbose_mode = verbose_mode;
        self
    }

    pub fn extra_headers(mut self, extra_headers: Option<Multimap>) -> Self {
        self.extra_headers = extra_headers;
        self
    }

    pub fn extra_query_params(mut self, extra_query_params: Option<Multimap>) -> Self {
        self.extra_query_params = extra_query_params;
        self
    }

    /// Sets the region for the request
    pub fn region(mut self, region: Option<String>) -> Self {
        self.region = region;
        self
    }
}

impl S3Api for DeleteObjects {
    type S3Response = DeleteObjectsResponse;
}

impl ToS3Request for DeleteObjects {
    fn to_s3request(self) -> Result<S3Request, ValidationErr> {
        check_bucket_name(&self.bucket, true)?;

        let mut data: String = String::from("<Delete>");
        if !self.verbose_mode {
            data.push_str("<Quiet>true</Quiet>");
        }
        for object in self.objects.iter() {
            data.push_str("<Object>");
            data.push_str("<Key>");
            data.push_str(&object.key);
            data.push_str("</Key>");
            if let Some(v) = object.version_id.as_ref() {
                data.push_str("<VersionId>");
                data.push_str(v);
                data.push_str("</VersionId>");
            }
            data.push_str("</Object>");
        }
        data.push_str("</Delete>");
        let bytes: Bytes = data.into();

        let mut headers: Multimap = self.extra_headers.unwrap_or_default();
        {
            if self.bypass_governance_mode {
                headers.add(X_AMZ_BYPASS_GOVERNANCE_RETENTION, "true");
            }
            headers.add(CONTENT_TYPE, "application/xml");
            headers.add(CONTENT_MD5, md5sum_hash(bytes.as_ref()));
        }

        Ok(S3Request::new(self.client, Method::POST)
            .region(self.region)
            .bucket(Some(self.bucket))
            .query_params(insert(self.extra_query_params, "delete"))
            .headers(headers)
            .body(Some(bytes.into())))
    }
}

// endregion: delete-objects

// region: object-stream

pub struct ObjectsStream {
    items: Pin<Box<dyn Stream<Item = ObjectToDelete> + Send + Sync>>,
}

impl ObjectsStream {
    pub fn from_stream(s: impl Stream<Item = ObjectToDelete> + Send + Sync + 'static) -> Self {
        Self { items: Box::pin(s) }
    }
}

impl From<ObjectToDelete> for ObjectsStream {
    fn from(delete_object: ObjectToDelete) -> Self {
        Self::from_stream(iter(std::iter::once(delete_object)))
    }
}

impl<I> From<I> for ObjectsStream
where
    I: Iterator<Item = ObjectToDelete> + Send + Sync + 'static,
{
    fn from(keys: I) -> Self {
        Self::from_stream(iter(keys))
    }
}

// endregion: object-stream

// region: delete-objects-streaming

/// Argument builder for the [`DeleteObjectsStreaming`](https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeleteObjects.html) S3 API operation.
/// Note that this API is not part of the official S3 API, but is a MinIO extension for streaming deletion of multiple objects.
///
/// This struct constructs the parameters required for the [`Client::`](crate::s3::client::Client::get_bucket_encryption) method.
pub struct DeleteObjectsStreaming {
    client: Client,

    bucket: String,
    objects: ObjectsStream,

    bypass_governance_mode: bool,
    verbose_mode: bool,

    extra_headers: Option<Multimap>,
    extra_query_params: Option<Multimap>,
    region: Option<String>,
}

impl DeleteObjectsStreaming {
    pub fn new(client: Client, bucket: String, objects: impl Into<ObjectsStream>) -> Self {
        Self {
            client,
            bucket,
            objects: objects.into(),

            bypass_governance_mode: false,
            verbose_mode: false,

            extra_headers: None,
            extra_query_params: None,
            region: None,
        }
    }

    pub fn bypass_governance_mode(mut self, bypass_governance_mode: bool) -> Self {
        self.bypass_governance_mode = bypass_governance_mode;
        self
    }

    /// Enable verbose mode (defaults to false). If enabled, the response will
    /// include the keys of objects that were successfully deleted. Otherwise
    /// only objects that encountered an error are returned.
    pub fn verbose_mode(mut self, verbose_mode: bool) -> Self {
        self.verbose_mode = verbose_mode;
        self
    }

    pub fn extra_headers(mut self, extra_headers: Option<Multimap>) -> Self {
        self.extra_headers = extra_headers;
        self
    }

    pub fn extra_query_params(mut self, extra_query_params: Option<Multimap>) -> Self {
        self.extra_query_params = extra_query_params;
        self
    }

    /// Sets the region for the request
    pub fn region(mut self, region: Option<String>) -> Self {
        self.region = region;
        self
    }

    async fn next_request(&mut self) -> Result<Option<DeleteObjects>, ValidationErr> {
        let mut objects = Vec::new();
        while let Some(object) = self.objects.items.next().await {
            objects.push(object);
            if objects.len() >= MAX_MULTIPART_COUNT as usize {
                break;
            }
        }
        if objects.is_empty() {
            return Ok(None);
        }

        Ok(Some(
            DeleteObjects::new(self.client.clone(), self.bucket.clone(), objects)
                .bypass_governance_mode(self.bypass_governance_mode)
                .verbose_mode(self.verbose_mode)
                .extra_headers(self.extra_headers.clone())
                .extra_query_params(self.extra_query_params.clone())
                .region(self.region.clone()),
        ))
    }
}

#[async_trait]
impl ToStream for DeleteObjectsStreaming {
    type Item = DeleteObjectsResponse;

    async fn to_stream(
        mut self,
    ) -> Box<dyn Stream<Item = Result<Self::Item, Error>> + Unpin + Send> {
        Box::new(Box::pin(futures_stream::unfold(
            self,
            move |mut this| async move {
                match this.next_request().await {
                    Ok(Some(request)) => {
                        let response = request.send().await;
                        Some((response, this))
                    }
                    Ok(None) => None,
                    Err(e) => Some((Err(e.into()), this)),
                }
            },
        )))
    }
}

// endregion: delete-objects-streaming
