// MinIO Rust Library for Amazon S3 Compatible Cloud Storage
// Copyright 2022-2024 MinIO, Inc.
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

//! Builders for RemoveObject APIs.

use async_trait::async_trait;
use bytes::Bytes;
use futures_util::{Stream, StreamExt, stream as futures_stream};
use http::Method;
use std::pin::Pin;
use std::sync::Arc;
use tokio_stream::iter as stream_iter;

use crate::s3::multimap::{Multimap, MultimapExt};
use crate::s3::response::DeleteError;
use crate::s3::types::ListEntry;
use crate::s3::utils::{check_object_name, insert};
use crate::s3::{
    Client,
    error::Error,
    response::{RemoveObjectResponse, RemoveObjectsResponse},
    types::{S3Api, S3Request, ToS3Request, ToStream},
    utils::{check_bucket_name, md5sum_hash},
};

// region: object-to-delete
/// Specify an object to be deleted. The object can be specified by key or by
/// key and version_id via the From trait.
#[derive(Debug, Clone, Default)]
pub struct ObjectToDelete {
    key: String,
    version_id: Option<String>,
}

/// A key can be converted into a DeleteObject. The version_id is set to None.
impl From<&str> for ObjectToDelete {
    fn from(key: &str) -> Self {
        Self {
            key: key.to_owned(),
            version_id: None,
        }
    }
}

/// A tuple of key and version_id can be converted into a DeleteObject.
impl From<(&str, &str)> for ObjectToDelete {
    fn from((key, version_id): (&str, &str)) -> Self {
        Self {
            key: key.to_string(),
            version_id: Some(version_id.to_string()),
        }
    }
}

/// A tuple of key and option version_id can be converted into a DeleteObject.
impl From<(&str, Option<&str>)> for ObjectToDelete {
    fn from((key, version_id): (&str, Option<&str>)) -> Self {
        Self {
            key: key.to_string(),
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

// region: remove-object

#[derive(Debug, Clone, Default)]
pub struct RemoveObject {
    client: Arc<Client>,

    extra_headers: Option<Multimap>,
    extra_query_params: Option<Multimap>,
    region: Option<String>,
    bucket: String,

    object: ObjectToDelete,
    bypass_governance_mode: bool,
}

impl RemoveObject {
    pub fn new(client: &Arc<Client>, bucket: String, object: impl Into<ObjectToDelete>) -> Self {
        Self {
            client: Arc::clone(client),
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

    pub fn region(mut self, region: Option<String>) -> Self {
        self.region = region;
        self
    }
}

impl S3Api for RemoveObject {
    type S3Response = RemoveObjectResponse;
}

impl ToS3Request for RemoveObject {
    fn to_s3request(self) -> Result<S3Request, Error> {
        check_bucket_name(&self.bucket, true)?;
        check_object_name(&self.object.key)?;

        let mut query_params: Multimap = self.extra_query_params.unwrap_or_default();
        query_params.add_version(self.object.version_id);

        Ok(S3Request::new(self.client, Method::DELETE)
            .region(self.region)
            .bucket(Some(self.bucket))
            .object(Some(self.object.key))
            .query_params(query_params)
            .headers(self.extra_headers.unwrap_or_default()))
    }
}

// endregion: remove-object

// region: remove-object-api
#[derive(Debug, Clone, Default)]
pub struct RemoveObjectsApi {
    client: Arc<Client>,

    bucket: String,
    objects: Vec<ObjectToDelete>,

    bypass_governance_mode: bool,
    verbose_mode: bool,

    extra_headers: Option<Multimap>,
    extra_query_params: Option<Multimap>,
    region: Option<String>,
}

impl RemoveObjectsApi {
    pub fn new(client: &Arc<Client>, bucket: String, objects: Vec<ObjectToDelete>) -> Self {
        RemoveObjectsApi {
            client: Arc::clone(client),
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

    pub fn region(mut self, region: Option<String>) -> Self {
        self.region = region;
        self
    }
}

impl S3Api for RemoveObjectsApi {
    type S3Response = RemoveObjectsResponse;
}

impl ToS3Request for RemoveObjectsApi {
    fn to_s3request(self) -> Result<S3Request, Error> {
        check_bucket_name(&self.bucket, true)?;

        let mut data = String::from("<Delete>");
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
        let data: Bytes = data.into();

        let mut headers: Multimap = self.extra_headers.unwrap_or_default();
        {
            if self.bypass_governance_mode {
                headers.add("x-amz-bypass-governance-retention", "true");
            }
            headers.add("Content-Type", "application/xml");
            headers.add("Content-MD5", md5sum_hash(data.as_ref()));
        }

        Ok(S3Request::new(self.client, Method::POST)
            .region(self.region)
            .bucket(Some(self.bucket))
            .query_params(insert(self.extra_query_params, "delete"))
            .headers(headers)
            .body(Some(data.into())))
    }
}

// endregion: remove-object-api

// region: delete-object
pub struct DeleteObjects {
    items: Pin<Box<dyn Stream<Item = ObjectToDelete> + Send + Sync>>,
}

impl DeleteObjects {
    pub fn from_stream(s: impl Stream<Item = ObjectToDelete> + Send + Sync + 'static) -> Self {
        Self { items: Box::pin(s) }
    }
}

impl From<ObjectToDelete> for DeleteObjects {
    fn from(delete_object: ObjectToDelete) -> Self {
        Self::from_stream(stream_iter(std::iter::once(delete_object)))
    }
}

impl<I> From<I> for DeleteObjects
where
    I: Iterator<Item = ObjectToDelete> + Send + Sync + 'static,
{
    fn from(keys: I) -> Self {
        Self::from_stream(stream_iter(keys))
    }
}

// endregion: delete-object

// region: remove-objects

pub struct RemoveObjects {
    client: Arc<Client>,

    bucket: String,
    objects: DeleteObjects,

    bypass_governance_mode: bool,
    verbose_mode: bool,

    extra_headers: Option<Multimap>,
    extra_query_params: Option<Multimap>,
    region: Option<String>,
}

impl RemoveObjects {
    pub fn new(client: &Arc<Client>, bucket: String, objects: impl Into<DeleteObjects>) -> Self {
        Self {
            client: Arc::clone(client),
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

    pub fn region(mut self, region: Option<String>) -> Self {
        self.region = region;
        self
    }

    async fn next_request(&mut self) -> Result<Option<RemoveObjectsApi>, Error> {
        let mut objects = Vec::new();
        while let Some(object) = self.objects.items.next().await {
            objects.push(object);
            if objects.len() >= 1000 {
                break;
            }
        }
        if objects.is_empty() {
            return Ok(None);
        }

        Ok(Some(
            RemoveObjectsApi::new(&self.client, self.bucket.clone(), objects)
                .bypass_governance_mode(self.bypass_governance_mode)
                .verbose_mode(self.verbose_mode)
                .extra_headers(self.extra_headers.clone())
                .extra_query_params(self.extra_query_params.clone())
                .region(self.region.clone()),
        ))
    }
}

#[async_trait]
impl ToStream for RemoveObjects {
    type Item = RemoveObjectsResponse;

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
                    Err(e) => Some((Err(e), this)),
                }
            },
        )))
    }
}

// endregion: remove-objects
