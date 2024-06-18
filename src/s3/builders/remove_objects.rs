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

use std::pin::Pin;

use async_trait::async_trait;
use bytes::Bytes;
use futures_util::{stream as futures_stream, Stream, StreamExt};
use http::Method;
use tokio_stream::iter as stream_iter;

use crate::s3::{
    client_core::ClientCore,
    error::Error,
    response::{RemoveObjectResponse, RemoveObjectsResponse},
    types::{S3Api, S3Request, ToS3Request, ToStream},
    utils::{check_bucket_name, md5sum_hash, merge, Multimap},
    Client,
};

/// Specify an object to be deleted. The object can be specified by key or by
/// key and version_id via the From trait.
#[derive(Debug, Clone)]
pub struct ObjectToDelete {
    key: String,
    version_id: Option<String>,
}

/// A key can be converted into a DeleteObject. The version_id is set to None.
impl From<&str> for ObjectToDelete {
    fn from(key: &str) -> Self {
        ObjectToDelete {
            key: key.to_string(),
            version_id: None,
        }
    }
}

/// A tuple of key and version_id can be converted into a DeleteObject.
impl From<(&str, &str)> for ObjectToDelete {
    fn from((key, version_id): (&str, &str)) -> Self {
        ObjectToDelete {
            key: key.to_string(),
            version_id: Some(version_id.to_string()),
        }
    }
}

/// A tuple of key and option version_id can be converted into a DeleteObject.
impl From<(&str, Option<&str>)> for ObjectToDelete {
    fn from((key, version_id): (&str, Option<&str>)) -> Self {
        ObjectToDelete {
            key: key.to_string(),
            version_id: version_id.map(|v| v.to_string()),
        }
    }
}

#[derive(Debug, Clone)]
pub struct RemoveObject {
    client: Option<Client>,

    bucket: String,
    object: ObjectToDelete,

    bypass_governance_mode: bool,

    extra_headers: Option<Multimap>,
    extra_query_params: Option<Multimap>,
    region: Option<String>,
}

impl RemoveObject {
    pub fn new(bucket: &str, object: impl Into<ObjectToDelete>) -> Self {
        Self {
            client: None,

            bucket: bucket.to_string(),
            object: object.into(),

            bypass_governance_mode: false,

            extra_headers: None,
            extra_query_params: None,
            region: None,
        }
    }

    pub fn client(mut self, client: &Client) -> Self {
        self.client = Some(client.clone());
        self
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
    fn to_s3request(&self) -> Result<S3Request, Error> {
        check_bucket_name(&self.bucket, true)?;

        let mut headers = Multimap::new();
        if let Some(v) = &self.extra_headers {
            merge(&mut headers, v);
        }

        let mut query_params = Multimap::new();
        if let Some(v) = &self.extra_query_params {
            merge(&mut query_params, v);
        }
        if let Some(v) = &self.object.version_id {
            query_params.insert(String::from("versionId"), v.to_string());
        }

        let req = S3Request::new(
            self.client.as_ref().ok_or(Error::NoClientProvided)?,
            Method::DELETE,
        )
        .region(self.region.as_deref())
        .bucket(Some(&self.bucket))
        .object(Some(&self.object.key))
        .query_params(query_params)
        .headers(headers);
        Ok(req)
    }
}

#[derive(Debug, Clone)]
pub struct RemoveObjectsApi {
    client: Option<ClientCore>,

    bucket: String,
    objects: Vec<ObjectToDelete>,

    bypass_governance_mode: bool,
    verbose_mode: bool,

    extra_headers: Option<Multimap>,
    extra_query_params: Option<Multimap>,
    region: Option<String>,
}

impl RemoveObjectsApi {
    pub fn new(bucket: &str, objects: Vec<ObjectToDelete>) -> Self {
        RemoveObjectsApi {
            client: None,

            bucket: bucket.to_string(),
            objects,

            bypass_governance_mode: false,
            verbose_mode: false,

            extra_headers: None,
            extra_query_params: None,
            region: None,
        }
    }

    pub fn client(mut self, client: &ClientCore) -> Self {
        self.client = Some(client.clone());
        self
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

impl ToS3Request for RemoveObjectsApi {
    fn to_s3request(&self) -> Result<S3Request, Error> {
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

        let mut headers = Multimap::new();
        if let Some(v) = &self.extra_headers {
            merge(&mut headers, v);
        }
        if self.bypass_governance_mode {
            headers.insert(
                String::from("x-amz-bypass-governance-retention"),
                String::from("true"),
            );
        }
        headers.insert(
            String::from("Content-Type"),
            String::from("application/xml"),
        );
        headers.insert(String::from("Content-MD5"), md5sum_hash(data.as_ref()));

        let mut query_params = Multimap::new();
        if let Some(v) = &self.extra_query_params {
            merge(&mut query_params, v);
        }
        query_params.insert(String::from("delete"), String::new());

        let client = self.client.as_ref().ok_or(Error::NoClientProvided)?.inner();
        let req = S3Request::new(client, Method::POST)
            .region(self.region.as_deref())
            .bucket(Some(&self.bucket))
            .query_params(query_params)
            .headers(headers)
            .body(Some(data.into()));
        Ok(req)
    }
}

impl S3Api for RemoveObjectsApi {
    type S3Response = RemoveObjectsResponse;
}

pub struct DeleteObjects {
    items: Pin<Box<dyn Stream<Item = ObjectToDelete> + Send + Sync>>,
}

impl DeleteObjects {
    pub fn from_stream(s: impl Stream<Item = ObjectToDelete> + Send + Sync + 'static) -> Self {
        DeleteObjects { items: Box::pin(s) }
    }
}

impl From<ObjectToDelete> for DeleteObjects {
    fn from(delete_object: ObjectToDelete) -> Self {
        DeleteObjects::from_stream(stream_iter(std::iter::once(delete_object)))
    }
}

impl<I: Iterator<Item = ObjectToDelete> + Send + Sync + 'static> From<I> for DeleteObjects {
    fn from(keys: I) -> Self {
        DeleteObjects::from_stream(stream_iter(keys))
    }
}

pub struct RemoveObjects {
    client: Option<Client>,

    bucket: String,
    objects: DeleteObjects,

    bypass_governance_mode: bool,
    verbose_mode: bool,

    extra_headers: Option<Multimap>,
    extra_query_params: Option<Multimap>,
    region: Option<String>,
}

impl RemoveObjects {
    pub fn new(bucket: &str, objects: impl Into<DeleteObjects>) -> Self {
        RemoveObjects {
            client: None,

            bucket: bucket.to_string(),
            objects: objects.into(),

            bypass_governance_mode: false,
            verbose_mode: false,

            extra_headers: None,
            extra_query_params: None,
            region: None,
        }
    }

    pub fn client(mut self, client: &Client) -> Self {
        self.client = Some(client.clone());
        self
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
        let client_core = ClientCore::new(self.client.as_ref().ok_or(Error::NoClientProvided)?);
        let request = RemoveObjectsApi::new(&self.bucket, objects)
            .client(&client_core)
            .bypass_governance_mode(self.bypass_governance_mode)
            .verbose_mode(self.verbose_mode)
            .extra_headers(self.extra_headers.clone())
            .extra_query_params(self.extra_query_params.clone())
            .region(self.region.clone());
        Ok(Some(request))
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
