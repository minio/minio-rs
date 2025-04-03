// MinIO Rust Library for Amazon S3 Compatible Cloud Storage
// Copyright 2023 MinIO, Inc.
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

use async_trait::async_trait;
use futures_util::Stream;
use http::Method;
use std::sync::Arc;

use crate::s3::{
    client::Client,
    error::Error,
    response::ListenBucketNotificationResponse,
    types::{NotificationRecords, S3Api, S3Request, ToS3Request},
    utils::{Multimap, check_bucket_name},
};

/// Argument builder for
/// [listen_bucket_notification()](crate::s3::client::Client::listen_bucket_notification)
/// API.
#[derive(Clone, Debug, Default)]
pub struct ListenBucketNotification {
    client: Arc<Client>,

    extra_headers: Option<Multimap>,
    extra_query_params: Option<Multimap>,
    region: Option<String>,
    bucket: String,
    prefix: Option<String>,
    suffix: Option<String>,
    events: Option<Vec<String>>,
}

impl ListenBucketNotification {
    pub fn new(client: &Arc<Client>, bucket: String) -> Self {
        Self {
            client: Arc::clone(client),
            bucket,
            ..Default::default()
        }
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

    pub fn prefix(mut self, prefix: Option<String>) -> Self {
        self.prefix = prefix;
        self
    }

    pub fn suffix(mut self, suffix: Option<String>) -> Self {
        self.suffix = suffix;
        self
    }

    pub fn events(mut self, events: Option<Vec<String>>) -> Self {
        self.events = events;
        self
    }
}

#[async_trait]
impl S3Api for ListenBucketNotification {
    type S3Response = (
        ListenBucketNotificationResponse,
        Box<dyn Stream<Item = Result<NotificationRecords, Error>> + Unpin + Send>,
    );
}

impl ToS3Request for ListenBucketNotification {
    fn to_s3request(self) -> Result<S3Request, Error> {
        {
            check_bucket_name(&self.bucket, true)?;
            if self.client.is_aws_host() {
                return Err(Error::UnsupportedApi("ListenBucketNotification".into()));
            }
        }

        let mut query_params: Multimap = self.extra_query_params.unwrap_or_default();
        {
            if let Some(v) = self.prefix {
                query_params.insert("prefix".into(), v);
            }
            if let Some(v) = self.suffix {
                query_params.insert("suffix".into(), v);
            }
            if let Some(v) = self.events {
                for e in v.into_iter() {
                    query_params.insert("events".into(), e);
                }
            } else {
                query_params.insert("events".into(), "s3:ObjectCreated:*".into());
                query_params.insert("events".into(), "s3:ObjectRemoved:*".into());
                query_params.insert("events".into(), "s3:ObjectAccessed:*".into());
            }
        }

        Ok(S3Request::new(self.client, Method::GET)
            .region(self.region)
            .bucket(Some(self.bucket))
            .query_params(query_params)
            .headers(self.extra_headers.unwrap_or_default()))
    }
}
