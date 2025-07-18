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

use crate::s3::client::Client;
use crate::s3::error::{Error, ValidationErr};
use crate::s3::multimap::{Multimap, MultimapExt};
use crate::s3::response::ListenBucketNotificationResponse;
use crate::s3::types::{NotificationRecords, S3Api, S3Request, ToS3Request};
use crate::s3::utils::check_bucket_name;
use async_trait::async_trait;
use futures_util::Stream;
use http::Method;

/// Argument builder for the [`ListenBucketNotification`](https://min.io/docs/minio/linux/developers/go/API.html#ListenBucketNotification)
///
/// This struct constructs the parameters required for the [`Client::listen_bucket_notification`](crate::s3::client::Client::listen_bucket_notification) method.
#[derive(Clone, Debug, Default)]
pub struct ListenBucketNotification {
    client: Client,

    extra_headers: Option<Multimap>,
    extra_query_params: Option<Multimap>,
    region: Option<String>,
    bucket: String,
    prefix: Option<String>,
    suffix: Option<String>,
    events: Option<Vec<String>>,
}

impl ListenBucketNotification {
    pub fn new(client: Client, bucket: String) -> Self {
        Self {
            client,
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

    /// Sets the region for the request
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
    fn to_s3request(self) -> Result<S3Request, ValidationErr> {
        {
            check_bucket_name(&self.bucket, true)?;
            if self.client.is_aws_host() {
                return Err(ValidationErr::UnsupportedAwsApi(
                    "ListenBucketNotification".into(),
                ));
            }
        }

        let mut query_params: Multimap = self.extra_query_params.unwrap_or_default();
        {
            if let Some(v) = self.prefix {
                query_params.add("prefix", v);
            }
            if let Some(v) = self.suffix {
                query_params.add("suffix", v);
            }
            if let Some(v) = self.events {
                for e in v.into_iter() {
                    query_params.add("events", e);
                }
            } else {
                query_params.add("events", "s3:ObjectCreated:*");
                query_params.add("events", "s3:ObjectRemoved:*");
                query_params.add("events", "s3:ObjectAccessed:*");
            }
        }

        Ok(S3Request::new(self.client, Method::GET)
            .region(self.region)
            .bucket(Some(self.bucket))
            .query_params(query_params)
            .headers(self.extra_headers.unwrap_or_default()))
    }
}
