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

use crate::s3::{
    client::Client,
    error::Error,
    response::ListenBucketNotificationResponse,
    types::{NotificationRecords, S3Api, S3Request, ToS3Request},
    utils::{check_bucket_name, merge, Multimap},
};

/// Argument builder for
/// [listen_bucket_notification()](crate::s3::client::Client::listen_bucket_notification)
/// API.
#[derive(Clone, Debug, Default)]
pub struct ListenBucketNotification {
    client: Option<Client>,

    extra_headers: Option<Multimap>,
    extra_query_params: Option<Multimap>,
    region: Option<String>,
    bucket: String,
    prefix: Option<String>,
    suffix: Option<String>,
    events: Option<Vec<String>>,
}

#[async_trait]
impl S3Api for ListenBucketNotification {
    type S3Response = (
        ListenBucketNotificationResponse,
        Box<dyn Stream<Item = Result<NotificationRecords, Error>> + Unpin + Send>,
    );
}

impl ToS3Request for ListenBucketNotification {
    fn to_s3request(&self) -> Result<S3Request, Error> {
        let client = self.client.as_ref().ok_or(Error::NoClientProvided)?;
        if client.is_aws_host() {
            return Err(Error::UnsupportedApi(String::from(
                "ListenBucketNotification",
            )));
        }

        check_bucket_name(&self.bucket, true)?;

        let mut headers = Multimap::new();
        if let Some(v) = &self.extra_headers {
            merge(&mut headers, v);
        }

        let mut query_params = Multimap::new();
        if let Some(v) = &self.extra_query_params {
            merge(&mut query_params, v);
        }
        if let Some(v) = &self.prefix {
            query_params.insert(String::from("prefix"), v.to_string());
        }
        if let Some(v) = &self.suffix {
            query_params.insert(String::from("suffix"), v.to_string());
        }
        if let Some(v) = &self.events {
            for e in v.iter() {
                query_params.insert(String::from("events"), e.to_string());
            }
        } else {
            query_params.insert(String::from("events"), String::from("s3:ObjectCreated:*"));
            query_params.insert(String::from("events"), String::from("s3:ObjectRemoved:*"));
            query_params.insert(String::from("events"), String::from("s3:ObjectAccessed:*"));
        }

        let req = S3Request::new(client, Method::GET)
            .region(self.region.as_deref())
            .bucket(Some(&self.bucket))
            .query_params(query_params)
            .headers(headers);
        Ok(req)
    }
}

impl ListenBucketNotification {
    pub fn new(bucket_name: &str) -> ListenBucketNotification {
        ListenBucketNotification {
            bucket: bucket_name.to_owned(),
            ..Default::default()
        }
    }

    pub fn client(mut self, client: &Client) -> Self {
        self.client = Some(client.clone());
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
