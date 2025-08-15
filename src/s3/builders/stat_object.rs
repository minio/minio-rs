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

use crate::s3::client::Client;
use crate::s3::error::ValidationErr;
use crate::s3::header_constants::*;
use crate::s3::multimap_ext::{Multimap, MultimapExt};
use crate::s3::response::StatObjectResponse;
use crate::s3::sse::{Sse, SseCustomerKey};
use crate::s3::types::{S3Api, S3Request, ToS3Request};
use crate::s3::utils::{
    UtcTime, check_bucket_name, check_object_name, check_ssec, to_http_header_value,
};
use async_trait::async_trait;
use http::Method;

/// Argument builder for the [`StatObject`](https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetObjectAttributes.html) S3 API operation.
/// Retrieves all of the metadata from an object without returning the object itself.
///
/// This struct constructs the parameters required for the [`Client::stat_object`](crate::s3::client::Client::stat_object) method.
#[derive(Debug, Clone, Default)]
pub struct StatObject {
    client: Client,

    extra_headers: Option<Multimap>,
    extra_query_params: Option<Multimap>,
    bucket: String,
    object: String,
    version_id: Option<String>,
    offset: Option<u64>,
    length: Option<u64>,
    region: Option<String>,
    ssec: Option<SseCustomerKey>,

    // Conditionals
    match_etag: Option<String>,
    not_match_etag: Option<String>,
    modified_since: Option<UtcTime>,
    unmodified_since: Option<UtcTime>,
}

impl StatObject {
    pub fn new(client: Client, bucket: String, object: String) -> Self {
        Self {
            client,
            bucket,
            object,
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

    pub fn version_id(mut self, version_id: Option<String>) -> Self {
        self.version_id = version_id;
        self
    }

    pub fn offset(mut self, offset: Option<u64>) -> Self {
        self.offset = offset;
        self
    }

    pub fn length(mut self, length: Option<u64>) -> Self {
        self.length = length;
        self
    }

    /// Sets the region for the request
    pub fn region(mut self, region: Option<String>) -> Self {
        self.region = region;
        self
    }

    pub fn ssec(mut self, ssec: Option<SseCustomerKey>) -> Self {
        self.ssec = ssec;
        self
    }

    pub fn match_etag(mut self, etag: Option<String>) -> Self {
        self.match_etag = etag;
        self
    }

    pub fn not_match_etag(mut self, etag: Option<String>) -> Self {
        self.not_match_etag = etag;
        self
    }

    pub fn modified_since(mut self, time: Option<UtcTime>) -> Self {
        self.modified_since = time;
        self
    }

    pub fn unmodified_since(mut self, time: Option<UtcTime>) -> Self {
        self.unmodified_since = time;
        self
    }
}

impl S3Api for StatObject {
    type S3Response = StatObjectResponse;
}

#[async_trait]
impl ToS3Request for StatObject {
    fn to_s3request(self) -> Result<S3Request, ValidationErr> {
        check_bucket_name(&self.bucket, true)?;
        check_object_name(&self.object)?;
        check_ssec(&self.ssec, &self.client)?;

        let mut headers: Multimap = self.extra_headers.unwrap_or_default();
        {
            if let Some(v) = self.match_etag {
                headers.add(IF_MATCH, v);
            }
            if let Some(v) = self.not_match_etag {
                headers.add(IF_NONE_MATCH, v);
            }
            if let Some(v) = self.modified_since {
                headers.add(IF_MODIFIED_SINCE, to_http_header_value(v));
            }
            if let Some(v) = self.unmodified_since {
                headers.add(IF_UNMODIFIED_SINCE, to_http_header_value(v));
            }
            if let Some(v) = self.ssec {
                headers.add_multimap(v.headers());
            }
        }

        let mut query_params: Multimap = self.extra_query_params.unwrap_or_default();
        query_params.add_version(self.version_id);

        Ok(S3Request::new(self.client, Method::GET)
            .region(self.region)
            .bucket(Some(self.bucket))
            .object(Some(self.object))
            .query_params(query_params)
            .headers(headers))
    }
}
