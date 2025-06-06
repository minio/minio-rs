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

use http::Method;

use crate::s3::multimap::{Multimap, MultimapExt};
use crate::s3::utils::check_object_name;
use crate::s3::{
    client::Client,
    error::Error,
    response::GetObjectResponse,
    sse::{Sse, SseCustomerKey},
    types::{S3Api, S3Request, ToS3Request},
    utils::{UtcTime, check_bucket_name, to_http_header_value},
};

/// Argument builder for the [`GetObject`](https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetObject.html) S3 API operation.
///
/// This struct constructs the parameters required for the [`Client::get_object`](crate::s3::client::Client::get_object) method.
#[derive(Debug, Clone, Default)]
pub struct GetObject {
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

impl GetObject {
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

impl S3Api for GetObject {
    type S3Response = GetObjectResponse;
}

impl ToS3Request for GetObject {
    fn to_s3request(self) -> Result<S3Request, Error> {
        {
            check_bucket_name(&self.bucket, true)?;
            check_object_name(&self.object)?;
            if self.ssec.is_some() && !self.client.is_secure() {
                return Err(Error::SseTlsRequired(None));
            }
        }

        let mut headers: Multimap = self.extra_headers.unwrap_or_default();
        {
            {
                let (offset, length): (Option<u64>, Option<u64>) = match self.length {
                    Some(_) => (Some(self.offset.unwrap_or(0_u64)), self.length),
                    None => (self.offset, None),
                };

                if let Some(o) = offset {
                    let mut range: String = String::new();
                    range.push_str("bytes=");
                    range.push_str(&o.to_string());
                    range.push('-');
                    if let Some(l) = length {
                        range.push_str(&(o + l - 1).to_string());
                    }
                    headers.add("Range", range);
                }
            }

            if let Some(v) = self.match_etag {
                headers.add("if-match", v);
            }

            if let Some(v) = self.not_match_etag {
                headers.add("if-none-match", v);
            }

            if let Some(v) = self.modified_since {
                headers.add("if-modified-since", to_http_header_value(v));
            }

            if let Some(v) = self.unmodified_since {
                headers.add("if-unmodified-since", to_http_header_value(v));
            }

            if let Some(v) = &self.ssec {
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
