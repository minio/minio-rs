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

use crate::s3::client::MinioClient;
use crate::s3::error::ValidationErr;
use crate::s3::header_constants::*;
use crate::s3::multimap_ext::{Multimap, MultimapExt};
use crate::s3::response::GetObjectResponse;
use crate::s3::sse::{Sse, SseCustomerKey};
use crate::s3::types::{BucketName, ObjectKey, Region, S3Api, S3Request, ToS3Request, VersionId};
use crate::s3::utils::{UtcTime, check_ssec, to_http_header_value};
use http::Method;
use typed_builder::TypedBuilder;

/// Argument builder for the [`GetObject`](https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetObject.html) S3 API operation.
///
/// This struct constructs the parameters required for the [`Client::get_object`](crate::s3::client::MinioClient::get_object) method.
#[derive(Debug, Clone, TypedBuilder)]
pub struct GetObject {
    #[builder(!default)] // force required
    client: MinioClient,
    #[builder(default, setter(into))]
    extra_headers: Option<Multimap>,
    #[builder(default, setter(into))]
    extra_query_params: Option<Multimap>,
    #[builder(default, setter(into))]
    region: Option<Region>,
    #[builder(setter(into))]
    bucket: BucketName,
    #[builder(setter(into))]
    object: ObjectKey,
    #[builder(default)]
    version_id: Option<String>,
    #[builder(default, setter(into))]
    offset: Option<u64>,
    #[builder(default, setter(into))]
    length: Option<u64>,
    #[builder(default, setter(into))]
    ssec: Option<SseCustomerKey>,

    // Conditionals
    #[builder(default, setter(into))]
    match_etag: Option<String>,
    #[builder(default, setter(into))]
    not_match_etag: Option<String>,
    #[builder(default, setter(into))]
    modified_since: Option<UtcTime>,
    #[builder(default, setter(into))]
    unmodified_since: Option<UtcTime>,
}

/// Builder type alias for [`GetObject`].
///
/// Constructed via [`GetObject::builder()`](GetObject::builder) and used to build a [`GetObject`] instance.
pub type GetObjectBldr = GetObjectBuilder<(
    (MinioClient,),
    (),
    (),
    (),
    (BucketName,),
    (ObjectKey,),
    (),
    (),
    (),
    (),
    (),
    (),
    (),
    (),
)>;

impl S3Api for GetObject {
    type S3Response = GetObjectResponse;
}

impl ToS3Request for GetObject {
    fn to_s3request(self) -> Result<S3Request, ValidationErr> {
        check_ssec(&self.ssec, &self.client)?;

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
                    headers.add(RANGE, range);
                }
            }

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

            if let Some(v) = &self.ssec {
                headers.add_multimap(v.headers());
            }
        }

        let mut query_params: Multimap = self.extra_query_params.unwrap_or_default();
        let version_id = self
            .version_id
            .map(|v| VersionId::new(v).expect("valid version id"));
        query_params.add_version(version_id);

        Ok(S3Request::builder()
            .client(self.client)
            .method(Method::GET)
            .region(self.region)
            .bucket(self.bucket)
            .object(self.object)
            .query_params(query_params)
            .headers(headers)
            .build())
    }
}
