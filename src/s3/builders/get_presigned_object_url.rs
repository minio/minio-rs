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

use crate::s3::client::MinioClient;
use crate::s3::creds::Credentials;
use crate::s3::error::Error;
use crate::s3::header_constants::*;
use crate::s3::multimap_ext::{Multimap, MultimapExt};
use crate::s3::response::GetPresignedObjectUrlResponse;
use crate::s3::signer::presign_v4;
use crate::s3::utils::{UtcTime, check_bucket_name, check_object_name, utc_now};
use http::Method;
use typed_builder::TypedBuilder;

/// The default expiry time in seconds for a [`GetPresignedObjectUrl`].
pub const DEFAULT_EXPIRY_SECONDS: u32 = 604_800; // 7 days

/// This struct constructs the parameters required for the [`Client::get_presigned_object_url`](crate::s3::client::MinioClient::get_presigned_object_url) method.
#[derive(Clone, Debug, TypedBuilder)]
pub struct GetPresignedObjectUrl {
    #[builder(!default)] // force required
    client: MinioClient,
    #[builder(default, setter(into))]
    extra_query_params: Option<Multimap>,
    #[builder(default, setter(into))]
    region: Option<String>,
    #[builder(setter(into))] // force required + accept Into<String>
    bucket: String,
    #[builder(setter(into))] // force required + accept Into<String>
    object: String,
    #[builder(default, setter(into))]
    version_id: Option<String>,
    #[builder(!default)]
    method: Method,

    #[builder(default=Some(DEFAULT_EXPIRY_SECONDS), setter(into))]
    expiry_seconds: Option<u32>,
    #[builder(default, setter(into))]
    request_time: Option<UtcTime>,
}

/// Builder type alias for [`GetPresignedObjectUrl`].
///
/// Constructed via [`GetPresignedObjectUrl::builder()`](GetPresignedObjectUrl::builder) and used to build a [`GetPresignedObjectUrl`] instance.
pub type GetPresignedObjectUrlBldr = GetPresignedObjectUrlBuilder<(
    (MinioClient,),
    (),
    (),
    (String,),
    (String,),
    (),
    (Method,),
    (),
    (),
)>;

impl GetPresignedObjectUrl {
    /// Sends the request to generate a presigned URL for an S3 object.
    pub async fn send(self) -> Result<GetPresignedObjectUrlResponse, Error> {
        check_bucket_name(&self.bucket, true)?;
        check_object_name(&self.object)?;

        let region: String = self
            .client
            .get_region_cached(&self.bucket, &self.region)
            .await?;

        let mut query_params: Multimap = self.extra_query_params.unwrap_or_default();
        query_params.add_version(self.version_id.clone());

        let mut url = self.client.shared.base_url.build_url(
            &self.method,
            &region,
            &query_params,
            Some(&self.bucket),
            Some(&self.object),
        )?;

        if let Some(p) = &self.client.shared.provider {
            let creds: Credentials = p.fetch();
            if let Some(t) = creds.session_token {
                query_params.add(X_AMZ_SECURITY_TOKEN, t);
            }

            let date = match self.request_time {
                Some(v) => v,
                _ => utc_now(),
            };

            presign_v4(
                &self.method,
                &url.host_header_value(),
                &url.path,
                &region,
                &mut query_params,
                &creds.access_key,
                &creds.secret_key,
                date,
                self.expiry_seconds.unwrap_or(DEFAULT_EXPIRY_SECONDS),
            );

            url.query = query_params;
        }

        Ok(GetPresignedObjectUrlResponse {
            region,
            bucket: self.bucket,
            object: self.object,
            version_id: self.version_id,
            url: url.to_string(),
        })
    }
}
