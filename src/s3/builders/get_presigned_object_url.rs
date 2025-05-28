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
use crate::s3::creds::Credentials;
use crate::s3::error::Error;
use crate::s3::multimap::{Multimap, MultimapExt};
use crate::s3::response::GetPresignedObjectUrlResponse;
use crate::s3::signer::presign_v4;
use crate::s3::utils::{UtcTime, check_bucket_name, check_object_name, utc_now};
use http::Method;

/// The default expiry time in seconds for a [`GetPresignedObjectUrl`].
pub const DEFAULT_EXPIRY_SECONDS: u32 = 604_800; // 7 days

/// This struct constructs the parameters required for the [`Client::get_presigned_object_url`](crate::s3::client::Client::get_presigned_object_url) method.
#[derive(Clone, Debug, Default)]
pub struct GetPresignedObjectUrl {
    client: Client,

    extra_query_params: Option<Multimap>,
    region: Option<String>,
    bucket: String,

    object: String,
    version_id: Option<String>,
    method: Method,

    expiry_seconds: Option<u32>,
    request_time: Option<UtcTime>,
}

impl GetPresignedObjectUrl {
    pub fn new(client: Client, bucket: String, object: String, method: Method) -> Self {
        Self {
            client,
            bucket,
            object,
            method,
            expiry_seconds: Some(DEFAULT_EXPIRY_SECONDS),
            ..Default::default()
        }
    }

    /// Sets the expiry time for the presigned URL, defaulting to 7 days if not specified.
    pub fn expiry_seconds(mut self, seconds: u32) -> Self {
        self.expiry_seconds = Some(seconds);
        self
    }

    /// Sets the request time for the presigned URL, defaulting to the current time if not specified.
    pub fn request_time(mut self, time: UtcTime) -> Self {
        self.request_time = Some(time);
        self
    }

    /// Sends the request to generate a presigned URL for an S3 object.
    pub async fn send(self) -> Result<GetPresignedObjectUrlResponse, Error> {
        // NOTE: this send function is async and because of that, not comparable with other send functions...
        check_bucket_name(&self.bucket, true)?;
        check_object_name(&self.object)?;

        let region: String = self.client.get_region_cached(&self.bucket, &self.region)?;

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
                query_params.add("X-Amz-Security-Token", t);
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
