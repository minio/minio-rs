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
use crate::s3::client::DEFAULT_EXPIRY_SECONDS;
use crate::s3::error::Error;
use crate::s3::response::GetPresignedObjectUrlResponse;
use crate::s3::signer::presign_v4;
use crate::s3::utils::{Multimap, UtcTime, check_bucket_name, check_object_name, utc_now};
use http::Method;
use std::sync::Arc;

/// Argument for [get_presigned_object_url()](crate::s3::client::Client::get_presigned_object_url) API
#[derive(Clone, Debug, Default)]
pub struct GetPresignedObjectUrl {
    client: Arc<Client>,

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
    pub fn new(client: &Arc<Client>, bucket: String, object: String, method: Method) -> Self {
        Self {
            client: Arc::clone(client),
            bucket,
            object,
            method,
            expiry_seconds: Some(DEFAULT_EXPIRY_SECONDS),
            ..Default::default()
        }
    }

    pub async fn send(self) -> Result<GetPresignedObjectUrlResponse, Error> {
        // NOTE: this send function is async to be comparable with other functions...
        check_bucket_name(&self.bucket, true)?;
        check_object_name(&self.object)?;

        let region: String = self.client.get_region_cached(&self.bucket, &self.region)?;

        let mut query_params: Multimap = self.extra_query_params.unwrap_or_default();
        if let Some(v) = &self.version_id {
            query_params.insert("versionId".into(), v.to_owned());
        }

        let mut url = self.client.base_url.build_url(
            &self.method,
            &region,
            &query_params,
            Some(&self.bucket),
            Some(&self.object),
        )?;

        if let Some(p) = &self.client.provider {
            let creds = p.fetch();
            if let Some(t) = creds.session_token {
                query_params.insert("X-Amz-Security-Token".into(), t);
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
