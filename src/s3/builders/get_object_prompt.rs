// MinIO Rust Library for Amazon S3 Compatible Cloud Storage
// Copyright 2024 MinIO, Inc.
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
use crate::s3::multimap_ext::{Multimap, MultimapExt};
use crate::s3::response::GetObjectPromptResponse;
use crate::s3::segmented_bytes::SegmentedBytes;
use crate::s3::sse::SseCustomerKey;
use crate::s3::types::{S3Api, S3Request, ToS3Request};
use crate::s3::utils::{check_bucket_name, check_object_name, check_ssec};
use bytes::Bytes;
use http::Method;
use serde_json::json;

/// Argument builder for the `GetObjectPrompt` operation.
///
/// This struct constructs the parameters required for the [`Client::get_object_prompt`](crate::s3::client::Client::get_object_prompt) method.
#[derive(Debug, Clone, Default)]
pub struct GetObjectPrompt {
    client: Client,
    bucket: String,
    object: String,
    prompt: String,
    lambda_arn: Option<String>,

    version_id: Option<String>,
    region: Option<String>,
    ssec: Option<SseCustomerKey>,
    extra_headers: Option<Multimap>,
    extra_query_params: Option<Multimap>,
}

// builder interface
impl GetObjectPrompt {
    pub fn new(client: Client, bucket: String, object: String, prompt: String) -> Self {
        GetObjectPrompt {
            client,
            bucket,
            object,
            prompt,
            ..Default::default()
        }
    }

    pub fn lambda_arn(mut self, lambda_arn: &str) -> Self {
        self.lambda_arn = Some(lambda_arn.to_string());
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

    pub fn version_id(mut self, version_id: Option<String>) -> Self {
        self.version_id = version_id;
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
}

impl S3Api for GetObjectPrompt {
    type S3Response = GetObjectPromptResponse;
}

impl ToS3Request for GetObjectPrompt {
    fn to_s3request(self) -> Result<S3Request, ValidationErr> {
        {
            check_bucket_name(&self.bucket, true)?;
            check_object_name(&self.object)?;
            check_ssec(&self.ssec, &self.client)?;
            if self.client.is_aws_host() {
                return Err(ValidationErr::UnsupportedAwsApi("ObjectPrompt".into()));
            }
        }
        let mut query_params: Multimap = self.extra_query_params.unwrap_or_default();
        query_params.add_version(self.version_id);

        query_params.add(
            "lambdaArn",
            self.lambda_arn
                .as_ref()
                .map(ToString::to_string)
                .unwrap_or_default(),
        );

        let prompt_body = json!({ "prompt": self.prompt });
        let body: SegmentedBytes = SegmentedBytes::from(Bytes::from(prompt_body.to_string()));

        Ok(S3Request::new(self.client, Method::POST)
            .region(self.region)
            .bucket(Some(self.bucket))
            .object(Some(self.object))
            .query_params(query_params)
            .headers(self.extra_headers.unwrap_or_default())
            .body(Some(body)))
    }
}
