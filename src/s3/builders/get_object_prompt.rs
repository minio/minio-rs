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

use crate::s3::client::MinioClient;
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
use std::sync::Arc;
use typed_builder::TypedBuilder;

/// Argument builder for the `GetObjectPrompt` operation.
///
/// This struct constructs the parameters required for the [`Client::get_object_prompt`](crate::s3::client::MinioClient::get_object_prompt) method.
#[derive(Debug, Clone, TypedBuilder)]
pub struct GetObjectPrompt {
    #[builder(!default)] // force required
    client: MinioClient,
    #[builder(default, setter(into))]
    extra_headers: Option<Multimap>,
    #[builder(default, setter(into))]
    extra_query_params: Option<Multimap>,
    #[builder(default, setter(into))]
    region: Option<String>,
    #[builder(setter(into))] // force required + accept Into<String>
    bucket: String,
    #[builder(setter(into))] // force required + accept Into<String>
    object: String,
    #[builder(setter(into))] // force required + accept Into<String>
    prompt: String,
    #[builder(default, setter(into))]
    lambda_arn: Option<String>,
    #[builder(default, setter(into))]
    version_id: Option<String>,
    #[builder(default, setter(into))]
    ssec: Option<SseCustomerKey>,
}

pub type GetObjectPromptBldr = GetObjectPromptBuilder<(
    (MinioClient,),
    (),
    (),
    (),
    (String,),
    (String,),
    (String,),
    (),
    (),
    (),
)>;

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
        let body = Arc::new(SegmentedBytes::from(Bytes::from(prompt_body.to_string())));

        Ok(S3Request::builder()
            .client(self.client)
            .method(Method::POST)
            .region(self.region)
            .bucket(self.bucket)
            .object(self.object)
            .query_params(query_params)
            .headers(self.extra_headers.unwrap_or_default())
            .body(body)
            .build())
    }
}
