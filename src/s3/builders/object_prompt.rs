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

use crate::s3::builders::SegmentedBytes;
use crate::s3::sse::SseCustomerKey;
use crate::s3::utils::{Multimap, check_bucket_name};
use crate::s3::{
    client::Client,
    error::Error,
    response::ObjectPromptResponse,
    types::{S3Api, S3Request, ToS3Request},
};
use async_trait::async_trait;
use bytes::Bytes;
use http::Method;
use serde_json::json;

#[derive(Debug, Clone, Default)]
pub struct ObjectPrompt {
    client: Option<Client>,
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
impl ObjectPrompt {
    pub fn new(bucket: &str, object: &str, prompt: &str) -> Self {
        ObjectPrompt {
            client: None,
            bucket: bucket.to_string(),
            object: object.to_string(),
            prompt: prompt.to_string(),
            ..Default::default()
        }
    }

    pub fn client(mut self, client: &Client) -> Self {
        self.client = Some(client.clone());
        self
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

    pub fn region(mut self, region: Option<String>) -> Self {
        self.region = region;
        self
    }

    pub fn ssec(mut self, ssec: Option<SseCustomerKey>) -> Self {
        self.ssec = ssec;
        self
    }
}

impl S3Api for ObjectPrompt {
    type S3Response = ObjectPromptResponse;
}

#[async_trait]
impl ToS3Request for ObjectPrompt {
    async fn to_s3request(self) -> Result<S3Request, Error> {
        check_bucket_name(&self.bucket, true)?;

        let client: Client = self.client.ok_or(Error::NoClientProvided)?;
        if client.is_aws_host() {
            return Err(Error::UnsupportedApi(String::from("ObjectPrompt")));
        }

        if self.object.is_empty() {
            return Err(Error::InvalidObjectName(String::from(
                "object name cannot be empty",
            )));
        }

        if self.ssec.is_some() && !client.is_secure() {
            return Err(Error::SseTlsRequired(None));
        }

        let headers: Multimap = self.extra_headers.unwrap_or_default();
        let mut query_params: Multimap = self.extra_query_params.unwrap_or_default();
        if let Some(v) = &self.version_id {
            query_params.insert(String::from("versionId"), v.to_string());
        }
        query_params.insert(
            String::from("lambdaArn"),
            self.lambda_arn
                .as_ref()
                .map(ToString::to_string)
                .unwrap_or_default(),
        );

        let prompt_body = json!({ "prompt": self.prompt });
        let body: SegmentedBytes = SegmentedBytes::from(Bytes::from(prompt_body.to_string()));

        Ok(S3Request::new(client, Method::POST)
            .region(self.region)
            .bucket(Some(self.bucket))
            .object(Some(self.object))
            .query_params(query_params)
            .headers(headers)
            .body(Some(body)))
    }
}
