// MinIO Rust Library for Amazon S3 Compatible Cloud Storage
// Copyright 2022-2024 MinIO, Inc.
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

//! Module containing lower level APIs.

use super::{
    builders::{ObjectToDelete, RemoveObjectsApi},
    Client,
};

/// ClientCore exposes lower-level APIs not exposed by the high-level client.
#[derive(Debug, Clone)]
pub struct ClientCore(Client);

impl ClientCore {
    pub fn new(client: &Client) -> Self {
        Self(client.clone())
    }

    pub(crate) fn inner(&self) -> &Client {
        &self.0
    }

    /// Creates a builder to execute
    /// [DeleteObjects](https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeleteObjects.html)
    /// S3 API
    pub fn delete_objects(&self, bucket: &str, object: Vec<ObjectToDelete>) -> RemoveObjectsApi {
        RemoveObjectsApi::new(bucket, object).client(self)
    }
}
