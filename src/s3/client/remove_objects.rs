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

//! APIs to remove objects.

use crate::s3::{
    builders::{DeleteObjects, ObjectToDelete, RemoveObject, RemoveObjects},
    client::Client,
};

impl Client {
    pub fn remove_object(&self, bucket: &str, object: impl Into<ObjectToDelete>) -> RemoveObject {
        RemoveObject::new(bucket, object).client(self)
    }

    pub fn remove_objects(&self, bucket: &str, object: impl Into<DeleteObjects>) -> RemoveObjects {
        RemoveObjects::new(bucket, object).client(self)
    }
}
