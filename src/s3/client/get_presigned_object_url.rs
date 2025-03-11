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
use crate::s3::builders::GetPresignedObjectUrl;
use crate::s3::error::Error;
use crate::s3::response::GetPresignedObjectUrlResponse;
use http::Method;

impl Client {
    /// Create a GetPresignedObjectURL builder.
    pub fn get_presigned_object_url(
        &self,
        bucket: &str,
        object: &str,
        method: Method,
    ) -> Result<GetPresignedObjectUrlResponse, Error> {
        GetPresignedObjectUrl::new(self, bucket.to_string(), object.to_string(), method)?.compute()
    }
}
