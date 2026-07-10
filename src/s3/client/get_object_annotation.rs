// MinIO Rust Library for Amazon S3 Compatible Cloud Storage
// Copyright 2026 MinIO, Inc.
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

use crate::s3::builders::{GetObjectAnnotation, GetObjectAnnotationBldr};
use crate::s3::client::MinioClient;
use crate::s3::error::ValidationErr;
use crate::s3::types::{AnnotationName, BucketName, ObjectKey};

impl MinioClient {
    /// Creates a [`GetObjectAnnotation`] request builder that returns the
    /// payload of a single named annotation.
    ///
    /// To execute, call [`S3Api::send()`](crate::s3::types::S3Api::send), which
    /// returns a [`GetObjectAnnotationResponse`](crate::s3::response::GetObjectAnnotationResponse)
    /// whose `payload()` is the annotation bytes.
    ///
    /// 🛈 This is an AIStor extension and is not part of the S3 API.
    pub fn get_object_annotation<B, O, N>(
        &self,
        bucket: B,
        object: O,
        annotation_name: N,
    ) -> Result<GetObjectAnnotationBldr, ValidationErr>
    where
        B: TryInto<BucketName>,
        B::Error: Into<ValidationErr>,
        O: TryInto<ObjectKey>,
        O::Error: Into<ValidationErr>,
        N: TryInto<AnnotationName>,
        N::Error: Into<ValidationErr>,
    {
        Ok(GetObjectAnnotation::builder()
            .client(self.clone())
            .bucket(bucket.try_into().map_err(Into::into)?)
            .object(object.try_into().map_err(Into::into)?)
            .annotation_name(annotation_name.try_into().map_err(Into::into)?))
    }
}
