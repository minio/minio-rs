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

use crate::s3::builders::{PutObjectAnnotation, PutObjectAnnotationBldr};
use crate::s3::client::MinioClient;
use crate::s3::error::ValidationErr;
use crate::s3::types::{BucketName, ObjectKey};
use bytes::Bytes;

impl MinioClient {
    /// Creates a [`PutObjectAnnotation`] request builder to create or overwrite
    /// a named annotation (1 byte to 1 MiB) on an object. The parent object's
    /// data and ETag are never modified.
    ///
    /// To execute, call [`S3Api::send()`](crate::s3::types::S3Api::send), which
    /// returns a [`PutObjectAnnotationResponse`](crate::s3::response::PutObjectAnnotationResponse).
    ///
    /// 🛈 This is an AIStor extension and is not part of the S3 API.
    pub fn put_object_annotation<B, O, N, P>(
        &self,
        bucket: B,
        object: O,
        annotation_name: N,
        payload: P,
    ) -> Result<PutObjectAnnotationBldr, ValidationErr>
    where
        B: TryInto<BucketName>,
        B::Error: Into<ValidationErr>,
        O: TryInto<ObjectKey>,
        O::Error: Into<ValidationErr>,
        N: Into<String>,
        P: Into<Bytes>,
    {
        Ok(PutObjectAnnotation::builder()
            .client(self.clone())
            .bucket(bucket.try_into().map_err(Into::into)?)
            .object(object.try_into().map_err(Into::into)?)
            .annotation_name(annotation_name)
            .payload(payload))
    }
}
