// MinIO Rust Library for Amazon S3 Compatible Cloud Storage
// Copyright 2023 MinIO, Inc.
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

//! S3 APIs for uploading objects.

use super::Client;
use crate::s3::{
    builders::{
        AbortMultipartUpload, CompleteMultipartUpload, CreateMultipartUpload, ObjectContent,
        PutObject, PutObjectContent, SegmentedBytes, UploadPart,
    },
    types::PartInfo,
};

impl Client {
    /// Create a PutObject request builder. This is a lower-level API that
    /// performs a non-multipart object upload.
    pub fn put_object(&self, bucket: &str, object: &str, data: SegmentedBytes) -> PutObject {
        PutObject::new(bucket, object, data).client(self)
    }

    /// Create a CreateMultipartUpload request builder.
    pub fn create_multipart_upload(&self, bucket: &str, object: &str) -> CreateMultipartUpload {
        CreateMultipartUpload::new(bucket, object).client(self)
    }

    pub fn abort_multipart_upload(
        &self,
        bucket: &str,
        object: &str,
        upload_id: &str,
    ) -> AbortMultipartUpload {
        AbortMultipartUpload::new(bucket, object, upload_id).client(self)
    }

    pub fn complete_multipart_upload(
        &self,
        bucket: &str,
        object: &str,
        upload_id: &str,
        parts: Vec<PartInfo>,
    ) -> CompleteMultipartUpload {
        CompleteMultipartUpload::new(bucket, object, upload_id, parts).client(self)
    }

    pub fn upload_part(
        &self,
        bucket: &str,
        object: &str,
        upload_id: &str,
        part_number: u16,
        data: SegmentedBytes,
    ) -> UploadPart {
        UploadPart::new(bucket, object, upload_id, part_number, data).client(self)
    }

    pub fn put_object_content(
        &self,
        bucket: &str,
        object: &str,
        content: impl Into<ObjectContent>,
    ) -> PutObjectContent {
        PutObjectContent::new(bucket, object, content).client(self)
    }
}
