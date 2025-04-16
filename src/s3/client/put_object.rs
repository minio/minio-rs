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
use crate::s3::segmented_bytes::SegmentedBytes;
use crate::s3::{
    builders::{
        AbortMultipartUpload, CompleteMultipartUpload, CreateMultipartUpload, ObjectContent,
        PutObject, PutObjectContent, UploadPart,
    },
    types::PartInfo,
};

impl Client {
    /// Creates a [`PutObject`] request builder. This is a lower-level API that
    /// performs a non-multipart object upload.
    ///
    /// To execute the request, call [`PutObject::send()`](crate::s3::types::S3Api::send),
    /// which returns a [`Result`] containing a [`PutObjectResponse`](crate::s3::response::PutObjectResponse).
    ///
    /// # Example
    ///
    /// ```no_run
    /// use minio::s3::Client;
    /// use minio::s3::response::PutObjectResponse;
    /// use minio::s3::types::S3Api;
    /// use minio::s3::segmented_bytes::SegmentedBytes;
    ///
    ///
    /// #[tokio::main]
    /// async fn main() {
    ///     let client: Client = Default::default(); // configure your client here
    ///     let data = SegmentedBytes::from("Hello world".to_string());
    ///     let resp: PutObjectResponse =
    ///         client.put_object("bucket-name", "object-name", data).send().await.unwrap();
    ///     println!("successfully put object '{}'", resp.object);
    /// }
    /// ```
    pub fn put_object<S1: Into<String>, S2: Into<String>>(
        &self,
        bucket: S1,
        object: S2,
        data: SegmentedBytes,
    ) -> PutObject {
        PutObject::new(self.clone(), bucket.into(), object.into(), data)
    }

    /// Create a CreateMultipartUpload request builder.
    pub fn create_multipart_upload<S1: Into<String>, S2: Into<String>>(
        &self,
        bucket: S1,
        object: S2,
    ) -> CreateMultipartUpload {
        CreateMultipartUpload::new(self.clone(), bucket.into(), object.into())
    }

    pub fn abort_multipart_upload<S1: Into<String>, S2: Into<String>, S3: Into<String>>(
        &self,
        bucket: S1,
        object: S2,
        upload_id: S3,
    ) -> AbortMultipartUpload {
        AbortMultipartUpload::new(self.clone(), bucket.into(), object.into(), upload_id.into())
    }

    pub fn complete_multipart_upload<S1: Into<String>, S2: Into<String>, S3: Into<String>>(
        &self,
        bucket: S1,
        object: S2,
        upload_id: S3,
        parts: Vec<PartInfo>,
    ) -> CompleteMultipartUpload {
        CompleteMultipartUpload::new(
            self.clone(),
            bucket.into(),
            object.into(),
            upload_id.into(),
            parts,
        )
    }

    pub fn upload_part<S1: Into<String>, S2: Into<String>, S3: Into<String>>(
        &self,
        bucket: S1,
        object: S2,
        upload_id: S3,
        part_number: u16,
        data: SegmentedBytes,
    ) -> UploadPart {
        UploadPart::new(
            self.clone(),
            bucket.into(),
            object.into(),
            upload_id.into(),
            part_number,
            data,
        )
    }

    /// Creates a PutObjectContent request builder to upload data to MinIO/S3.
    /// The content is streamed, and this higher-level API handles multipart uploads transparently.
    pub fn put_object_content<S: Into<String>, C: Into<ObjectContent>>(
        &self,
        bucket: S,
        object: S,
        content: C,
    ) -> PutObjectContent {
        PutObjectContent::new(self.clone(), bucket.into(), object.into(), content)
    }
}
