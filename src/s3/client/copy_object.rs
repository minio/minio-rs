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

use crate::s3::builders::{
    ComposeObject, ComposeObjectBldr, ComposeObjectInternal, ComposeObjectInternalBldr,
    ComposeSource, CopyObject, CopyObjectBldr, CopyObjectInternal, CopyObjectInternalBldr,
    UploadPartCopy, UploadPartCopyBldr,
};
use crate::s3::client::MinioClient;

impl MinioClient {
    /// Creates a [`UploadPartCopy`] request builder.
    /// See [UploadPartCopy](https://docs.aws.amazon.com/AmazonS3/latest/API/API_UploadPartCopy.html) S3 API
    ///
    /// To execute the request, call [`UploadPartCopy::send()`](crate::s3::types::S3Api::send),
    /// which returns a [`Result`] containing a [`UploadPartCopyResponse`](crate::s3::response::UploadPartCopyResponse).    
    ///
    /// # Example
    ///
    /// ```no_run
    /// use minio::s3::MinioClient;
    /// use minio::s3::response::UploadPartCopyResponse;
    /// use minio::s3::segmented_bytes::SegmentedBytes;
    /// use minio::s3::types::S3Api;
    /// use minio::s3::response::a_response_traits::HasObject;
    ///
    /// #[tokio::main]
    /// async fn main() {    
    ///     let client = MinioClient::create_client_on_localhost().unwrap(); // configure your client here
    ///     let data1: SegmentedBytes = SegmentedBytes::from("aaaa".to_string());
    ///     todo!();
    ///     let resp: UploadPartCopyResponse = client
    ///         .upload_part_copy("bucket-name", "object-name", "TODO")
    ///         .build().send().await.unwrap();
    ///     println!("uploaded {}", resp.object());
    /// }
    /// ```
    pub fn upload_part_copy<S1: Into<String>, S2: Into<String>, S3: Into<String>>(
        &self,
        bucket: S1,
        object: S2,
        upload_id: S3,
    ) -> UploadPartCopyBldr {
        UploadPartCopy::builder()
            .client(self.clone())
            .bucket(bucket)
            .object(object)
            .upload_id(upload_id)
    }

    /// Create a CopyObject request builder. This is a lower-level API that
    /// performs a non-multipart object copy.
    pub(crate) fn copy_object_internal<S1: Into<String>, S2: Into<String>>(
        &self,
        bucket: S1,
        object: S2,
    ) -> CopyObjectInternalBldr {
        CopyObjectInternal::builder()
            .client(self.clone())
            .bucket(bucket)
            .object(object)
    }

    /// Create a CopyObject request builder.
    /// See [CopyObject](https://docs.aws.amazon.com/AmazonS3/latest/API/API_CopyObject.html) S3 API
    ///
    /// To execute the copy operation, call [`CopyObject::send()`](crate::s3::types::S3Api::send),
    /// which returns a [`Result`] containing a [`CopyObjectResponse`](crate::s3::response::CopyObjectResponse).
    ///
    /// The destination of the copy is specified via the `bucket` and `object` parameters of this function.
    /// To specify the source object to be copied, call `.source(...)` on the returned [`CopyObject`] builder.
    ///
    /// Internally, this function first performs a [`stat_object`](MinioClient::stat_object) call
    /// to retrieve metadata about the source object. It then constructs a
    /// [`compose_object`](MinioClient::compose_object) request to perform the actual copy.
    ///
    /// # Arguments
    ///
    /// - `bucket`: The name of the destination bucket.
    /// - `object`: The key (name) of the destination object.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use minio::s3::MinioClient;
    /// use minio::s3::response::CopyObjectResponse;
    /// use minio::s3::builders::CopySource;
    /// use minio::s3::types::S3Api;
    ///
    /// #[tokio::main]
    /// async fn main() {
    /// use minio::s3::response::a_response_traits::HasVersion;
    ///     let client = MinioClient::create_client_on_localhost().unwrap(); // configure your client here
    ///     let resp: CopyObjectResponse = client
    ///         .copy_object("bucket-name-dst", "object-name-dst")
    ///         .source(CopySource::builder().bucket("bucket-name-src").object("object-name-src").build())
    ///         .build().send().await.unwrap();
    ///     println!("copied the file from src to dst. New version: {:?}", resp.version_id());
    /// }
    /// ```
    pub fn copy_object<S1: Into<String>, S2: Into<String>>(
        &self,
        bucket: S1,
        object: S2,
    ) -> CopyObjectBldr {
        CopyObject::builder()
            .client(self.clone())
            .bucket(bucket)
            .object(object)
    }

    /// Create a ComposeObjectInternal request builder. This is a higher-level API that
    /// performs a multipart object compose.
    pub(crate) fn compose_object_internal<S1: Into<String>, S2: Into<String>>(
        &self,
        bucket: S1,
        object: S2,
    ) -> ComposeObjectInternalBldr {
        ComposeObjectInternal::builder()
            .client(self.clone())
            .bucket(bucket)
            .object(object)
    }

    /// compose object is higher-level API that calls an internal compose object, and if that call fails,
    /// it calls ['abort_multipart_upload`](MinioClient::abort_multipart_upload).
    pub fn compose_object<S1: Into<String>, S2: Into<String>>(
        &self,
        bucket: S1,
        object: S2,
        sources: Vec<ComposeSource>,
    ) -> ComposeObjectBldr {
        ComposeObject::builder()
            .client(self.clone())
            .bucket(bucket)
            .object(object)
            .sources(sources)
    }
}
