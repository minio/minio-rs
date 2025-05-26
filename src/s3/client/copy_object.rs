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

//! S3 APIs for bucket objects.

use super::Client;
use crate::s3::builders::{
    ComposeObject, ComposeObjectInternal, ComposeSource, CopyObject, CopyObjectInternal,
    UploadPartCopy,
};

impl Client {
    /// Creates a [`UploadPartCopy`] request builder.
    /// See [UploadPartCopy](https://docs.aws.amazon.com/AmazonS3/latest/API/API_UploadPartCopy.html) S3 API
    ///
    /// To execute the request, call [`UploadPartCopy::send()`](crate::s3::types::S3Api::send),
    /// which returns a [`Result`] containing a [`UploadPartCopyResponse`](crate::s3::response::UploadPartCopyResponse).    
    ///
    /// # Example
    ///
    /// ```no_run
    /// use minio::s3::Client;
    /// use minio::s3::response::{UploadPartCopyResponse};
    /// use minio::s3::segmented_bytes::SegmentedBytes;
    /// use minio::s3::types::S3Api;
    ///
    /// #[tokio::main]
    /// async fn main() {    
    ///     let client: Client = Default::default(); // configure your client here
    ///     let data1: SegmentedBytes = SegmentedBytes::from("aaaa".to_string());
    ///     todo!();
    ///     let resp: UploadPartCopyResponse = client
    ///         .upload_part_copy("bucket-name", "object-name", "TODO")
    ///         .send().await.unwrap();
    ///     println!("uploaded {}", resp.object);
    /// }
    /// ```
    pub fn upload_part_copy<S1: Into<String>, S2: Into<String>, S3: Into<String>>(
        &self,
        bucket: S1,
        object: S2,
        upload_id: S3,
    ) -> UploadPartCopy {
        UploadPartCopy::new(self.clone(), bucket.into(), object.into(), upload_id.into())
    }

    /// Create a CopyObject request builder. This is a lower-level API that
    /// performs a non-multipart object copy.
    pub(crate) fn copy_object_internal<S1: Into<String>, S2: Into<String>>(
        &self,
        bucket: S1,
        object: S2,
    ) -> CopyObjectInternal {
        CopyObjectInternal::new(self.clone(), bucket.into(), object.into())
    }

    /// copy object is a high-order API that calls [stat_object](Client::stat_object) and based on the results calls
    /// [compose_object](Client::compose_object) to copy the object.
    pub fn copy_object<S1: Into<String>, S2: Into<String>>(
        &self,
        bucket: S1,
        object: S2,
    ) -> CopyObject {
        CopyObject::new(self.clone(), bucket.into(), object.into())
    }

    pub(crate) fn compose_object_internal<S1: Into<String>, S2: Into<String>>(
        &self,
        bucket: S1,
        object: S2,
    ) -> ComposeObjectInternal {
        ComposeObjectInternal::new(self.clone(), bucket.into(), object.into())
    }

    /// compose object is high-order API that calls an internal compose object, and if that call fails,
    /// it calls ['abort_multipart_upload`](Client::abort_multipart_upload).
    pub fn compose_object<S1: Into<String>, S2: Into<String>>(
        &self,
        bucket: S1,
        object: S2,
        sources: Vec<ComposeSource>,
    ) -> ComposeObject {
        ComposeObject::new(self.clone(), bucket.into(), object.into()).sources(sources)
    }
}
