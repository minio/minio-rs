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
use std::sync::Arc;

impl Client {
    /// Executes [UploadPartCopy](https://docs.aws.amazon.com/AmazonS3/latest/API/API_UploadPartCopy.html) S3 API
    pub fn upload_part_copy(self: &Arc<Self>, bucket: &str) -> UploadPartCopy {
        UploadPartCopy::new(self, bucket)
    }

    /// Create a CopyObject request builder. This is a lower-level API that
    /// performs a non-multipart object copy.
    pub fn copy_object_internal(self: &Arc<Self>, bucket: &str) -> CopyObjectInternal {
        CopyObjectInternal::new(self, bucket)
    }

    /// copy object is a high-order API that calls [`stat_object`] and based on the results calls
    /// either [`compose_object`] or [`copy_object_leaf`]  to copy the object.
    pub fn copy_object(self: &Arc<Self>, bucket: &str) -> CopyObject {
        CopyObject::new(self, bucket)
    }

    pub(crate) fn compose_object_internal(self: &Arc<Self>, bucket: &str) -> ComposeObjectInternal {
        ComposeObjectInternal::new(self, bucket)
    }

    /// compose object is high-order API that calls [`compose_object_internal`] and if that call fails,
    /// it calls ['abort_multipart_upload`].
    pub fn compose_object(
        self: &Arc<Self>,
        bucket_name: &str,
        object_name: &str,
        sources: Vec<ComposeSource>,
    ) -> ComposeObject {
        ComposeObject::new(self, bucket_name)
            .object(object_name.to_owned())
            .sources(sources)
    }
}
