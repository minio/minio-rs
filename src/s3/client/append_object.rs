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

// ! S3 APIs for appending objects.

use super::Client;
use crate::s3::builders::ObjectContent;
use crate::s3::builders::{AppendObject, AppendObjectContent};
use crate::s3::segmented_bytes::SegmentedBytes;

impl Client {
    /// Creates an AppendObject request builder to append data to the end of an (existing) object.
    /// This is a lower-level API that performs a non-multipart object upload.
    ///
    /// 🛈 This operation is not supported for regular non-express buckets.
    pub fn append_object<S1: Into<String>, S2: Into<String>>(
        &self,
        bucket: S1,
        object: S2,
        data: SegmentedBytes,
        offset_bytes: u64,
    ) -> AppendObject {
        AppendObject::new(
            self.clone(),
            bucket.into(),
            object.into(),
            data,
            offset_bytes,
        )
    }

    /// Creates an AppendObjectContent request builder to append data to the end of an existing
    /// object. The content is streamed and appended to MinIO/S3. This is a higher-level API that
    /// handles multipart appends transparently.
    pub fn append_object_content<S1: Into<String>, S2: Into<String>, C: Into<ObjectContent>>(
        &self,
        bucket: S1,
        object: S2,
        content: C,
    ) -> AppendObjectContent {
        AppendObjectContent::new(self.clone(), bucket.into(), object.into(), content)
    }
}
