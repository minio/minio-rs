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

//! MinIO Extension API for S3 Buckets: ListenBucketNotification

use crate::s3::builders::ListenBucketNotification;

use super::Client;

impl Client {
    /// Listens for bucket notifications. This is MinIO extension API. This
    /// function returns a tuple of `ListenBucketNotificationResponse` and a
    /// stream of `NotificationRecords`. The former contains the HTTP headers
    /// returned by the server and the latter is a stream of notification
    /// records. In normal operation (when there are no errors), the stream
    /// never ends.
    pub fn listen_bucket_notification(&self, bucket: &str) -> ListenBucketNotification {
        ListenBucketNotification::new(bucket).client(self)
    }
}
