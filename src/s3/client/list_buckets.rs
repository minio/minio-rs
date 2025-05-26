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

use super::Client;
use crate::s3::builders::ListBuckets;

impl Client {
    /// Creates a [`ListBuckets`] request builder to retrieve the list of all buckets owned by the authenticated sender of the request.
    ///
    /// To execute the request, call [`ListBuckets::send()`](crate::s3::types::S3Api::send),
    /// which returns a [`Result`] containing a [`ListBucketsResponse`](crate::s3::response::ListBucketsResponse).
    ///
    /// For more information, refer to the [AWS S3 ListBuckets API documentation](https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListBuckets.html).
    ///
    /// # Example
    ///
    /// ```no_run
    /// use minio::s3::Client;
    /// use minio::s3::response::ListBucketsResponse;
    /// use minio::s3::types::S3Api;
    ///
    /// #[tokio::main]
    /// async fn main() {
    ///     let client: Client = Default::default(); // configure your client here
    ///     let resp: ListBucketsResponse = client
    ///         .list_buckets()
    ///         .send().await.unwrap();
    ///     println!("retrieved buckets '{:?}'", resp.buckets());
    /// }
    /// ```
    pub fn list_buckets(&self) -> ListBuckets {
        ListBuckets::new(self.clone())
    }
}
