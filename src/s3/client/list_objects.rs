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

//! S3 APIs for listing objects.

use super::Client;
use crate::s3::builders::{ListObjectVersions, ListObjects, ListObjectsV1, ListObjectsV2};

impl Client {
    /// Returns a builder type to list objects in a bucket using the older
    /// ListObjectsV1 API. This is a lower level API - prefer using the more
    /// powerful `list_objects` method instead.
    pub fn list_objects_v1(&self, bucket: &str) -> ListObjectsV1 {
        ListObjectsV1::new(bucket).client(self)
    }

    /// Returns a builder type to list objects in a bucket using the newer
    /// ListObjectsV2 API. This is a lower level API - prefer using the more
    /// powerful `list_objects` method instead.
    pub fn list_objects_v2(&self, bucket: &str) -> ListObjectsV2 {
        ListObjectsV2::new(bucket).client(self)
    }

    /// Returns a builder type to list object versions in a bucket. This is a
    /// lower level API - prefer using the more powerful `list_objects` method
    /// instead.
    pub fn list_object_versions(&self, bucket: &str) -> ListObjectVersions {
        ListObjectVersions::new(bucket).client(self)
    }

    /// List objects with version information optionally. This function returns
    /// the ListObjects builder type. This builder type has methods to set
    /// listing parameters and provides the `.execute_stream()` method to
    /// perform automatic pagination and return a stream of list "pages" as
    /// results.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// use minio::s3::client::{Client, ClientBuilder};
    /// use minio::s3::creds::StaticProvider;
    /// use minio::s3::http::BaseUrl;
    /// use minio::s3::types::ExecuteStream;
    /// use futures_util::StreamExt;
    ///
    /// #[tokio::main]
    /// async fn main() {
    ///     let base_url: BaseUrl = "play.min.io".parse().unwrap();
    ///     let static_provider = StaticProvider::new(
    ///         "Q3AM3UQ867SPQQA43P2F",
    ///         "zuf+tfteSlswRu7BJ86wekitnifILbZam1KYY3TG",
    ///         None,
    ///     );
    ///
    ///     let client = ClientBuilder::new(base_url)
    ///         .provider(Some(Box::new(static_provider)))
    ///         .build()
    ///         .unwrap();
    ///
    ///     // List all objects in a directory.
    ///     let mut list_objects = client
    ///         .list_objects("my-bucket")
    ///         .recursive(true)
    ///         .execute_stream()
    ///         .await;
    ///     while let Some(result) = list_objects.next().await {
    ///        match result {
    ///            Ok(resp) => {
    ///                for item in resp.contents {
    ///                    println!("{:?}", item);
    ///                }
    ///            }
    ///            Err(e) => println!("Error: {:?}", e),
    ///        }
    ///     }
    /// }
    pub fn list_objects(&self, bucket: &str) -> ListObjects {
        ListObjects::new(bucket).client(self)
    }
}
