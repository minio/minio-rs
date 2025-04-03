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
use crate::s3::builders::{ObjectToDelete, RemoveBucket, RemoveObject};
use crate::s3::error::Error;
use crate::s3::response::DeleteResult;
use crate::s3::response::{
    DisableObjectLegalHoldResponse, RemoveBucketResponse, RemoveObjectResponse,
    RemoveObjectsResponse,
};
use crate::s3::types::{S3Api, ToStream};
use futures::StreamExt;
use std::sync::Arc;

impl Client {
    /// Creates a [`RemoveBucket`] request builder.
    ///
    /// To execute the request, call [`RemoveBucket::send()`](crate::s3::types::S3Api::send),
    /// which returns a [`Result`] containing a [`RemoveBucketResponse`](crate::s3::response::RemoveBucketResponse).
    ///
    /// # Example
    ///
    /// ```no_run
    /// use minio::s3::Client;
    /// use minio::s3::response::RemoveBucketResponse;
    /// use minio::s3::types::S3Api;
    /// use std::sync::Arc;
    ///
    /// #[tokio::main]
    /// async fn main() {
    ///     let client: Arc<Client> = Arc::new(Default::default()); // configure your client here
    ///     let resp: RemoveBucketResponse =
    ///         client.remove_bucket("bucket-name").send().await.unwrap();
    ///     println!("bucket '{}' in region '{}' is removed", resp.bucket, resp.region);
    /// }
    /// ```
    pub fn remove_bucket(self: &Arc<Self>, bucket: &str) -> RemoveBucket {
        RemoveBucket::new(self, bucket.to_owned())
    }

    /// Removes a bucket and also removes non-empty buckets by first removing all objects before
    /// deleting the bucket. Bypasses governance mode and legal hold.
    pub async fn remove_and_purge_bucket(
        self: &Arc<Self>,
        bucket: &str,
    ) -> Result<RemoveBucketResponse, Error> {
        let mut stream = self
            .list_objects(bucket)
            .include_versions(true)
            .to_stream()
            .await;

        while let Some(items) = stream.next().await {
            let mut resp = self
                .remove_objects(
                    bucket,
                    items?.contents.into_iter().map(ObjectToDelete::from),
                )
                .bypass_governance_mode(true)
                .to_stream()
                .await;

            while let Some(item) = resp.next().await {
                let res: RemoveObjectsResponse = item?;
                for obj in res.result.into_iter() {
                    match obj {
                        DeleteResult::Deleted(_) => {}
                        DeleteResult::Error(v) => {
                            // the object is not deleted. try to disable legal hold and try again.
                            let _resp: DisableObjectLegalHoldResponse = self
                                .disable_object_legal_hold(bucket, &v.object_name)
                                .version_id(v.version_id.clone())
                                .send()
                                .await?;

                            let _resp: RemoveObjectResponse = RemoveObject::new(
                                self,
                                bucket.to_string(),
                                ObjectToDelete::from(v),
                            )
                            .bypass_governance_mode(true)
                            .send()
                            .await?;
                        }
                    }
                }
            }
        }
        self.remove_bucket(bucket).send().await
    }
}
