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
use crate::s3::builders::{ObjectToDelete, RemoveBucket};
use crate::s3::error::Error;
use crate::s3::response::DeleteResult;
use crate::s3::response::{
    DisableObjectLegalHoldResponse, RemoveBucketResponse, RemoveObjectResponse,
    RemoveObjectsResponse,
};
use crate::s3::types::{ListEntry, S3Api, ToStream};
use futures::StreamExt;

impl Client {
    /// Create a RemoveBucket request builder.
    pub fn remove_bucket(&self, bucket: &str) -> RemoveBucket {
        RemoveBucket::new(bucket).client(self)
    }

    /// Removes a bucket and also removes non-empty buckets by first removing all objects before
    /// deleting the bucket. Bypasses governance mode and legal hold.
    pub async fn remove_and_purge_bucket(
        &self,
        bucket_name: &str,
    ) -> Result<RemoveBucketResponse, Error> {
        let mut stream = self
            .list_objects(bucket_name)
            .include_versions(true)
            .to_stream()
            .await;

        while let Some(items) = stream.next().await {
            let items: Vec<ListEntry> = items?.contents;
            let mut to_delete: Vec<ObjectToDelete> = Vec::with_capacity(items.len());
            for item in items {
                to_delete.push(ObjectToDelete::from((
                    item.name.as_ref(),
                    item.version_id.as_deref(),
                )))
            }
            let mut resp = self
                .remove_objects(bucket_name, to_delete.into_iter())
                .bypass_governance_mode(true)
                .to_stream()
                .await;

            while let Some(item) = resp.next().await {
                let res: RemoveObjectsResponse = item?;
                for obj in res.result.iter() {
                    match obj {
                        DeleteResult::Deleted(_) => {}
                        DeleteResult::Error(v) => {
                            // the object is not deleted. try to disable legal hold and try again.
                            let _resp: DisableObjectLegalHoldResponse = self
                                .disable_object_legal_hold(bucket_name)
                                .object(v.object_name.clone())
                                .version_id(v.version_id.clone())
                                .send()
                                .await?;

                            let key: &str = &v.object_name;
                            let version: Option<&str> = v.version_id.as_deref();
                            let otd: ObjectToDelete = ObjectToDelete::from((key, version));
                            let _resp: RemoveObjectResponse = self
                                .remove_object(bucket_name, otd)
                                .bypass_governance_mode(true)
                                .send()
                                .await?;
                        }
                    }
                }
            }
        }
        self.remove_bucket(bucket_name).send().await
    }
}
