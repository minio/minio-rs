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

use super::{Client, DEFAULT_REGION};
use crate::s3::builders::GetRegion;
use crate::s3::error::Error;
use crate::s3::response::GetRegionResponse;
use crate::s3::types::S3Api;

use tokio::task;

impl Client {
    /// Creates a [`GetRegion`] request builder.
    ///
    /// To execute the request, call [`GetRegion::send()`](crate::s3::types::S3Api::send),
    /// which returns a [`Result`] containing a [`GetRegionResponse`].
    ///
    /// # Example
    ///
    /// ```no_run
    /// use minio::s3::Client;
    /// use minio::s3::response::GetRegionResponse;
    /// use minio::s3::types::S3Api;
    ///
    /// #[tokio::main]
    /// async fn main() {
    ///     let client: Client = Default::default(); // configure your client here
    ///     let resp: GetRegionResponse = client
    ///         .get_region("bucket-name")
    ///         .send().await.unwrap();
    ///     println!("retrieved region '{:?}' for bucket '{}'", resp.region_response, resp.bucket);
    /// }
    /// ```
    pub fn get_region<S: Into<String>>(&self, bucket: S) -> GetRegion {
        GetRegion::new(self.clone(), bucket.into())
    }

    /// Retrieves the region for the specified bucket name from the cache.
    /// If the region is not found in the cache, it is fetched via a call to S3 or MinIO
    /// and then stored in the cache for future lookups.
    pub async fn get_region_cached_async<S: Into<String>>(
        &self,
        bucket: S,
        region: &Option<String>, // the region as provided by the S3Request
    ) -> Result<String, Error> {
        // If a region is provided, validate it against the base_url region
        if let Some(requested_region) = region {
            if !self.shared.base_url.region.is_empty()
                && (self.shared.base_url.region != *requested_region)
            {
                return Err(Error::RegionMismatch(
                    self.shared.base_url.region.clone(),
                    requested_region.clone(),
                ));
            }
            return Ok(requested_region.clone());
        }

        // If base_url has a region set, use it
        if !self.shared.base_url.region.is_empty() {
            return Ok(self.shared.base_url.region.clone());
        }

        let bucket: String = bucket.into();
        // If no bucket or provider is configured, fall back to default
        if bucket.is_empty() || self.shared.provider.is_none() {
            return Ok(DEFAULT_REGION.to_owned());
        }

        // Return cached region if available
        if let Some(v) = self.shared.region_map.get(&bucket) {
            return Ok(v.value().clone());
        }

        // Otherwise, fetch the region and cache it
        let resp: GetRegionResponse = self.get_region(&bucket).send().await?;

        let resolved_region: String = if resp.region_response.is_empty() {
            DEFAULT_REGION.to_owned()
        } else {
            resp.region_response
        };

        self.shared
            .region_map
            .insert(bucket, resolved_region.clone());
        Ok(resolved_region)
    }

    /// Retrieves the region for the specified bucket name from the cache.
    /// If the region is not found in the cache, it is fetched via a call to S3 or MinIO
    /// and then stored in the cache for future lookups.
    pub fn get_region_cached(
        &self,
        bucket: &str,
        region: &Option<String>,
    ) -> Result<String, Error> {
        task::block_in_place(|| {
            tokio::runtime::Runtime::new()?.block_on(self.get_region_cached_async(bucket, region))
        })
    }
}
