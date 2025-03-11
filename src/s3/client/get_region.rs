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

//! S3 APIs for downloading objects.

use super::{Client, DEFAULT_REGION};
use crate::s3::builders::GetRegion;
use crate::s3::error::Error;
use crate::s3::types::S3Api;

impl Client {
    /// Create a GetRegion request builder.
    pub fn get_region(&self, bucket: &str, region: Option<&str>) -> GetRegion {
        GetRegion::new(bucket)
            .region(region.map(|s| s.to_string()))
            .client(self)
    }

    /// Retrieves the region for the specified bucket name from the cache.
    /// If the region is not found in the cache, it is fetched via a call to S3 or MinIO
    /// and then stored in the cache for future lookups.
    pub async fn get_region_cached(
        &self,
        bucket: &str,
        region: Option<&str>,
    ) -> Result<String, Error> {
        if let Some(region) = region.filter(|v| !v.is_empty()) {
            if !self.base_url.region.is_empty() && self.base_url.region != region {
                return Err(Error::RegionMismatch(
                    self.base_url.region.clone(),
                    region.to_string(),
                ));
            }
            return Ok(region.to_string());
        }

        if !self.base_url.region.is_empty() {
            return Ok(self.base_url.region.clone());
        }

        if bucket.is_empty() || self.provider.is_none() {
            return Ok(DEFAULT_REGION.to_string());
        }

        if let Some(v) = self.region_map.get(bucket) {
            return Ok(v.value().clone());
        }

        // Fallback: Fetch and cache the region
        let mut location = self
            .get_region(bucket, region)
            .send()
            .await?
            .region_response;
        if location.is_empty() {
            location = DEFAULT_REGION.to_string();
        }

        self.region_map.insert(bucket.to_string(), location.clone());
        Ok(location)
    }
}
