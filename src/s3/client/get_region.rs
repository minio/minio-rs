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

use super::{DEFAULT_REGION, MinioClient};
use crate::s3::builders::{GetRegion, GetRegionBldr};
use crate::s3::error::{Error, ValidationErr};
use crate::s3::types::S3Api;

impl MinioClient {
    /// Creates a [`GetRegion`] request builder.
    ///
    /// To execute the request, call [`GetRegion::send()`](crate::s3::types::S3Api::send),
    /// which returns a [`Result`] containing a [`crate::s3::response::GetRegionResponse`].
    ///
    /// # Example
    ///
    /// ```no_run
    /// use minio::s3::MinioClient;
    /// use minio::s3::creds::StaticProvider;
    /// use minio::s3::http::BaseUrl;
    /// use minio::s3::response::GetRegionResponse;
    /// use minio::s3::types::S3Api;
    /// use minio::s3::response_traits::HasBucket;
    ///
    /// #[tokio::main]
    /// async fn main() {
    ///     let base_url = "http://localhost:9000/".parse::<BaseUrl>().unwrap();
    ///     let static_provider = StaticProvider::new("minioadmin", "minioadmin", None);
    ///     let client = MinioClient::new(base_url, Some(static_provider), None, None).unwrap();
    ///     let resp: GetRegionResponse = client
    ///         .get_region("bucket-name")
    ///         .build().send().await.unwrap();
    ///     println!("retrieved region '{:?}' for bucket '{}'", resp.region_response(), resp.bucket());
    /// }
    /// ```
    pub fn get_region<S: Into<String>>(&self, bucket: S) -> GetRegionBldr {
        GetRegion::builder().client(self.clone()).bucket(bucket)
    }

    /// Retrieves the region for the specified bucket name from the cache.
    /// If the region is not found in the cache, it is fetched via a call to S3 or MinIO
    /// and then stored in the cache for future lookups.
    ///
    /// If `skip_region_lookup` is enabled on the client, this method returns
    /// the default region immediately without making any network calls.
    pub async fn get_region_cached<S: Into<String>>(
        &self,
        bucket: S,
        region: &Option<String>, // the region as provided by the S3Request
    ) -> Result<String, Error> {
        // If skip_region_lookup is enabled (for MinIO servers), return default region immediately
        if self.shared.skip_region_lookup {
            return Ok(DEFAULT_REGION.to_owned());
        }

        // If a region is provided, validate it against the base_url region
        if let Some(requested_region) = region {
            if !self.shared.base_url.region.is_empty()
                && (self.shared.base_url.region != *requested_region)
            {
                return Err(ValidationErr::RegionMismatch {
                    bucket_region: self.shared.base_url.region.clone(),
                    region: requested_region.clone(),
                }
                .into());
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

        // Otherwise, fetch the region from the server and cache it
        let resolved_region: String = {
            let region = self
                .get_region(&bucket)
                .build()
                .send()
                .await?
                .region_response()?;
            if !region.is_empty() {
                region
            } else {
                DEFAULT_REGION.to_owned()
            }
        };

        self.shared
            .region_map
            .insert(bucket, resolved_region.clone());

        Ok(resolved_region)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::s3::client::MinioClientBuilder;
    use crate::s3::creds::StaticProvider;
    use crate::s3::http::BaseUrl;

    fn create_test_client(skip_region_lookup: bool) -> MinioClient {
        let base_url: BaseUrl = "http://localhost:9000".parse().unwrap();
        MinioClientBuilder::new(base_url)
            .provider(Some(StaticProvider::new("test", "test", None)))
            .skip_region_lookup(skip_region_lookup)
            .build()
            .unwrap()
    }

    #[tokio::test]
    async fn test_skip_region_lookup_returns_default_region() {
        let client = create_test_client(true);

        // With skip_region_lookup enabled, should return default region immediately
        let region = client.get_region_cached("any-bucket", &None).await.unwrap();

        assert_eq!(region, DEFAULT_REGION);
    }

    #[tokio::test]
    async fn test_skip_region_lookup_ignores_provided_region() {
        let client = create_test_client(true);

        // Even with a provided region, skip_region_lookup should return default
        let region = client
            .get_region_cached("any-bucket", &Some("eu-west-1".to_string()))
            .await
            .unwrap();

        // skip_region_lookup takes precedence and returns default region
        assert_eq!(region, DEFAULT_REGION);
    }

    #[tokio::test]
    async fn test_skip_region_lookup_multiple_calls_return_same_region() {
        let client = create_test_client(true);

        // Multiple calls should consistently return the default region
        for bucket in ["bucket1", "bucket2", "bucket3"] {
            let region = client.get_region_cached(bucket, &None).await.unwrap();
            assert_eq!(region, DEFAULT_REGION);
        }
    }

    #[tokio::test]
    async fn test_without_skip_region_lookup_uses_provided_region() {
        let client = create_test_client(false);

        // Without skip_region_lookup, provided region should be used
        let region = client
            .get_region_cached("any-bucket", &Some("eu-west-1".to_string()))
            .await
            .unwrap();

        assert_eq!(region, "eu-west-1");
    }

    #[tokio::test]
    async fn test_without_skip_region_lookup_empty_bucket_returns_default() {
        let client = create_test_client(false);

        // Empty bucket name should return default region
        let region = client.get_region_cached("", &None).await.unwrap();

        assert_eq!(region, DEFAULT_REGION);
    }

    #[test]
    fn test_skip_region_lookup_builder_default_is_false() {
        let base_url: BaseUrl = "http://localhost:9000".parse().unwrap();
        let client = MinioClientBuilder::new(base_url)
            .provider(Some(StaticProvider::new("test", "test", None)))
            .build()
            .unwrap();

        // Default should be false (region lookup enabled)
        assert!(!client.shared.skip_region_lookup);
    }

    #[test]
    fn test_skip_region_lookup_builder_can_be_enabled() {
        let base_url: BaseUrl = "http://localhost:9000".parse().unwrap();
        let client = MinioClientBuilder::new(base_url)
            .provider(Some(StaticProvider::new("test", "test", None)))
            .skip_region_lookup(true)
            .build()
            .unwrap();

        assert!(client.shared.skip_region_lookup);
    }

    #[test]
    fn test_skip_region_lookup_builder_can_be_toggled() {
        let base_url: BaseUrl = "http://localhost:9000".parse().unwrap();

        // Enable then disable
        let client = MinioClientBuilder::new(base_url)
            .provider(Some(StaticProvider::new("test", "test", None)))
            .skip_region_lookup(true)
            .skip_region_lookup(false)
            .build()
            .unwrap();

        assert!(!client.shared.skip_region_lookup);
    }
}
