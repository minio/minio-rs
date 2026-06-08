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

use crate::s3::builders::{
    GetBucketQOS, GetBucketQOSBldr, GetBucketQOSMetrics, GetBucketQOSMetricsBldr, SetBucketQOS,
    SetBucketQOSBldr,
};
use crate::s3::client::MinioClient;
use crate::s3::error::ValidationErr;
use crate::s3::types::BucketName;

impl MinioClient {
    /// Creates a [`GetBucketQOS`] request builder (MinIO extension).
    ///
    /// Retrieves the Quality of Service (QoS) configuration for a bucket.
    /// To execute the request, call [`GetBucketQOS::send()`](crate::s3::types::S3Api::send),
    /// which returns a [`Result`] containing a [`GetBucketQOSResponse`](crate::s3::response::GetBucketQOSResponse).
    ///
    /// See: <https://min.io/docs/minio/linux/developers/minio-drivers.html>
    ///
    /// # Example
    ///
    /// ```no_run
    /// use minio::s3::MinioClient;
    /// use minio::s3::creds::StaticProvider;
    /// use minio::s3::http::BaseUrl;
    /// use minio::s3::response::GetBucketQOSResponse;
    /// use minio::s3::types::S3Api;
    ///
    /// #[tokio::main]
    /// async fn main() {
    ///     let base_url = "http://localhost:9000/".parse::<BaseUrl>().unwrap();
    ///     let static_provider = StaticProvider::new("minioadmin", "minioadmin", None);
    ///     let client = MinioClient::new(base_url, Some(static_provider), None, None).unwrap();
    ///     let resp: GetBucketQOSResponse = client
    ///         .get_bucket_qos("bucket-name").unwrap()
    ///         .build().send().await.unwrap();
    ///     println!("QoS config: {:?}", resp.config());
    /// }
    /// ```
    pub fn get_bucket_qos<B>(&self, bucket: B) -> Result<GetBucketQOSBldr, ValidationErr>
    where
        B: TryInto<BucketName>,
        B::Error: Into<ValidationErr>,
    {
        Ok(GetBucketQOS::builder()
            .client(self.clone())
            .bucket(bucket.try_into().map_err(Into::into)?))
    }

    /// Creates a [`SetBucketQOS`] request builder (MinIO extension).
    ///
    /// Applies a Quality of Service (QoS) configuration to a bucket.
    /// To execute the request, call [`SetBucketQOS::send()`](crate::s3::types::S3Api::send),
    /// which returns a [`Result`] containing a [`SetBucketQOSResponse`](crate::s3::response::SetBucketQOSResponse).
    ///
    /// See: <https://min.io/docs/minio/linux/developers/minio-drivers.html>
    ///
    /// # Example
    ///
    /// ```no_run
    /// use minio::s3::MinioClient;
    /// use minio::s3::builders::QOSConfig;
    /// use minio::s3::creds::StaticProvider;
    /// use minio::s3::http::BaseUrl;
    /// use minio::s3::response::SetBucketQOSResponse;
    /// use minio::s3::types::S3Api;
    ///
    /// #[tokio::main]
    /// async fn main() {
    ///     let base_url = "http://localhost:9000/".parse::<BaseUrl>().unwrap();
    ///     let static_provider = StaticProvider::new("minioadmin", "minioadmin", None);
    ///     let client = MinioClient::new(base_url, Some(static_provider), None, None).unwrap();
    ///     let _resp: SetBucketQOSResponse = client
    ///         .set_bucket_qos("bucket-name").unwrap()
    ///         .qos_config(QOSConfig::new())
    ///         .build().send().await.unwrap();
    /// }
    /// ```
    pub fn set_bucket_qos<B>(&self, bucket: B) -> Result<SetBucketQOSBldr, ValidationErr>
    where
        B: TryInto<BucketName>,
        B::Error: Into<ValidationErr>,
    {
        Ok(SetBucketQOS::builder()
            .client(self.clone())
            .bucket(bucket.try_into().map_err(Into::into)?))
    }

    /// Creates a [`GetBucketQOSMetrics`] request builder (MinIO extension).
    ///
    /// Retrieves Quality of Service (QoS) metrics for a bucket, optionally
    /// scoped to a single node via [`GetBucketQOSMetrics::node`].
    /// To execute the request, call [`GetBucketQOSMetrics::send()`](crate::s3::types::S3Api::send),
    /// which returns a [`Result`] containing a [`GetBucketQOSMetricsResponse`](crate::s3::response::GetBucketQOSMetricsResponse).
    ///
    /// See: <https://min.io/docs/minio/linux/developers/minio-drivers.html>
    ///
    /// # Example
    ///
    /// ```no_run
    /// use minio::s3::MinioClient;
    /// use minio::s3::creds::StaticProvider;
    /// use minio::s3::http::BaseUrl;
    /// use minio::s3::response::GetBucketQOSMetricsResponse;
    /// use minio::s3::types::S3Api;
    ///
    /// #[tokio::main]
    /// async fn main() {
    ///     let base_url = "http://localhost:9000/".parse::<BaseUrl>().unwrap();
    ///     let static_provider = StaticProvider::new("minioadmin", "minioadmin", None);
    ///     let client = MinioClient::new(base_url, Some(static_provider), None, None).unwrap();
    ///     let resp: GetBucketQOSMetricsResponse = client
    ///         .get_bucket_qos_metrics("bucket-name").unwrap()
    ///         .build().send().await.unwrap();
    ///     println!("QoS metrics: {:?}", resp.metrics());
    /// }
    /// ```
    pub fn get_bucket_qos_metrics<B>(
        &self,
        bucket: B,
    ) -> Result<GetBucketQOSMetricsBldr, ValidationErr>
    where
        B: TryInto<BucketName>,
        B::Error: Into<ValidationErr>,
    {
        Ok(GetBucketQOSMetrics::builder()
            .client(self.clone())
            .bucket(bucket.try_into().map_err(Into::into)?))
    }
}
