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

use crate::s3::client::MinioClient;
use crate::s3::error::{Error, ValidationErr};
use crate::s3::multimap_ext::{Multimap, MultimapExt};
use crate::s3::response::ListenBucketNotificationResponse;
use crate::s3::types::NotificationRecords;
use crate::s3::types::{BucketName, Region, S3Api, S3Request, ToS3Request};
use async_trait::async_trait;
use futures_util::Stream;
use http::Method;
use typed_builder::TypedBuilder;

/// Argument builder for the [`ListenBucketNotification`](https://min.io/docs/minio/linux/developers/go/API.html#ListenBucketNotification)
///
/// This struct constructs the parameters required for the [`Client::listen_bucket_notification`](crate::s3::client::MinioClient::listen_bucket_notification) method.
#[derive(Clone, Debug, TypedBuilder)]
pub struct ListenBucketNotification {
    #[builder(!default)] // force required
    client: MinioClient,
    #[builder(default, setter(into))]
    extra_headers: Option<Multimap>,
    #[builder(default, setter(into))]
    extra_query_params: Option<Multimap>,
    #[builder(default, setter(into))]
    region: Option<Region>,
    #[builder(setter(into))] // force required + accept Into<String>
    #[builder(!default)]
    bucket: BucketName,
    #[builder(default, setter(into))]
    prefix: Option<String>,
    #[builder(default, setter(into))]
    suffix: Option<String>,
    #[builder(default, setter(into))]
    events: Option<Vec<String>>,
}

/// Builder type alias for [`ListenBucketNotification`].
///
/// Constructed via [`ListenBucketNotification::builder()`](ListenBucketNotification::builder) and used to build a [`ListenBucketNotification`] instance.
pub type ListenBucketNotificationBldr =
    ListenBucketNotificationBuilder<((MinioClient,), (), (), (), (BucketName,), (), (), ())>;

#[async_trait]
impl S3Api for ListenBucketNotification {
    type S3Response = (
        ListenBucketNotificationResponse,
        Box<dyn Stream<Item = Result<NotificationRecords, Error>> + Unpin + Send>,
    );
}

impl ToS3Request for ListenBucketNotification {
    fn to_s3request(self) -> Result<S3Request, ValidationErr> {
        {
            if self.client.is_aws_host() {
                return Err(ValidationErr::UnsupportedAwsApi(
                    "ListenBucketNotification".into(),
                ));
            }
        }

        let mut query_params: Multimap = self.extra_query_params.unwrap_or_default();
        {
            if let Some(v) = self.prefix {
                query_params.add("prefix", v);
            }
            if let Some(v) = self.suffix {
                query_params.add("suffix", v);
            }
            if let Some(v) = self.events {
                for e in v.into_iter() {
                    query_params.add("events", e);
                }
            } else {
                query_params.add("events", "s3:ObjectCreated:*");
                query_params.add("events", "s3:ObjectRemoved:*");
                query_params.add("events", "s3:ObjectAccessed:*");
            }
        }

        Ok(S3Request::builder()
            .client(self.client)
            .method(Method::GET)
            .region(self.region)
            .bucket(self.bucket)
            .query_params(query_params)
            .headers(self.extra_headers.unwrap_or_default())
            .build())
    }
}
