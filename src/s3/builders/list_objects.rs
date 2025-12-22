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

//! Argument builders for ListObject APIs.

use crate::s3::client::MinioClient;
use crate::s3::error::{Error, ValidationErr};
use crate::s3::multimap_ext::{Multimap, MultimapExt};
use crate::s3::response::ListObjectsResponse;
use crate::s3::response::list_objects::{
    ListObjectVersionsResponse, ListObjectsV1Response, ListObjectsV2Response,
};
use crate::s3::types::{BucketName, Region, S3Api, S3Request, ToS3Request, ToStream};
use crate::s3::utils::{check_bucket_name, insert};
use async_trait::async_trait;
use futures_util::{Stream, StreamExt, stream as futures_stream};
use http::Method;
use typed_builder::TypedBuilder;

fn add_common_list_objects_query_params(
    query_params: &mut Multimap,
    delimiter: Option<String>,
    disable_url_encoding: bool,
    max_keys: Option<u16>,
    prefix: Option<String>,
) {
    query_params.add("delimiter", delimiter.unwrap_or("".into()));
    query_params.add("max-keys", max_keys.unwrap_or(1000).to_string());
    query_params.add("prefix", prefix.unwrap_or("".into()));
    if !disable_url_encoding {
        query_params.add("encoding-type", "url");
    }
}

/// Helper function delimiter based on recursive flag when delimiter is not provided.
fn delim_helper(delim: Option<String>, recursive: bool) -> Option<String> {
    if delim.is_some() {
        return delim;
    }
    match recursive {
        true => None,
        false => Some(String::from("/")),
    }
}

// region: list-objects-v1

/// Argument builder for the [`ListObjectsV1`](https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListObjects.html) S3 API operation.
///
/// This struct constructs the parameters required for the [`Client::list_objects`](crate::s3::client::MinioClient::list_objects) method.
#[derive(Clone, Debug)]
struct ListObjectsV1 {
    client: MinioClient,

    extra_headers: Option<Multimap>,
    extra_query_params: Option<Multimap>,
    region: Option<Region>,
    bucket: BucketName,
    delimiter: Option<String>,
    disable_url_encoding: bool,
    max_keys: Option<u16>,
    prefix: Option<String>,
    marker: Option<String>,
}

#[async_trait]
impl ToStream for ListObjectsV1 {
    type Item = ListObjectsV1Response;

    async fn to_stream(self) -> Box<dyn Stream<Item = Result<Self::Item, Error>> + Unpin + Send> {
        Box::new(Box::pin(futures_stream::unfold(
            (self, false),
            move |(args, mut is_done)| async move {
                // Stop the stream if no more data is available
                if is_done {
                    return None;
                }
                // Prepare a clone of `args` for the next iteration
                let mut args_for_next_request: ListObjectsV1 = args.clone();

                // Handle the result of the API call
                match args.send().await {
                    Ok(resp) => {
                        // Update the marker for the next request
                        args_for_next_request.marker.clone_from(&resp.next_marker);

                        // Determine if there are more results to fetch
                        is_done = !resp.is_truncated;

                        // Return the response and prepare for the next iteration
                        Some((Ok(resp), (args_for_next_request, is_done)))
                    }
                    Err(e) => Some((Err(e), (args_for_next_request, true))),
                }
            },
        )))
    }
}

impl S3Api for ListObjectsV1 {
    type S3Response = ListObjectsV1Response;
}

impl ToS3Request for ListObjectsV1 {
    fn to_s3request(self) -> Result<S3Request, ValidationErr> {
        check_bucket_name(&self.bucket, true)?;

        let mut query_params: Multimap = self.extra_query_params.unwrap_or_default();
        {
            add_common_list_objects_query_params(
                &mut query_params,
                self.delimiter,
                self.disable_url_encoding,
                self.max_keys,
                self.prefix,
            );
            if let Some(v) = self.marker {
                query_params.add("marker", v);
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

impl From<ListObjects> for ListObjectsV1 {
    fn from(value: ListObjects) -> Self {
        ListObjectsV1 {
            client: value.client,
            extra_headers: value.extra_headers,
            extra_query_params: value.extra_query_params,
            region: value.region,
            bucket: value.bucket,
            delimiter: delim_helper(value.delimiter, value.recursive),
            disable_url_encoding: value.disable_url_encoding,
            max_keys: value.max_keys,
            prefix: value.prefix,
            marker: value.marker,
        }
    }
}
// endregion: list-objects-v1

// region: list-objects-v2

/// Argument builder for the [`ListObjectsV2`](https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListObjectsV2.html) S3 API operation.
///
/// This struct constructs the parameters required for the [`Client::list_objects`](crate::s3::client::MinioClient::list_objects) method.
#[derive(Clone, Debug)]
struct ListObjectsV2 {
    client: MinioClient,

    extra_headers: Option<Multimap>,
    extra_query_params: Option<Multimap>,
    region: Option<Region>,
    bucket: BucketName,
    delimiter: Option<String>,
    disable_url_encoding: bool,
    max_keys: Option<u16>,
    prefix: Option<String>,
    start_after: Option<String>,
    continuation_token: Option<String>,
    fetch_owner: bool,
    include_user_metadata: bool,
}

#[async_trait]
impl ToStream for ListObjectsV2 {
    type Item = ListObjectsV2Response;

    async fn to_stream(self) -> Box<dyn Stream<Item = Result<Self::Item, Error>> + Unpin + Send> {
        Box::new(Box::pin(futures_stream::unfold(
            (self, false),
            move |(args, mut is_done)| async move {
                // Stop the stream if no more data is available
                if is_done {
                    return None;
                }
                // Prepare a clone of `args` for the next iteration
                let mut args_for_next_request = args.clone();
                match args.send().await {
                    Ok(resp) => {
                        // Update the continuation_token for the next request
                        args_for_next_request
                            .continuation_token
                            .clone_from(&resp.next_continuation_token);

                        // Determine if there are more results to fetch
                        is_done = !resp.is_truncated;

                        // Return the response and prepare for the next iteration
                        Some((Ok(resp), (args_for_next_request, is_done)))
                    }
                    Err(e) => Some((Err(e), (args_for_next_request, true))),
                }
            },
        )))
    }
}

impl S3Api for ListObjectsV2 {
    type S3Response = ListObjectsV2Response;
}

impl ToS3Request for ListObjectsV2 {
    fn to_s3request(self) -> Result<S3Request, ValidationErr> {
        check_bucket_name(&self.bucket, true)?;

        let mut query_params: Multimap = self.extra_query_params.unwrap_or_default();
        {
            query_params.add("list-type", "2");
            add_common_list_objects_query_params(
                &mut query_params,
                self.delimiter,
                self.disable_url_encoding,
                self.max_keys,
                self.prefix,
            );
            if let Some(v) = self.continuation_token {
                query_params.add("continuation-token", v);
            }
            if self.fetch_owner {
                query_params.add("fetch-owner", "true");
            }
            if let Some(v) = self.start_after {
                query_params.add("start-after", v);
            }
            if self.include_user_metadata {
                query_params.add("metadata", "true");
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

impl From<ListObjects> for ListObjectsV2 {
    fn from(value: ListObjects) -> Self {
        ListObjectsV2 {
            client: value.client,
            extra_headers: value.extra_headers,
            extra_query_params: value.extra_query_params,
            region: value.region,
            bucket: value.bucket,
            delimiter: delim_helper(value.delimiter, value.recursive),
            disable_url_encoding: value.disable_url_encoding,
            max_keys: value.max_keys,
            prefix: value.prefix,
            start_after: value.start_after,
            continuation_token: value.continuation_token,
            fetch_owner: value.fetch_owner,
            include_user_metadata: value.include_user_metadata,
        }
    }
}
// endregion: list-objects-v2

// region: list-object-versions

/// Argument builder for the [`ListObjectVersions`](https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListObjectVersions.html) S3 API operation.
///
/// This struct constructs the parameters required for the [`Client::list_objects`](crate::s3::client::MinioClient::list_objects) method.
#[derive(Clone, Debug)]
struct ListObjectVersions {
    client: MinioClient,

    extra_headers: Option<Multimap>,
    extra_query_params: Option<Multimap>,
    region: Option<Region>,
    bucket: BucketName,
    delimiter: Option<String>,
    disable_url_encoding: bool,
    max_keys: Option<u16>,
    prefix: Option<String>,
    key_marker: Option<String>,
    version_id_marker: Option<String>,
    include_user_metadata: bool,
}

#[async_trait]
impl ToStream for ListObjectVersions {
    type Item = ListObjectVersionsResponse;

    async fn to_stream(self) -> Box<dyn Stream<Item = Result<Self::Item, Error>> + Unpin + Send> {
        Box::new(Box::pin(futures_stream::unfold(
            (self, false),
            move |(args, mut is_done)| async move {
                // Stop the stream if no more data is available
                if is_done {
                    return None;
                }
                // Prepare a clone of `args` for the next iteration
                let mut args_for_next_request = args.clone();
                match args.send().await {
                    Ok(resp) => {
                        // Update the key_marker for the next request
                        args_for_next_request
                            .key_marker
                            .clone_from(&resp.next_key_marker);
                        // Update the version_id_marker for the next request
                        args_for_next_request
                            .version_id_marker
                            .clone_from(&resp.next_version_id_marker);

                        // Determine if there are more results to fetch
                        is_done = !resp.is_truncated;

                        // Return the response and prepare for the next iteration
                        Some((Ok(resp), (args_for_next_request, is_done)))
                    }
                    Err(e) => Some((Err(e), (args_for_next_request, true))),
                }
            },
        )))
    }
}

impl S3Api for ListObjectVersions {
    type S3Response = ListObjectVersionsResponse;
}

impl ToS3Request for ListObjectVersions {
    fn to_s3request(self) -> Result<S3Request, ValidationErr> {
        check_bucket_name(&self.bucket, true)?;

        let mut query_params: Multimap = insert(self.extra_query_params, "versions");
        {
            add_common_list_objects_query_params(
                &mut query_params,
                self.delimiter,
                self.disable_url_encoding,
                self.max_keys,
                self.prefix,
            );
            if let Some(v) = self.key_marker {
                query_params.add("key-marker", v);
            }
            if let Some(v) = self.version_id_marker {
                query_params.add("version-id-marker", v);
            }
            if self.include_user_metadata {
                query_params.add("metadata", "true");
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

impl From<ListObjects> for ListObjectVersions {
    fn from(value: ListObjects) -> Self {
        Self {
            client: value.client,
            extra_headers: value.extra_headers,
            extra_query_params: value.extra_query_params,
            region: value.region,
            bucket: value.bucket,
            delimiter: delim_helper(value.delimiter, value.recursive),
            disable_url_encoding: value.disable_url_encoding,
            max_keys: value.max_keys,
            prefix: value.prefix,
            key_marker: value.key_marker,
            version_id_marker: value.version_id_marker,
            include_user_metadata: value.include_user_metadata,
        }
    }
}

// endregion: list-object-versions

// region: list-objects

/// Argument builder for
/// [list_objects()](crate::s3::client::MinioClient::list_objects) API.
///
/// Use the various builder methods to set parameters on the request. Finally, to
/// send the request and consume the results. Use the `ToStream` instance to get
/// a stream of results. Pagination is automatically performed.
#[derive(Clone, Debug, TypedBuilder)]
pub struct ListObjects {
    #[builder(!default)] // force required
    client: MinioClient,

    // Parameters common to all ListObjects APIs.
    #[builder(default, setter(into))]
    extra_headers: Option<Multimap>,
    #[builder(default, setter(into))]
    extra_query_params: Option<Multimap>,
    /// Sets the region for the request
    #[builder(default, setter(into))]
    region: Option<Region>,
    #[builder(setter(into))] // force required + accept Into<String>
    bucket: BucketName,

    /// Delimiter to roll up common prefixes on.
    #[builder(default, setter(into))]
    delimiter: Option<String>,
    /// Disable setting the `EncodingType` parameter in the ListObjects request.
    /// By default, it is set to `url`.
    #[builder(default)]
    disable_url_encoding: bool,
    #[builder(default, setter(into))]
    max_keys: Option<u16>,
    #[builder(default, setter(into))]
    prefix: Option<String>,

    // Options specific to ListObjectsV1.
    /// Used only with ListObjectsV1.
    #[builder(default, setter(into))]
    marker: Option<String>,

    // Options specific to ListObjectsV2.
    /// Used only with ListObjectsV2
    #[builder(default, setter(into))]
    start_after: Option<String>,

    /// Used only with ListObjectsV2
    #[builder(default, setter(into))]
    continuation_token: Option<String>,

    /// Used only with ListObjectsV2
    #[builder(default)]
    fetch_owner: bool,

    /// Used only with ListObjectsV2. MinIO extension.
    #[builder(default)]
    include_user_metadata: bool,

    // Options specific to ListObjectVersions.
    /// Used only with GetObjectVersions.
    #[builder(default, setter(into))]
    key_marker: Option<String>,

    /// Used only with GetObjectVersions.
    #[builder(default, setter(into))]
    version_id_marker: Option<String>,

    // Higher level options.
    /// This parameter takes effect only when delimiter is None. Enables
    /// recursive traversal for listing of the bucket and prefix.
    #[builder(default)]
    recursive: bool,

    /// Set this to use ListObjectsV1. Defaults to false.
    /// * For general purpose buckets, ListObjectsV2 returns objects in
    ///   lexicographical order based on their key names.
    /// * For directory buckets (S3-Express), ListObjectsV2 returns objects
    ///   in an unspecified order implementation-dependent order.
    #[builder(default)]
    use_api_v1: bool,

    /// Set this to include versions. Defaults to false. Has no effect when
    /// `use_api_v1` is set.
    #[builder(default)]
    include_versions: bool,
}

/// Builder type alias for [`ListObjects`].
///
/// Constructed via [`ListObjects::builder()`](ListObjects::builder) and used to build a [`ListObjects`] instance.
pub type ListObjectBldr = ListObjectsBuilder<(
    (MinioClient,),
    (),
    (),
    (),
    (BucketName,),
    (),
    (),
    (),
    (),
    (),
    (),
    (),
    (),
    (),
    (),
    (),
    (),
    (),
    (),
)>;

#[async_trait]
impl ToStream for ListObjects {
    type Item = ListObjectsResponse;

    async fn to_stream(self) -> Box<dyn Stream<Item = Result<Self::Item, Error>> + Unpin + Send> {
        if self.use_api_v1 {
            let stream = ListObjectsV1::from(self).to_stream().await;
            Box::new(stream.map(|v| v.map(|v| v.into())))
        } else if self.include_versions {
            let stream = ListObjectVersions::from(self).to_stream().await;
            Box::new(stream.map(|v| v.map(|v| v.into())))
        } else {
            let stream = ListObjectsV2::from(self).to_stream().await;
            Box::new(stream.map(|v| v.map(|v| v.into())))
        }
    }
}
// endregion: list-objects
