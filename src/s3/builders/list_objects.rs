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

use async_trait::async_trait;
use futures_util::{Stream, StreamExt, stream as futures_stream};
use http::Method;

use crate::s3::multimap::{Multimap, MultimapExt};
use crate::s3::utils::insert;
use crate::s3::{
    client::Client,
    error::Error,
    response::ListObjectsResponse,
    response::list_objects::{
        ListObjectVersionsResponse, ListObjectsV1Response, ListObjectsV2Response,
    },
    types::{S3Api, S3Request, ToS3Request, ToStream},
    utils::check_bucket_name,
};

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

/// Argument for ListObjectsV1 S3 API.
#[derive(Clone, Debug, Default)]
struct ListObjectsV1 {
    client: Client,

    extra_headers: Option<Multimap>,
    extra_query_params: Option<Multimap>,
    region: Option<String>,
    bucket: String,
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
    fn to_s3request(self) -> Result<S3Request, Error> {
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

        Ok(S3Request::new(self.client, Method::GET)
            .region(self.region)
            .bucket(Some(self.bucket))
            .query_params(query_params)
            .headers(self.extra_headers.unwrap_or_default()))
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

/// Argument for ListObjectsV2 S3 API.
#[derive(Clone, Debug, Default)]
struct ListObjectsV2 {
    client: Client,

    extra_headers: Option<Multimap>,
    extra_query_params: Option<Multimap>,
    region: Option<String>,
    bucket: String,
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
    fn to_s3request(self) -> Result<S3Request, Error> {
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

        Ok(S3Request::new(self.client, Method::GET)
            .region(self.region)
            .bucket(Some(self.bucket))
            .query_params(query_params)
            .headers(self.extra_headers.unwrap_or_default()))
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

/// Argument for ListObjectVersions S3 API
#[derive(Clone, Debug, Default)]
struct ListObjectVersions {
    client: Client,

    extra_headers: Option<Multimap>,
    extra_query_params: Option<Multimap>,
    region: Option<String>,
    bucket: String,
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
    fn to_s3request(self) -> Result<S3Request, Error> {
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

        Ok(S3Request::new(self.client, Method::GET)
            .region(self.region)
            .bucket(Some(self.bucket))
            .query_params(query_params)
            .headers(self.extra_headers.unwrap_or_default()))
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
/// [list_objects()](crate::s3::client::Client::list_objects) API.
///
/// Use the various builder methods to set parameters on the request. Finally to
/// send the request and consume the results use the `ToStream` instance to get
/// a stream of results. Pagination is automatically performed.
#[derive(Clone, Debug, Default)]
pub struct ListObjects {
    client: Client,

    // Parameters common to all ListObjects APIs.
    extra_headers: Option<Multimap>,
    extra_query_params: Option<Multimap>,
    region: Option<String>,
    bucket: String,
    delimiter: Option<String>,
    disable_url_encoding: bool,
    max_keys: Option<u16>,
    prefix: Option<String>,

    // Options specific to ListObjectsV1.
    marker: Option<String>,

    // Options specific to ListObjectsV2.
    start_after: Option<String>,
    continuation_token: Option<String>,
    fetch_owner: bool,
    include_user_metadata: bool,

    // Options specific to ListObjectVersions.
    key_marker: Option<String>,
    version_id_marker: Option<String>,

    // Higher level options.
    recursive: bool,
    use_api_v1: bool,
    include_versions: bool,
}

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

impl ListObjects {
    pub fn new(client: Client, bucket: String) -> Self {
        Self {
            client,
            bucket,
            ..Default::default()
        }
    }
    pub fn extra_headers(mut self, extra_headers: Option<Multimap>) -> Self {
        self.extra_headers = extra_headers;
        self
    }

    pub fn extra_query_params(mut self, extra_query_params: Option<Multimap>) -> Self {
        self.extra_query_params = extra_query_params;
        self
    }

    pub fn region(mut self, region: Option<String>) -> Self {
        self.region = region;
        self
    }

    /// Delimiter to roll up common prefixes on.
    pub fn delimiter(mut self, delimiter: Option<String>) -> Self {
        self.delimiter = delimiter;
        self
    }

    /// Disable setting the `EncodingType` parameter in the ListObjects request.
    /// By default it is set to `url`.
    pub fn disable_url_encoding(mut self, disable_url_encoding: bool) -> Self {
        self.disable_url_encoding = disable_url_encoding;
        self
    }

    pub fn max_keys(mut self, max_keys: Option<u16>) -> Self {
        self.max_keys = max_keys;
        self
    }

    pub fn prefix(mut self, prefix: Option<String>) -> Self {
        self.prefix = prefix;
        self
    }

    /// Used only with ListObjectsV1.
    pub fn marker(mut self, marker: Option<String>) -> Self {
        self.marker = marker;
        self
    }

    /// Used only with ListObjectsV2
    pub fn start_after(mut self, start_after: Option<String>) -> Self {
        self.start_after = start_after;
        self
    }

    /// Used only with ListObjectsV2
    pub fn continuation_token(mut self, continuation_token: Option<String>) -> Self {
        self.continuation_token = continuation_token;
        self
    }

    /// Used only with ListObjectsV2
    pub fn fetch_owner(mut self, fetch_owner: bool) -> Self {
        self.fetch_owner = fetch_owner;
        self
    }

    /// Used only with ListObjectsV2. MinIO extension.
    pub fn include_user_metadata(mut self, include_user_metadata: bool) -> Self {
        self.include_user_metadata = include_user_metadata;
        self
    }

    /// Used only with GetObjectVersions.
    pub fn key_marker(mut self, key_marker: Option<String>) -> Self {
        self.key_marker = key_marker;
        self
    }

    /// Used only with GetObjectVersions.
    pub fn version_id_marker(mut self, version_id_marker: Option<String>) -> Self {
        self.version_id_marker = version_id_marker;
        self
    }

    /// This parameter takes effect only when delimiter is None. Enables
    /// recursive traversal for listing of the bucket and prefix.
    pub fn recursive(mut self, recursive: bool) -> Self {
        self.recursive = recursive;
        self
    }

    /// Set this to use ListObjectsV1. Defaults to false. 
    /// * For general purpose buckets, ListObjectsV2 returns objects in 
    /// lexicographical order based on their key names.
    /// * For directory buckets (S3-Express), ListObjectsV2 returns objects
    /// in an unspecified order.
    pub fn use_api_v1(mut self, use_api_v1: bool) -> Self {
        self.use_api_v1 = use_api_v1;
        self
    }

    /// Set this to include versions. Defaults to false. Has no effect when
    /// `use_api_v1` is set.
    pub fn include_versions(mut self, include_versions: bool) -> Self {
        self.include_versions = include_versions;
        self
    }
}
// endregion: list-objects
