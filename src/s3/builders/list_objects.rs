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
use http::Method;

use crate::s3::{
    client::Client,
    error::Error,
    response::{
        ListObjectVersionsResponse, ListObjectsResponse, ListObjectsV1Response,
        ListObjectsV2Response,
    },
    types::{Paginated, S3Api, S3Request, ToS3Request},
    utils::{check_bucket_name, merge, Multimap},
};

fn add_common_list_objects_query_params(
    query_params: &mut Multimap,
    delimiter: Option<&str>,
    disable_url_encoding: bool,
    max_keys: Option<u16>,
    prefix: Option<&str>,
) {
    query_params.insert(
        String::from("delimiter"),
        delimiter.unwrap_or("").to_string(),
    );
    query_params.insert(
        String::from("max-keys"),
        max_keys.unwrap_or(1000).to_string(),
    );
    query_params.insert(String::from("prefix"), prefix.unwrap_or("").to_string());
    if !disable_url_encoding {
        query_params.insert(String::from("encoding-type"), String::from("url"));
    }
}

/// Argument builder for ListObjectsV1 S3 API, created by
/// [list_objects_v1()](crate::s3::client::Client::list_objects_v1).
#[derive(Clone, Debug, Default)]
pub struct ListObjectsV1 {
    client: Option<Client>,

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

impl ToS3Request for ListObjectsV1 {
    fn to_s3request<'a>(&'a self) -> Result<S3Request<'a>, Error> {
        check_bucket_name(&self.bucket, true)?;

        let mut headers = Multimap::new();
        if let Some(v) = &self.extra_headers {
            merge(&mut headers, v);
        }

        let mut query_params = Multimap::new();
        if let Some(v) = &self.extra_query_params {
            merge(&mut query_params, v);
        }

        add_common_list_objects_query_params(
            &mut query_params,
            self.delimiter.as_deref(),
            self.disable_url_encoding,
            self.max_keys,
            self.prefix.as_deref(),
        );
        if let Some(v) = &self.marker {
            query_params.insert(String::from("marker"), v.to_string());
        }

        let req = S3Request::new(
            self.client.as_ref().ok_or(Error::NoClientProvided)?,
            Method::GET,
        )
        .region(self.region.as_deref())
        .bucket(Some(&self.bucket))
        .query_params(query_params)
        .headers(headers);

        Ok(req)
    }
}

impl S3Api for ListObjectsV1 {
    type S3Response = ListObjectsV1Response;
}

// Helper function delimiter based on recursive flag when delimiter is not
// provided.
fn delim_helper(delim: Option<String>, recursive: bool) -> Option<String> {
    if delim.is_some() {
        return delim;
    }
    match recursive {
        true => None,
        false => Some(String::from("/")),
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

#[derive(Debug, Clone)]
pub struct ListObjectsV1Paginated {
    args: ListObjectsV1,
    is_done: bool,
}

#[async_trait]
impl Paginated for ListObjectsV1Paginated {
    type Item = ListObjectsV1Response;

    async fn next_page(&mut self, max_items: Option<u16>) -> Option<Result<Self::Item, Error>> {
        if self.is_done {
            return None;
        }
        self.args.max_keys = max_items.or(self.args.max_keys);
        let resp = self.args.send().await;
        if let Ok(ref resp) = resp {
            self.args.marker = resp.next_marker.clone();
            self.is_done = !resp.is_truncated;
        } else {
            self.is_done = true;
        }
        Some(resp)
    }
}

impl ListObjectsV1 {
    pub fn new(bucket: &str) -> Self {
        Self {
            bucket: bucket.to_owned(),
            ..Default::default()
        }
    }

    pub fn client(mut self, client: &Client) -> Self {
        self.client = Some(client.clone());
        self
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

    pub fn delimiter(mut self, delimiter: Option<String>) -> Self {
        self.delimiter = delimiter;
        self
    }

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

    pub fn marker(mut self, marker: Option<String>) -> Self {
        self.marker = marker;
        self
    }

    /// Enables pagination for ListObjectsV1 API by converting to a type that is
    /// an instance of `Paginated`.
    pub fn to_paginated(self) -> ListObjectsV1Paginated {
        ListObjectsV1Paginated {
            args: self,
            is_done: false,
        }
    }
}

/// Argument builder for ListObjectsV2 S3 API, created by
/// [list_objects_v2()](crate::s3::client::Client::list_objects_v2).
#[derive(Clone, Debug, Default)]
pub struct ListObjectsV2 {
    client: Option<Client>,

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

impl S3Api for ListObjectsV2 {
    type S3Response = ListObjectsV2Response;
}

impl ToS3Request for ListObjectsV2 {
    fn to_s3request(&self) -> Result<S3Request, Error> {
        check_bucket_name(&self.bucket, true)?;

        let mut headers = Multimap::new();
        if let Some(v) = &self.extra_headers {
            merge(&mut headers, v);
        }

        let mut query_params = Multimap::new();
        if let Some(v) = &self.extra_query_params {
            merge(&mut query_params, v);
        }
        query_params.insert(String::from("list-type"), String::from("2"));
        add_common_list_objects_query_params(
            &mut query_params,
            self.delimiter.as_deref(),
            self.disable_url_encoding,
            self.max_keys,
            self.prefix.as_deref(),
        );
        if let Some(v) = &self.continuation_token {
            query_params.insert(String::from("continuation-token"), v.to_string());
        }
        if self.fetch_owner {
            query_params.insert(String::from("fetch-owner"), String::from("true"));
        }
        if let Some(v) = &self.start_after {
            query_params.insert(String::from("start-after"), v.to_string());
        }
        if self.include_user_metadata {
            query_params.insert(String::from("metadata"), String::from("true"));
        }

        let req = S3Request::new(
            self.client.as_ref().ok_or(Error::NoClientProvided)?,
            Method::GET,
        )
        .region(self.region.as_deref())
        .bucket(Some(&self.bucket))
        .query_params(query_params)
        .headers(headers);
        Ok(req)
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

#[derive(Debug, Clone)]
pub struct ListObjectsV2Paginated {
    args: ListObjectsV2,
    is_done: bool,
}

#[async_trait]
impl Paginated for ListObjectsV2Paginated {
    type Item = ListObjectsV2Response;

    async fn next_page(&mut self, max_items: Option<u16>) -> Option<Result<Self::Item, Error>> {
        if self.is_done {
            return None;
        }
        self.args.max_keys = max_items.or(self.args.max_keys);
        let resp = self.args.send().await;
        if let Ok(ref resp) = resp {
            self.args.continuation_token = resp.next_continuation_token.clone();
            self.is_done = !resp.is_truncated;
        } else {
            self.is_done = true;
        }
        Some(resp)
    }
}

impl ListObjectsV2 {
    pub fn new(bucket: &str) -> Self {
        Self {
            bucket: bucket.to_owned(),
            ..Default::default()
        }
    }

    pub fn client(mut self, client: &Client) -> Self {
        self.client = Some(client.clone());
        self
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

    pub fn delimiter(mut self, delimiter: Option<String>) -> Self {
        self.delimiter = delimiter;
        self
    }

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

    pub fn start_after(mut self, start_after: Option<String>) -> Self {
        self.start_after = start_after;
        self
    }

    pub fn continuation_token(mut self, continuation_token: Option<String>) -> Self {
        self.continuation_token = continuation_token;
        self
    }

    pub fn fetch_owner(mut self, fetch_owner: bool) -> Self {
        self.fetch_owner = fetch_owner;
        self
    }

    pub fn include_user_metadata(mut self, include_user_metadata: bool) -> Self {
        self.include_user_metadata = include_user_metadata;
        self
    }

    pub fn to_paginated(self) -> ListObjectsV2Paginated {
        ListObjectsV2Paginated {
            args: self,
            is_done: false,
        }
    }
}

/// Argument builder for ListObjectVerions S3 API created by
/// [list_object_versions()](crate::s3::client::Client::list_object_versions).
#[derive(Clone, Debug, Default)]
pub struct ListObjectVersions {
    client: Option<Client>,

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

impl S3Api for ListObjectVersions {
    type S3Response = ListObjectVersionsResponse;
}

impl ToS3Request for ListObjectVersions {
    fn to_s3request(&self) -> Result<S3Request, Error> {
        check_bucket_name(&self.bucket, true)?;

        let mut headers = Multimap::new();
        if let Some(v) = &self.extra_headers {
            merge(&mut headers, v);
        }

        let mut query_params = Multimap::new();
        if let Some(v) = &self.extra_query_params {
            merge(&mut query_params, v);
        }
        query_params.insert(String::from("versions"), String::new());
        add_common_list_objects_query_params(
            &mut query_params,
            self.delimiter.as_deref(),
            self.disable_url_encoding,
            self.max_keys,
            self.prefix.as_deref(),
        );
        if let Some(v) = &self.key_marker {
            query_params.insert(String::from("key-marker"), v.to_string());
        }
        if let Some(v) = &self.version_id_marker {
            query_params.insert(String::from("version-id-marker"), v.to_string());
        }
        if self.include_user_metadata {
            query_params.insert(String::from("metadata"), String::from("true"));
        }

        let req = S3Request::new(
            self.client.as_ref().ok_or(Error::NoClientProvided)?,
            Method::GET,
        )
        .region(self.region.as_deref())
        .bucket(Some(&self.bucket))
        .query_params(query_params)
        .headers(headers);
        Ok(req)
    }
}

impl From<ListObjects> for ListObjectVersions {
    fn from(value: ListObjects) -> Self {
        ListObjectVersions {
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

#[derive(Debug, Clone)]
pub struct ListObjectVersionsPaginated {
    args: ListObjectVersions,
    is_done: bool,
}

#[async_trait]
impl Paginated for ListObjectVersionsPaginated {
    type Item = ListObjectVersionsResponse;

    async fn next_page(&mut self, max_items: Option<u16>) -> Option<Result<Self::Item, Error>> {
        if self.is_done {
            return None;
        }
        self.args.max_keys = max_items.or(self.args.max_keys);
        let resp = self.args.send().await;
        if let Ok(ref resp) = resp {
            self.args.key_marker = resp.next_key_marker.clone();
            self.args.version_id_marker = resp.next_version_id_marker.clone();
            self.is_done = !resp.is_truncated;
        } else {
            self.is_done = true;
        }
        Some(resp)
    }
}

impl ListObjectVersions {
    pub fn new(bucket: &str) -> Self {
        Self {
            bucket: bucket.to_owned(),
            ..Default::default()
        }
    }

    pub fn client(mut self, client: &Client) -> Self {
        self.client = Some(client.clone());
        self
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

    pub fn delimiter(mut self, delimiter: Option<String>) -> Self {
        self.delimiter = delimiter;
        self
    }

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

    pub fn key_marker(mut self, key_marker: Option<String>) -> Self {
        self.key_marker = key_marker;
        self
    }

    pub fn version_id_marker(mut self, version_id_marker: Option<String>) -> Self {
        self.version_id_marker = version_id_marker;
        self
    }

    pub fn include_user_metadata(mut self, include_user_metadata: bool) -> Self {
        self.include_user_metadata = include_user_metadata;
        self
    }

    pub fn to_paginated(self) -> ListObjectVersionsPaginated {
        ListObjectVersionsPaginated {
            args: self,
            is_done: false,
        }
    }
}

/// Argument builder for
/// [list_objects()](crate::s3::client::Client::list_objects) API.
///
/// Use the various builder methods to set parameters on the request. To send
/// the request, use [`.send()`](ListObjects::send), or to paginate
/// automatically, use [`.to_paginated()`](ListObjects::to_paginated).
#[derive(Clone, Debug, Default)]
pub struct ListObjects {
    client: Option<Client>,

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

/// Adds pagination to the [`ListObjects`] type.
#[derive(Debug, Clone)]
pub struct ListObjectsPaginated {
    args: ListObjects,
    is_done: bool,
}

#[async_trait]
impl Paginated for ListObjectsPaginated {
    type Item = ListObjectsResponse;

    async fn next_page(&mut self, max_items: Option<u16>) -> Option<Result<Self::Item, Error>> {
        if self.is_done {
            return None;
        }
        self.args.max_keys = max_items.or(self.args.max_keys);

        let resp = self.args.send().await;
        if let Ok(ref resp) = resp {
            self.args.marker = resp.next_marker.clone();
            self.args.continuation_token = resp.next_continuation_token.clone();
            self.args.key_marker = resp.next_key_marker.clone();
            self.args.version_id_marker = resp.next_version_id_marker.clone();
            self.is_done = !resp.is_truncated;
        } else {
            self.is_done = true;
        }
        Some(resp)
    }
}

impl ListObjects {
    pub fn new(bucket: &str) -> Self {
        Self {
            bucket: bucket.to_owned(),
            ..Default::default()
        }
    }

    pub fn client(mut self, client: &Client) -> Self {
        self.client = Some(client.clone());
        self
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
    pub fn use_api_v1(mut self, use_api_v1: bool) -> Self {
        self.use_api_v1 = use_api_v1;
        self
    }

    /// Set this to include versions.
    pub fn include_versions(mut self, include_versions: bool) -> Self {
        self.include_versions = include_versions;
        self
    }

    /// Send a single list objects request and return the response.
    pub async fn send(&self) -> Result<ListObjectsResponse, Error> {
        if self.use_api_v1 {
            let rsp = ListObjectsV1::from(self.clone()).send().await?;
            Ok(rsp.into())
        } else if self.include_versions {
            let rsp = ListObjectVersions::from(self.clone()).send().await?;
            Ok(rsp.into())
        } else {
            let rsp = ListObjectsV2::from(self.clone()).send().await?;
            Ok(rsp.into())
        }
    }

    /// Converts to a type that performs pagination automatically.
    pub fn to_paginated(self) -> ListObjectsPaginated {
        ListObjectsPaginated {
            args: self,
            is_done: false,
        }
    }
}
