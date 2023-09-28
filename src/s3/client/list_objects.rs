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

//! S3 API: ListObjectsV2, ListObjectsV1, ListObjectVersions and streaming helper.

use std::collections::HashMap;

use super::Client;
use crate::s3::{
    args::{ListObjectVersionsArgs, ListObjectsArgs, ListObjectsV1Args, ListObjectsV2Args},
    error::Error,
    response::{ListObjectVersionsResponse, ListObjectsV1Response, ListObjectsV2Response},
    types::ListEntry,
    utils::{
        from_iso8601utc, get_default_text, get_option_text, get_text, merge, urldecode, Multimap,
    },
};

use bytes::Buf;
use futures_util::{stream as futures_stream, Stream, StreamExt};
use http::Method;
use xmltree::Element;

fn url_decode(
    encoding_type: &Option<String>,
    prefix: Option<String>,
) -> Result<Option<String>, Error> {
    if let Some(v) = encoding_type.as_ref() {
        if v == "url" {
            if let Some(v) = prefix {
                return Ok(Some(urldecode(&v)?.to_string()));
            }
        }
    }

    if let Some(v) = prefix.as_ref() {
        return Ok(Some(v.to_string()));
    }

    Ok(None)
}

fn add_common_list_objects_query_params(
    query_params: &mut Multimap,
    delimiter: Option<&str>,
    use_url_encoding_type: bool,
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
    if use_url_encoding_type {
        query_params.insert(String::from("encoding-type"), String::from("url"));
    }
}

fn parse_common_list_objects_response(
    root: &Element,
) -> Result<
    (
        String,
        Option<String>,
        Option<String>,
        Option<String>,
        bool,
        Option<u16>,
    ),
    Error,
> {
    let encoding_type = get_option_text(root, "EncodingType");
    let prefix = url_decode(&encoding_type, Some(get_default_text(root, "Prefix")))?;
    Ok((
        get_text(root, "Name")?,
        encoding_type,
        prefix,
        get_option_text(root, "Delimiter"),
        match get_option_text(root, "IsTruncated") {
            Some(v) => v.to_lowercase() == "true",
            None => false,
        },
        match get_option_text(root, "MaxKeys") {
            Some(v) => Some(v.parse::<u16>()?),
            None => None,
        },
    ))
}

fn parse_list_objects_contents(
    contents: &mut Vec<ListEntry>,
    root: &mut xmltree::Element,
    tag: &str,
    encoding_type: &Option<String>,
    is_delete_marker: bool,
) -> Result<(), Error> {
    while let Some(v) = root.take_child(tag) {
        let content = v;
        let etype = encoding_type.as_ref().cloned();
        let key = url_decode(&etype, Some(get_text(&content, "Key")?))?.unwrap();
        let last_modified = Some(from_iso8601utc(&get_text(&content, "LastModified")?)?);
        let etag = get_option_text(&content, "ETag");
        let v = get_default_text(&content, "Size");
        let size = match v.is_empty() {
            true => None,
            false => Some(v.parse::<usize>()?),
        };
        let storage_class = get_option_text(&content, "StorageClass");
        let is_latest = get_default_text(&content, "IsLatest").to_lowercase() == "true";
        let version_id = get_option_text(&content, "VersionId");
        let (owner_id, owner_name) = match content.get_child("Owner") {
            Some(v) => (get_option_text(v, "ID"), get_option_text(v, "DisplayName")),
            None => (None, None),
        };
        let user_metadata = match content.get_child("UserMetadata") {
            Some(v) => {
                let mut map: HashMap<String, String> = HashMap::new();
                for xml_node in &v.children {
                    let u = xml_node
                        .as_element()
                        .ok_or(Error::XmlError("unable to convert to element".to_string()))?;
                    map.insert(
                        u.name.to_string(),
                        u.get_text().unwrap_or_default().to_string(),
                    );
                }
                Some(map)
            }
            None => None,
        };

        contents.push(ListEntry {
            name: key,
            last_modified,
            etag,
            owner_id,
            owner_name,
            size,
            storage_class,
            is_latest,
            version_id,
            user_metadata,
            is_prefix: false,
            is_delete_marker,
            encoding_type: etype,
        });
    }

    Ok(())
}

fn parse_list_objects_common_prefixes(
    contents: &mut Vec<ListEntry>,
    root: &mut Element,
    encoding_type: &Option<String>,
) -> Result<(), Error> {
    while let Some(v) = root.take_child("CommonPrefixes") {
        let common_prefix = v;
        contents.push(ListEntry {
            name: url_decode(encoding_type, Some(get_text(&common_prefix, "Prefix")?))?.unwrap(),
            last_modified: None,
            etag: None,
            owner_id: None,
            owner_name: None,
            size: None,
            storage_class: None,
            is_latest: false,
            version_id: None,
            user_metadata: None,
            is_prefix: true,
            is_delete_marker: false,
            encoding_type: encoding_type.as_ref().cloned(),
        });
    }

    Ok(())
}

impl Client {
    /// Executes [ListObjects](https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListObjects.html) S3 API
    pub async fn list_objects_v1(
        &self,
        args: &ListObjectsV1Args,
    ) -> Result<ListObjectsV1Response, Error> {
        let region = self
            .get_region(&args.bucket, args.region.as_deref())
            .await?;

        let mut headers = Multimap::new();
        if let Some(v) = &args.extra_headers {
            merge(&mut headers, v);
        }

        let mut query_params = Multimap::new();
        if let Some(v) = &args.extra_query_params {
            merge(&mut query_params, v);
        }
        add_common_list_objects_query_params(
            &mut query_params,
            args.delimiter.as_deref(),
            args.use_url_encoding_type,
            args.max_keys,
            args.prefix.as_deref(),
        );
        if let Some(v) = &args.marker {
            query_params.insert(String::from("marker"), v.to_string());
        }

        let resp = self
            .execute(
                Method::GET,
                &region,
                &mut headers,
                &query_params,
                Some(&args.bucket),
                None,
                None,
            )
            .await?;
        let header_map = resp.headers().clone();
        let body = resp.bytes().await?;
        let mut root = Element::parse(body.reader())?;

        let (name, encoding_type, prefix, delimiter, is_truncated, max_keys) =
            parse_common_list_objects_response(&root)?;
        let marker = url_decode(&encoding_type, get_option_text(&root, "Marker"))?;
        let mut next_marker = url_decode(&encoding_type, get_option_text(&root, "NextMarker"))?;
        let mut contents: Vec<ListEntry> = Vec::new();
        parse_list_objects_contents(&mut contents, &mut root, "Contents", &encoding_type, false)?;
        if is_truncated && next_marker.is_none() {
            next_marker = contents.last().map(|v| v.name.clone())
        }
        parse_list_objects_common_prefixes(&mut contents, &mut root, &encoding_type)?;

        Ok(ListObjectsV1Response {
            headers: header_map,
            name,
            encoding_type,
            prefix,
            delimiter,
            is_truncated,
            max_keys,
            contents,
            marker,
            next_marker,
        })
    }

    /// Executes [ListObjectsV2](https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListObjectsV2.html) S3 API
    pub async fn list_objects_v2(
        &self,
        args: &ListObjectsV2Args,
    ) -> Result<ListObjectsV2Response, Error> {
        let region = self
            .get_region(&args.bucket, args.region.as_deref())
            .await?;

        let mut headers = Multimap::new();
        if let Some(v) = &args.extra_headers {
            merge(&mut headers, v);
        }

        let mut query_params = Multimap::new();
        if let Some(v) = &args.extra_query_params {
            merge(&mut query_params, v);
        }
        query_params.insert(String::from("list-type"), String::from("2"));
        add_common_list_objects_query_params(
            &mut query_params,
            args.delimiter.as_deref(),
            args.use_url_encoding_type,
            args.max_keys,
            args.prefix.as_deref(),
        );
        if let Some(v) = &args.continuation_token {
            query_params.insert(String::from("continuation-token"), v.to_string());
        }
        if args.fetch_owner {
            query_params.insert(String::from("fetch-owner"), String::from("true"));
        }
        if let Some(v) = &args.start_after {
            query_params.insert(String::from("start-after"), v.to_string());
        }
        if args.include_user_metadata {
            query_params.insert(String::from("metadata"), String::from("true"));
        }

        let resp = self
            .execute(
                Method::GET,
                &region,
                &mut headers,
                &query_params,
                Some(&args.bucket),
                None,
                None,
            )
            .await?;
        let header_map = resp.headers().clone();
        let body = resp.bytes().await?;
        let mut root = Element::parse(body.reader())?;

        let (name, encoding_type, prefix, delimiter, is_truncated, max_keys) =
            parse_common_list_objects_response(&root)?;
        let text = get_option_text(&root, "KeyCount");
        let key_count = match text {
            Some(v) => match v.is_empty() {
                true => None,
                false => Some(v.parse::<u16>()?),
            },
            None => None,
        };
        let start_after = url_decode(&encoding_type, get_option_text(&root, "StartAfter"))?;
        let continuation_token = get_option_text(&root, "ContinuationToken");
        let next_continuation_token = get_option_text(&root, "NextContinuationToken");
        let mut contents: Vec<ListEntry> = Vec::new();
        parse_list_objects_contents(&mut contents, &mut root, "Contents", &encoding_type, false)?;
        parse_list_objects_common_prefixes(&mut contents, &mut root, &encoding_type)?;

        Ok(ListObjectsV2Response {
            headers: header_map,
            name,
            encoding_type,
            prefix,
            delimiter,
            is_truncated,
            max_keys,
            contents,
            key_count,
            start_after,
            continuation_token,
            next_continuation_token,
        })
    }

    /// Executes [ListObjectVersions](https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListObjectVersions.html) S3 API
    pub async fn list_object_versions(
        &self,
        args: &ListObjectVersionsArgs,
    ) -> Result<ListObjectVersionsResponse, Error> {
        let region = self
            .get_region(&args.bucket, args.region.as_deref())
            .await?;

        let mut headers = Multimap::new();
        if let Some(v) = &args.extra_headers {
            merge(&mut headers, v);
        }

        let mut query_params = Multimap::new();
        if let Some(v) = &args.extra_query_params {
            merge(&mut query_params, v);
        }
        query_params.insert(String::from("versions"), String::new());
        add_common_list_objects_query_params(
            &mut query_params,
            args.delimiter.as_deref(),
            args.use_url_encoding_type,
            args.max_keys,
            args.prefix.as_deref(),
        );
        if let Some(v) = &args.key_marker {
            query_params.insert(String::from("key-marker"), v.to_string());
        }
        if let Some(v) = &args.version_id_marker {
            query_params.insert(String::from("version-id-marker"), v.to_string());
        }

        let resp = self
            .execute(
                Method::GET,
                &region,
                &mut headers,
                &query_params,
                Some(&args.bucket),
                None,
                None,
            )
            .await?;
        let header_map = resp.headers().clone();
        let body = resp.bytes().await?;
        let mut root = Element::parse(body.reader())?;

        let (name, encoding_type, prefix, delimiter, is_truncated, max_keys) =
            parse_common_list_objects_response(&root)?;
        let key_marker = url_decode(&encoding_type, get_option_text(&root, "KeyMarker"))?;
        let next_key_marker = url_decode(&encoding_type, get_option_text(&root, "NextKeyMarker"))?;
        let version_id_marker = get_option_text(&root, "VersionIdMarker");
        let next_version_id_marker = get_option_text(&root, "NextVersionIdMarker");
        let mut contents: Vec<ListEntry> = Vec::new();
        parse_list_objects_contents(&mut contents, &mut root, "Version", &encoding_type, false)?;
        parse_list_objects_common_prefixes(&mut contents, &mut root, &encoding_type)?;
        parse_list_objects_contents(
            &mut contents,
            &mut root,
            "DeleteMarker",
            &encoding_type,
            true,
        )?;

        Ok(ListObjectVersionsResponse {
            headers: header_map,
            name,
            encoding_type,
            prefix,
            delimiter,
            is_truncated,
            max_keys,
            contents,
            key_marker,
            next_key_marker,
            version_id_marker,
            next_version_id_marker,
        })
    }

    async fn list_objects_v1_stream(
        &self,
        args: ListObjectsV1Args,
    ) -> impl Stream<Item = Result<ListObjectsV1Response, Error>> + Unpin {
        Box::pin(futures_stream::unfold(
            (self.clone(), args),
            move |(client, mut args)| async move {
                let resp = client.list_objects_v1(&args).await;
                match resp {
                    Ok(resp) => {
                        if !resp.is_truncated {
                            None
                        } else {
                            args.marker = resp.next_marker.clone();
                            Some((Ok(resp), (client, args)))
                        }
                    }
                    Err(e) => Some((Err(e), (client, args))),
                }
            },
        ))
    }

    async fn list_objects_v2_stream(
        &self,
        args: ListObjectsV2Args,
    ) -> impl Stream<Item = Result<ListObjectsV2Response, Error>> + Unpin {
        Box::pin(futures_stream::unfold(
            (self.clone(), args),
            move |(client, mut args)| async move {
                let resp = client.list_objects_v2(&args).await;
                match resp {
                    Ok(resp) => {
                        if !resp.is_truncated {
                            None
                        } else {
                            args.continuation_token = resp.next_continuation_token.clone();
                            Some((Ok(resp), (client, args)))
                        }
                    }
                    Err(e) => Some((Err(e), (client, args))),
                }
            },
        ))
    }

    async fn list_object_versions_stream(
        &self,
        args: ListObjectVersionsArgs,
    ) -> impl Stream<Item = Result<ListObjectVersionsResponse, Error>> + Unpin {
        Box::pin(futures_stream::unfold(
            (self.clone(), args),
            move |(client, mut args)| async move {
                let resp = client.list_object_versions(&args).await;
                match resp {
                    Ok(resp) => {
                        if !resp.is_truncated {
                            None
                        } else {
                            args.key_marker = resp.next_key_marker.clone();
                            args.version_id_marker = resp.next_version_id_marker.clone();
                            Some((Ok(resp), (client, args)))
                        }
                    }
                    Err(e) => Some((Err(e), (client, args))),
                }
            },
        ))
    }

    /// List objects with version information optionally. This function handles
    /// pagination and returns a stream of results. Each result corresponds to
    /// the response of a single listing API call.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// use minio::s3::client::{Client, ClientBuilder};
    /// use minio::s3::creds::StaticProvider;
    /// use minio::s3::http::BaseUrl;
    /// use minio::s3::args::ListObjectsArgs;
    /// use futures_util::StreamExt;
    ///
    /// #[tokio::main]
    /// async fn main() {
    ///     let base_url: BaseUrl = "play.min.io".parse().unwrap();
    ///     let static_provider = StaticProvider::new(
    ///         "Q3AM3UQ867SPQQA43P2F",
    ///         "zuf+tfteSlswRu7BJ86wekitnifILbZam1KYY3TG",
    ///         None,
    ///     );
    ///
    ///     let client = ClientBuilder::new(base_url)
    ///         .provider(Some(Box::new(static_provider)))
    ///         .build()
    ///         .unwrap();
    ///
    ///     // List all objects in a directory.
    ///     let mut list_objects_arg = ListObjectsArgs::new("my-bucket").unwrap();
    ///     list_objects_arg.recursive = true;
    ///     let mut stream = client.list_objects(list_objects_arg).await;
    ///     while let Some(result) = stream.next().await {
    ///        match result {
    ///            Ok(items) => {
    ///                for item in items {
    ///                    println!("{:?}", item);
    ///                }
    ///            }
    ///            Err(e) => println!("Error: {:?}", e),
    ///        }
    ///     }
    /// }
    pub async fn list_objects(
        &self,
        args: ListObjectsArgs,
    ) -> Box<dyn Stream<Item = Result<Vec<ListEntry>, Error>> + Unpin> {
        if args.include_versions {
            let stream = self.list_object_versions_stream(args.into()).await;
            Box::new(stream.map(|v| v.map(|v| v.contents)))
        } else if args.use_api_v1 {
            let stream = self.list_objects_v1_stream(args.into()).await;
            Box::new(stream.map(|v| v.map(|v| v.contents)))
        } else {
            let stream = self.list_objects_v2_stream(args.into()).await;
            Box::new(stream.map(|v| v.map(|v| v.contents)))
        }
    }
}
