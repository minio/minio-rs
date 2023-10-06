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

//! Response types for ListObjects APIs

use std::collections::HashMap;

use async_trait::async_trait;
use bytes::Buf;
use reqwest::header::HeaderMap;
use xmltree::Element;

use crate::s3::{
    error::Error,
    types::{FromResponse, ListEntry, Request},
    utils::{from_iso8601utc, get_default_text, get_option_text, get_text, urldecode},
};

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

/// Response of [list_objects_v1()](crate::s3::client::Client::list_objects_v1) S3 API
#[derive(Clone, Debug)]
pub struct ListObjectsV1Response {
    pub headers: HeaderMap,
    pub name: String,
    pub encoding_type: Option<String>,
    pub prefix: Option<String>,
    pub delimiter: Option<String>,
    pub is_truncated: bool,
    pub max_keys: Option<u16>,
    pub contents: Vec<ListEntry>,
    pub marker: Option<String>,
    pub next_marker: Option<String>,
}

#[async_trait]
impl FromResponse for ListObjectsV1Response {
    async fn from_response<'a>(_req: Request<'a>, resp: reqwest::Response) -> Result<Self, Error> {
        let headers = resp.headers().clone();
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
            headers,
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
}

/// Response of [list_objects_v2()](crate::s3::client::Client::list_objects_v2) S3 API
#[derive(Clone, Debug)]
pub struct ListObjectsV2Response {
    pub headers: HeaderMap,
    pub name: String,
    pub encoding_type: Option<String>,
    pub prefix: Option<String>,
    pub delimiter: Option<String>,
    pub is_truncated: bool,
    pub max_keys: Option<u16>,
    pub contents: Vec<ListEntry>,
    pub key_count: Option<u16>,
    pub start_after: Option<String>,
    pub continuation_token: Option<String>,
    pub next_continuation_token: Option<String>,
}

#[async_trait]
impl FromResponse for ListObjectsV2Response {
    async fn from_response<'a>(_req: Request<'a>, resp: reqwest::Response) -> Result<Self, Error> {
        let headers = resp.headers().clone();
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
            headers,
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
}

/// Response of [list_object_versions()](crate::s3::client::Client::list_object_versions) S3 API
#[derive(Clone, Debug)]
pub struct ListObjectVersionsResponse {
    pub headers: HeaderMap,
    pub name: String,
    pub encoding_type: Option<String>,
    pub prefix: Option<String>,
    pub delimiter: Option<String>,
    pub is_truncated: bool,
    pub max_keys: Option<u16>,
    pub contents: Vec<ListEntry>,
    pub key_marker: Option<String>,
    pub next_key_marker: Option<String>,
    pub version_id_marker: Option<String>,
    pub next_version_id_marker: Option<String>,
}

#[async_trait]
impl FromResponse for ListObjectVersionsResponse {
    async fn from_response<'a>(_req: Request<'a>, resp: reqwest::Response) -> Result<Self, Error> {
        let headers = resp.headers().clone();
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
            headers,
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
}

/// Response of [list_objects()](crate::s3::client::Client::list_objects) API
#[derive(Clone, Debug, Default)]
pub struct ListObjectsResponse {
    pub headers: HeaderMap,
    pub name: String,
    pub encoding_type: Option<String>,
    pub prefix: Option<String>,
    pub delimiter: Option<String>,
    pub is_truncated: bool,
    pub max_keys: Option<u16>,
    pub contents: Vec<ListEntry>,

    // ListObjectsV1
    pub marker: Option<String>,
    pub next_marker: Option<String>,

    // ListObjectsV2
    pub key_count: Option<u16>,
    pub start_after: Option<String>,
    pub continuation_token: Option<String>,
    pub next_continuation_token: Option<String>,

    // ListObjectVersions
    pub key_marker: Option<String>,
    pub next_key_marker: Option<String>,
    pub version_id_marker: Option<String>,
    pub next_version_id_marker: Option<String>,
}

impl From<ListObjectVersionsResponse> for ListObjectsResponse {
    fn from(value: ListObjectVersionsResponse) -> Self {
        ListObjectsResponse {
            headers: value.headers,
            name: value.name,
            encoding_type: value.encoding_type,
            prefix: value.prefix,
            delimiter: value.delimiter,
            is_truncated: value.is_truncated,
            max_keys: value.max_keys,
            contents: value.contents,
            key_marker: value.key_marker,
            next_key_marker: value.next_key_marker,
            version_id_marker: value.version_id_marker,
            next_version_id_marker: value.next_version_id_marker,
            ..Default::default()
        }
    }
}

impl From<ListObjectsV2Response> for ListObjectsResponse {
    fn from(value: ListObjectsV2Response) -> Self {
        ListObjectsResponse {
            headers: value.headers,
            name: value.name,
            encoding_type: value.encoding_type,
            prefix: value.prefix,
            delimiter: value.delimiter,
            is_truncated: value.is_truncated,
            max_keys: value.max_keys,
            contents: value.contents,
            key_count: value.key_count,
            start_after: value.start_after,
            continuation_token: value.continuation_token,
            next_continuation_token: value.next_continuation_token,
            ..Default::default()
        }
    }
}

impl From<ListObjectsV1Response> for ListObjectsResponse {
    fn from(value: ListObjectsV1Response) -> Self {
        ListObjectsResponse {
            headers: value.headers,
            name: value.name,
            encoding_type: value.encoding_type,
            prefix: value.prefix,
            delimiter: value.delimiter,
            is_truncated: value.is_truncated,
            max_keys: value.max_keys,
            contents: value.contents,
            marker: value.marker,
            next_marker: value.next_marker,
            ..Default::default()
        }
    }
}
