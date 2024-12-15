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

use crate::s3::{
    error::Error,
    types::{FromS3Response, ListEntry, S3Request},
    utils::{
        from_iso8601utc, parse_tags, urldecode,
        xml::{Element, MergeXmlElements},
    },
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

#[allow(clippy::type_complexity)]
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
    let encoding_type = root.get_child_text("EncodingType");
    let prefix = url_decode(
        &encoding_type,
        Some(root.get_child_text("Prefix").unwrap_or_default()),
    )?;
    Ok((
        root.get_child_text_or_error("Name")?,
        encoding_type,
        prefix,
        root.get_child_text("Delimiter"),
        root.get_child_text("IsTruncated")
            .map(|x| x.to_lowercase() == "true")
            .unwrap_or(false),
        root.get_child_text("MaxKeys")
            .map(|x| x.parse::<u16>())
            .transpose()?,
    ))
}

fn parse_list_objects_contents(
    contents: &mut Vec<ListEntry>,
    root: &Element,
    main_tag: &str,
    encoding_type: &Option<String>,
    with_delete_marker: bool,
) -> Result<(), Error> {
    let children1 = root.get_matching_children(main_tag);
    let children2 = if with_delete_marker {
        root.get_matching_children("DeleteMarker")
    } else {
        vec![]
    };
    let merged = MergeXmlElements::new(&children1, &children2);
    for content in merged {
        let etype = encoding_type.as_ref().cloned();
        let key = url_decode(&etype, Some(content.get_child_text_or_error("Key")?))?.unwrap();
        let last_modified = Some(from_iso8601utc(
            &content.get_child_text_or_error("LastModified")?,
        )?);
        let etag = content.get_child_text("ETag");
        let size: Option<u64> = content
            .get_child_text("Size")
            .map(|x| x.parse::<u64>())
            .transpose()?;
        let storage_class = content.get_child_text("StorageClass");
        let is_latest = content
            .get_child_text("IsLatest")
            .unwrap_or_default()
            .to_lowercase()
            == "true";
        let version_id = content.get_child_text("VersionId");
        let (owner_id, owner_name) = content
            .get_child("Owner")
            .map(|v| (v.get_child_text("ID"), v.get_child_text("DisplayName")))
            .unwrap_or((None, None));
        let user_metadata = content.get_child("UserMetadata").map(|v| {
            v.get_xmltree_children()
                .into_iter()
                .map(|elem| {
                    (
                        elem.name.to_string(),
                        elem.get_text().unwrap_or_default().to_string(),
                    )
                })
                .collect::<HashMap<String, String>>()
        });
        let user_tags = content
            .get_child_text("UserTags")
            .as_ref()
            .map(|x| parse_tags(x))
            .transpose()?;
        let is_delete_marker = content.name() == "DeleteMarker";

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
            user_tags,
            is_prefix: false,
            is_delete_marker,
            encoding_type: etype,
        });
    }

    Ok(())
}

fn parse_list_objects_common_prefixes(
    contents: &mut Vec<ListEntry>,
    root: &Element,
    encoding_type: &Option<String>,
) -> Result<(), Error> {
    for (_, common_prefix) in root.get_matching_children("CommonPrefixes") {
        contents.push(ListEntry {
            name: url_decode(
                encoding_type,
                Some(common_prefix.get_child_text_or_error("Prefix")?),
            )?
            .unwrap(),
            last_modified: None,
            etag: None,
            owner_id: None,
            owner_name: None,
            size: None,
            storage_class: None,
            is_latest: false,
            version_id: None,
            user_metadata: None,
            user_tags: None,
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
impl FromS3Response for ListObjectsV1Response {
    async fn from_s3response<'a>(
        _req: S3Request<'a>,
        resp: reqwest::Response,
    ) -> Result<Self, Error> {
        let headers = resp.headers().clone();
        let body = resp.bytes().await?;
        let xmltree_root = xmltree::Element::parse(body.reader())?;
        let root = Element::from(&xmltree_root);

        let (name, encoding_type, prefix, delimiter, is_truncated, max_keys) =
            parse_common_list_objects_response(&root)?;
        let marker = url_decode(&encoding_type, root.get_child_text("Marker"))?;
        let mut next_marker = url_decode(&encoding_type, root.get_child_text("NextMarker"))?;
        let mut contents: Vec<ListEntry> = Vec::new();
        parse_list_objects_contents(&mut contents, &root, "Contents", &encoding_type, false)?;
        if is_truncated && next_marker.is_none() {
            next_marker = contents.last().map(|v| v.name.clone())
        }
        parse_list_objects_common_prefixes(&mut contents, &root, &encoding_type)?;

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
impl FromS3Response for ListObjectsV2Response {
    async fn from_s3response<'a>(
        _req: S3Request<'a>,
        resp: reqwest::Response,
    ) -> Result<Self, Error> {
        let headers = resp.headers().clone();
        let body = resp.bytes().await?;
        let xmltree_root = xmltree::Element::parse(body.reader())?;
        let root = Element::from(&xmltree_root);

        let (name, encoding_type, prefix, delimiter, is_truncated, max_keys) =
            parse_common_list_objects_response(&root)?;
        let key_count = root
            .get_child_text("KeyCount")
            .map(|x| x.parse::<u16>())
            .transpose()?;
        let start_after = url_decode(&encoding_type, root.get_child_text("StartAfter"))?;
        let continuation_token = root.get_child_text("ContinuationToken");
        let next_continuation_token = root.get_child_text("NextContinuationToken");
        let mut contents: Vec<ListEntry> = Vec::new();
        parse_list_objects_contents(&mut contents, &root, "Contents", &encoding_type, false)?;
        parse_list_objects_common_prefixes(&mut contents, &root, &encoding_type)?;

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
impl FromS3Response for ListObjectVersionsResponse {
    async fn from_s3response<'a>(
        _req: S3Request<'a>,
        resp: reqwest::Response,
    ) -> Result<Self, Error> {
        let headers = resp.headers().clone();
        let body = resp.bytes().await?;
        let xmltree_root = xmltree::Element::parse(body.reader())?;
        let root = Element::from(&xmltree_root);

        let (name, encoding_type, prefix, delimiter, is_truncated, max_keys) =
            parse_common_list_objects_response(&root)?;
        let key_marker = url_decode(&encoding_type, root.get_child_text("KeyMarker"))?;
        let next_key_marker = url_decode(&encoding_type, root.get_child_text("NextKeyMarker"))?;
        let version_id_marker = root.get_child_text("VersionIdMarker");
        let next_version_id_marker = root.get_child_text("NextVersionIdMarker");
        let mut contents: Vec<ListEntry> = Vec::new();
        parse_list_objects_contents(&mut contents, &root, "Version", &encoding_type, true)?;
        parse_list_objects_common_prefixes(&mut contents, &root, &encoding_type)?;

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
