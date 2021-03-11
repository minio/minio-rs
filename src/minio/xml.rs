/*
 * MinIO Rust Library for Amazon S3 Compatible Cloud Storage
 * Copyright 2019 MinIO, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

extern crate quick_xml;
extern crate serde;

use std::collections::HashMap;
use std::str::FromStr;
use std::str;

use hyper::body::Body;
use roxmltree;
use serde_derive::Deserialize;
use thiserror::Error;

use crate::minio::types::{BucketInfo, Err, ListObjectsResp, ObjectInfo, Region};
use crate::minio::woxml;

pub fn parse_bucket_location(s: String) -> Result<Region, Err> {
    let res = roxmltree::Document::parse(&s);
    match res {
        Ok(doc) => {
            let region_res = doc.root_element().text();
            if let Some(region) = region_res {
                Ok(Region::new(region))
            } else {
                Ok(Region::empty())
            }
        }
        Err(e) => Err(Err::XmlDocParseErr(e)),
    }
}

#[allow(non_snake_case)]
#[derive(Debug, Deserialize, PartialEq)]
pub struct Error {
    Param: Option<String>,
    Code: String,
    Message: String,
    BucketName: String,
    Key: Option<String>,
    RequestId: String,
    HostId: String,
    // Region where the bucket is located. This header is returned
    // only in HEAD bucket and ListObjects response.
    Region: Option<String>,
}


#[derive(Error, Debug, PartialEq)]
pub enum S3GenericError {
    #[error("no such bucket: {error:?}")]
    NoSuchBucket {
        error: Error,
    },
    #[error("unknown error: {error:?}")]
    Unknown {
        error: Error,
    },
}


pub(crate) fn parse_s3_error(response_xml: &str) -> S3GenericError {
    println!("{}",response_xml);
    let doc: Error = quick_xml::de::from_str(response_xml).unwrap();
    match doc.Code.as_str() {
        "NoSuchBucket" => {
            return S3GenericError::NoSuchBucket {
                error: doc,
            };
        }
        _ => {
            return S3GenericError::Unknown {
                error: doc,
            };
        }
    }
}

#[cfg(test)]
mod xml_tests {

    use super::*;

    #[test]
    fn parse_xml_error() {
        let response_xml = r#"
        <?xml version="1.0" encoding="UTF-8"?>
            <Error>
                <Code>NoSuchBucket</Code>
                <Message>The specified bucket does not exist</Message>
                <Key>hhhhhhhhhh</Key>
                <BucketName>aaaa</BucketName>
                <Resource>/aaaa/hhhhhhhhhh</Resource>
                <RequestId>166B5E4E3A406CC6</RequestId>
                <HostId>129c19c9-4cf6-44ff-9f2d-4cb7611be894</HostId>
            </Error>
        "#;

        let s3_error = parse_s3_error(response_xml);

        print!("test! {:?}", s3_error);
        assert!(matches!(s3_error, S3GenericError::NoSuchBucket {cerror} ));
    }
}


pub fn parse_bucket_list(s: String) -> Result<Vec<BucketInfo>, Err> {
    let res = roxmltree::Document::parse(&s);
    match res {
        Ok(doc) => {
            let mut bucket_infos: Vec<BucketInfo> = Vec::new();
            let bucket_nodes = doc
                .root_element()
                .descendants()
                .filter(|node| node.has_tag_name("Bucket"));
            for bucket in bucket_nodes {
                let bucket_names = bucket.children().filter(|node| node.has_tag_name("Name"));
                let bucket_ctimes = bucket
                    .children()
                    .filter(|node| node.has_tag_name("CreationDate"));
                for (name_node, ctime_node) in bucket_names.zip(bucket_ctimes) {
                    let name = name_node.text().ok_or(Err::InvalidXmlResponseErr(
                        "Missing name in list buckets XML response ".to_string(),
                    ))?;
                    let ctime = ctime_node.text().ok_or(Err::InvalidXmlResponseErr(
                        "Missing creation date in list buckets XML response".to_string(),
                    ))?;
                    match BucketInfo::new(name, ctime) {
                        Ok(bucket_info) => bucket_infos.push(bucket_info),
                        Err(err) => return Err(Err::InvalidTmFmt(format!("{:?}", err))),
                    }
                }
            }
            Ok(bucket_infos)
        }
        Err(err) => Err(Err::XmlDocParseErr(err)),
    }
}

pub fn parse_list_objects(s: String) -> Result<ListObjectsResp, Err> {
    let doc_res = roxmltree::Document::parse(&s);
    match doc_res {
        Ok(doc) => parse_list_objects_result(doc),
        Err(err) => panic!(err),
    }
}

pub fn get_mk_bucket_body() -> Result<Body, Err> {
    let lc_node = woxml::XmlNode::new("LocationConstraint").text("us-east-1");
    let mk_bucket_xml = woxml::XmlNode::new("CreateBucketConfiguration")
        .namespace("http://s3.amazonaws.com/doc/2006-03-01/")
        .children(vec![lc_node]);
    let mut xml_bytes = Vec::new();

    mk_bucket_xml
        .serialize(&mut xml_bytes)
        .or_else(|err| Err(Err::XmlWriteErr(err.to_string())))?;
    Ok(Body::from(xml_bytes))
}

fn get_child_node<'a>(node: &'a roxmltree::Node, tag_name: &str) -> Option<&'a str> {
    node.children()
        .find(|node| node.has_tag_name(tag_name))
        .and_then(|node| node.text())
}

// gets text value inside given tag or return default
fn get_child_node_or<'a>(node: &'a roxmltree::Node, tag_name: &str, default: &'a str) -> &'a str {
    get_child_node(&node, tag_name).unwrap_or(default)
}

fn parse_child_content<T>(node: &roxmltree::Node, tag: &str) -> Result<T, Err>
    where
        T: FromStr,
{
    let content = get_child_node(node, tag).ok_or(Err::XmlElemMissing(format!("{:?}", tag)))?;
    str::parse::<T>(content).map_err(|_| Err::XmlElemParseErr(format!("{}", tag)))
}

fn parse_tag_content<T>(node: &roxmltree::Node) -> Result<T, Err>
    where
        T: FromStr,
{
    let content = must_get_node_text(node)?;
    str::parse::<T>(content).map_err(|_| Err::XmlElemParseErr(format!("{:?}", node.tag_name())))
}

fn must_get_node_text<'a>(node: &'a roxmltree::Node) -> Result<&'a str, Err> {
    node.text()
        .ok_or(Err::XmlElemMissing(node.tag_name().name().to_string()))
}

fn parse_object_infos(node: roxmltree::Node) -> Result<Vec<ObjectInfo>, Err> {
    let mut object_infos: Vec<ObjectInfo> = Vec::new();
    let contents_nodes = node
        .descendants()
        .filter(|node| node.has_tag_name("Contents"));
    for node in contents_nodes {
        let keys = node.children().filter(|node| node.has_tag_name("Key"));
        let mtimes = node
            .children()
            .filter(|node| node.has_tag_name("LastModified"));
        let etags = node.children().filter(|node| node.has_tag_name("ETag"));
        let sizes = node.children().filter(|node| node.has_tag_name("Size"));
        let storage_classes = node
            .children()
            .filter(|node| node.has_tag_name("StorageClass"));
        for (key, (mtime, (etag, (size, storage_class)))) in
        keys.zip(mtimes.zip(etags.zip(sizes.zip(storage_classes))))
        {
            let sz: i64 = parse_tag_content(&size)?;
            let key_text = must_get_node_text(&key)?;
            let mtime_text = must_get_node_text(&mtime)?;
            let etag_text = must_get_node_text(&etag)?;
            let storage_class_text = must_get_node_text(&storage_class)?;
            let object_info = ObjectInfo::new(
                key_text,
                mtime_text,
                etag_text,
                sz,
                storage_class_text,
                HashMap::new(),
            )?;
            object_infos.push(object_info);
        }
    }
    Ok(object_infos)
}

fn parse_list_objects_result(doc: roxmltree::Document) -> Result<ListObjectsResp, Err> {
    let root = doc.root_element();
    let bucket_name =
        get_child_node(&root, "Name").ok_or(Err::XmlElemMissing("Name".to_string()))?;
    let prefix = get_child_node_or(&root, "Prefix", "");
    let key_count: i32 = parse_child_content(&root, "KeyCount")?;
    let max_keys: i32 = parse_child_content(&root, "MaxKeys")?;
    let is_truncated: bool = parse_child_content(&root, "IsTruncated")?;
    let object_infos = parse_object_infos(root)?;

    Ok(ListObjectsResp {
        bucket_name: bucket_name.to_string(),
        prefix: prefix.to_string(),
        max_keys: max_keys,
        key_count: key_count,
        is_truncated: is_truncated,
        next_continuation_token: "".to_string(),
        common_prefixes: Vec::new(),
        object_infos: object_infos,
    })
}
