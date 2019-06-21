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

use std::collections::HashMap;
use std::str::FromStr;

use hyper::body::Body;
use roxmltree;

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

fn get_child_node(node: roxmltree::Node, tag_name: &str) -> Option<String> {
    node.children()
        .find(|node| node.has_tag_name(tag_name))
        .and_then(|node| node.text())
        .map(|x| x.to_string())
}

// gets text value inside given tag or return default
fn get_child_node_or(node: roxmltree::Node, tag_name: &str, default: &str) -> String {
    get_child_node(node, tag_name).unwrap_or(default.to_string())
}

fn parse_child_content<T>(node: roxmltree::Node, tag: &str) -> Result<T, Err>
where
    T: FromStr,
{
    let content = get_child_node(node, tag).ok_or(Err::XmlElemMissing(format!("{:?}", tag)))?;
    str::parse::<T>(content.as_str()).map_err(|_| Err::XmlElemParseErr(format!("{}", tag)))
}

fn parse_tag_content<T>(node: roxmltree::Node) -> Result<T, Err>
where
    T: FromStr,
{
    let content = node
        .text()
        .ok_or(Err::XmlElemMissing(format!("{:?}", node.tag_name())))?;
    str::parse::<T>(content).map_err(|_| Err::XmlElemParseErr(format!("{:?}", node.tag_name())))
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
            let sz: i64 = parse_tag_content(size)?;
            let object_info = ObjectInfo::new(
                key.text().unwrap(),
                mtime.text().unwrap(),
                etag.text().unwrap(),
                sz,
                storage_class.text().unwrap(),
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
        get_child_node(root, "Name").ok_or(Err::XmlElemMissing("Name".to_string()))?;
    let prefix = get_child_node_or(root, "Prefix", "");
    let key_count: i32 = parse_child_content(root, "KeyCount")?;
    let max_keys: i32 = parse_child_content(root, "MaxKeys")?;
    let is_truncated: bool = parse_child_content(root, "IsTruncated")?;
    let object_infos = parse_object_infos(root)?;

    Ok(ListObjectsResp {
        bucket_name: bucket_name,
        prefix: prefix,
        max_keys: max_keys,
        key_count: key_count,
        is_truncated: is_truncated,
        next_continuation_token: "".to_string(),
        common_prefixes: Vec::new(),
        object_infos: object_infos,
    })
}
