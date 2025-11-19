// MinIO Rust Library for Amazon S3 Compatible Cloud Storage
// Copyright 2022 MinIO, Inc.
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

//! Filter information

use super::super::tag::Tag;
use super::and_operator::AndOperator;
use crate::s3::error::ValidationErr;
use crate::s3::utils::get_text_result;
use std::collections::HashMap;
use xmltree::Element;

#[derive(Clone, Debug, PartialEq, Default)]
pub struct Filter {
    pub and_operator: Option<AndOperator>,
    pub prefix: Option<String>,
    pub tag: Option<Tag>,
}

impl Filter {
    pub fn from_xml(element: &Element) -> Result<Filter, ValidationErr> {
        let and_operator = match element.get_child("And") {
            Some(v) => Some(AndOperator {
                prefix: match v.get_child("Prefix") {
                    Some(p) => Some(
                        p.get_text()
                            .ok_or(ValidationErr::xml_error(
                                "the text of <Prefix>-tag not found",
                            ))?
                            .to_string(),
                    ),
                    None => None,
                },
                tags: match v.get_child("Tag") {
                    Some(tags) => {
                        let mut map: HashMap<String, String> = HashMap::new();
                        for xml_node in &tags.children {
                            let tag = xml_node
                                .as_element()
                                .ok_or(ValidationErr::xml_error("<Tag> element not found"))?;
                            map.insert(
                                get_text_result(tag, "Key")?,
                                get_text_result(tag, "Value")?,
                            );
                        }
                        Some(map)
                    }
                    None => None,
                },
            }),
            None => None,
        };

        let prefix = match element.get_child("Prefix") {
            Some(v) => Some(
                v.get_text()
                    .ok_or(ValidationErr::xml_error(
                        "the text of <Prefix>-tag not found",
                    ))?
                    .to_string(),
            ),
            None => None,
        };

        let tag = match element.get_child("Tag") {
            Some(v) => Some(Tag {
                key: get_text_result(v, "Key")?,
                value: get_text_result(v, "Value")?,
            }),
            None => None,
        };

        Ok(Filter {
            and_operator,
            prefix,
            tag,
        })
    }

    pub fn validate(&self) -> Result<(), ValidationErr> {
        if self.and_operator.is_some() ^ self.prefix.is_some() ^ self.tag.is_some() {
            return Ok(());
        }
        Err(ValidationErr::InvalidFilter(self.to_xml()))
    }

    pub fn to_xml(&self) -> String {
        let mut data = String::from("<Filter>");
        if self.and_operator.is_some() {
            data.push_str("<And>");
            if self.and_operator.as_ref().unwrap().prefix.is_some() {
                data.push_str("<Prefix>");
                data.push_str(self.and_operator.as_ref().unwrap().prefix.as_ref().unwrap());
                data.push_str("</Prefix>");
            }
            if self.and_operator.as_ref().unwrap().tags.is_some() {
                for (key, value) in self.and_operator.as_ref().unwrap().tags.as_ref().unwrap() {
                    data.push_str("<Tag>");
                    data.push_str("<Key>");
                    data.push_str(key);
                    data.push_str("</Key>");
                    data.push_str("<Value>");
                    data.push_str(value);
                    data.push_str("</Value>");
                    data.push_str("</Tag>");
                }
            }
            data.push_str("</And>");
        }
        if self.prefix.is_some() {
            data.push_str("<Prefix>");
            data.push_str(self.prefix.as_ref().unwrap());
            data.push_str("</Prefix>");
        }
        if self.tag.is_some() {
            data.push_str("<Tag>");
            data.push_str("<Key>");
            data.push_str(&self.tag.as_ref().unwrap().key);
            data.push_str("</Key>");
            data.push_str("<Value>");
            data.push_str(&self.tag.as_ref().unwrap().value);
            data.push_str("</Value>");
            data.push_str("</Tag>");
        }
        data.push_str("</Filter>");

        data
    }
}
