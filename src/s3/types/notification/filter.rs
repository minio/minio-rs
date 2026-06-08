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

    pub fn is_null(&self) -> bool {
        let and_is_empty = self.and_operator.as_ref().is_none_or(|and| {
            and.prefix.is_none() && and.tags.as_ref().is_none_or(|tags| tags.is_empty())
        });
        and_is_empty && self.prefix.is_none() && self.tag.is_none()
    }

    pub fn to_xml(&self) -> String {
        let mut data = String::from("<Filter>");
        if let Some(and_op) = &self.and_operator {
            data.push_str("<And>");
            if let Some(prefix) = &and_op.prefix {
                data.push_str("<Prefix>");
                data.push_str(prefix);
                data.push_str("</Prefix>");
            }
            if let Some(tags) = &and_op.tags {
                for (key, value) in tags {
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
        if let Some(prefix) = &self.prefix {
            data.push_str("<Prefix>");
            data.push_str(prefix);
            data.push_str("</Prefix>");
        }
        if let Some(tag) = &self.tag {
            data.push_str("<Tag>");
            data.push_str("<Key>");
            data.push_str(&tag.key);
            data.push_str("</Key>");
            data.push_str("<Value>");
            data.push_str(&tag.value);
            data.push_str("</Value>");
            data.push_str("</Tag>");
        }
        data.push_str("</Filter>");

        data
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn is_null_for_default_filter() {
        assert!(Filter::default().is_null());
    }

    #[test]
    fn is_null_for_empty_and_operator() {
        let filter = Filter {
            and_operator: Some(AndOperator {
                prefix: None,
                tags: Some(HashMap::new()),
            }),
            prefix: None,
            tag: None,
        };
        assert!(filter.is_null());
    }

    #[test]
    fn is_not_null_with_prefix() {
        let filter = Filter {
            and_operator: None,
            prefix: Some("logs/".to_string()),
            tag: None,
        };
        assert!(!filter.is_null());
    }

    #[test]
    fn is_not_null_with_tag() {
        let filter = Filter {
            and_operator: None,
            prefix: None,
            tag: Some(Tag {
                key: "k".to_string(),
                value: "v".to_string(),
            }),
        };
        assert!(!filter.is_null());
    }

    #[test]
    fn is_not_null_with_and_prefix() {
        let filter = Filter {
            and_operator: Some(AndOperator {
                prefix: Some("logs/".to_string()),
                tags: None,
            }),
            prefix: None,
            tag: None,
        };
        assert!(!filter.is_null());
    }

    #[test]
    fn is_not_null_with_and_tags() {
        let mut tags = HashMap::new();
        tags.insert("k".to_string(), "v".to_string());
        let filter = Filter {
            and_operator: Some(AndOperator {
                prefix: None,
                tags: Some(tags),
            }),
            prefix: None,
            tag: None,
        };
        assert!(!filter.is_null());
    }
}
