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

//! Common notification configuration utilities

use super::prefix_filter_rule::PrefixFilterRule;
use super::suffix_filter_rule::SuffixFilterRule;
use crate::s3::error::ValidationErr;
use crate::s3::utils::{get_text_option, get_text_result};
use crate::s3::xml_body::XmlBody;
use xmltree::Element;

#[allow(clippy::type_complexity)]
pub fn parse_common_notification_config(
    element: &mut Element,
) -> Result<
    (
        Vec<String>,
        Option<String>,
        Option<PrefixFilterRule>,
        Option<SuffixFilterRule>,
    ),
    ValidationErr,
> {
    let mut events = Vec::new();
    while let Some(v) = element.take_child("Event") {
        events.push(
            v.get_text()
                .ok_or(ValidationErr::xml_error(
                    "the text of the <Event>-tag is not found",
                ))?
                .to_string(),
        );
    }

    let id = get_text_option(element, "Id");

    let (prefix_filter_rule, suffix_filter_rule) = match element.get_child("Filter") {
        Some(filter) => {
            let mut prefix = None;
            let mut suffix = None;
            let rules = filter
                .get_child("S3Key")
                .ok_or(ValidationErr::xml_error("<S3Key> tag not found"))?;
            for rule in &rules.children {
                let v = rule
                    .as_element()
                    .ok_or(ValidationErr::xml_error("<FilterRule> tag not found"))?;
                let name = get_text_result(v, "Name")?;
                let value = get_text_result(v, "Value")?;
                if PrefixFilterRule::NAME == name {
                    prefix = Some(PrefixFilterRule { value });
                } else {
                    suffix = Some(SuffixFilterRule { value });
                }
            }
            (prefix, suffix)
        }
        _ => (None, None),
    };

    Ok((events, id, prefix_filter_rule, suffix_filter_rule))
}

pub(crate) fn write_common_notification_config(
    body: &mut XmlBody,
    events: &[String],
    id: &Option<String>,
    prefix_filter_rule: &Option<PrefixFilterRule>,
    suffix_filter_rule: &Option<SuffixFilterRule>,
) {
    for event in events {
        body.element("Event", event);
    }
    body.optional("Id", id.as_deref());
    if prefix_filter_rule.is_some() || suffix_filter_rule.is_some() {
        body.start("Filter").start("S3Key");
        if let Some(v) = prefix_filter_rule {
            body.start("FilterRule")
                .element("Name", "prefix")
                .element("Value", &v.value)
                .end();
        }
        if let Some(v) = suffix_filter_rule {
            body.start("FilterRule")
                .element("Name", "suffix")
                .element("Value", &v.value)
                .end();
        }
        body.end().end();
    }
}
