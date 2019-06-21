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

extern crate xml;

use xml::writer::{EmitterConfig, EventWriter, XmlEvent};

pub struct XmlNode {
    name: String,
    namespace: Option<String>,
    text: Option<String>,
    children: Vec<XmlNode>,
}

impl XmlNode {
    pub fn new(name: &str) -> XmlNode {
        XmlNode {
            name: name.to_string(),
            namespace: None,
            text: None,
            children: Vec::new(),
        }
    }
    pub fn namespace(mut self, ns: &str) -> XmlNode {
        self.namespace = Some(ns.to_string());
        self
    }

    pub fn text(mut self, value: &str) -> XmlNode {
        self.text = Some(value.to_string());
        self
    }

    pub fn children(mut self, kids: Vec<XmlNode>) -> XmlNode {
        self.children = kids;
        self
    }

    fn serialize_rec<W>(&self, xml_writer: &mut EventWriter<W>) -> xml::writer::Result<()>
    where
        W: std::io::Write,
    {
        let st_elem = XmlEvent::start_element(self.name.as_str());
        let st_elem = match &self.namespace {
            Some(ns) => st_elem.ns("", ns.clone()),
            None => st_elem,
        };
        xml_writer.write(st_elem)?;

        // An xml node would have a text field or child nodes, not both, at least not usually.
        match &self.text {
            Some(content) => {
                let content_node = XmlEvent::characters(content.as_str());
                xml_writer.write(content_node)?;
            }
            None => {
                for child in &self.children {
                    child.serialize_rec(xml_writer)?;
                }
            }
        }

        let end_elem: XmlEvent = XmlEvent::end_element().name(self.name.as_str()).into();
        xml_writer.write(end_elem)?;

        Ok(())
    }
    pub fn serialize<W>(&self, writer: W) -> xml::writer::Result<()>
    where
        W: std::io::Write,
    {
        let mut xml_writer = EmitterConfig::new()
            .perform_indent(true)
            .create_writer(writer);
        self.serialize_rec(&mut xml_writer)
    }
}
