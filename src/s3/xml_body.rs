// MinIO Rust Library for Amazon S3 Compatible Cloud Storage
// Copyright 2026 MinIO, Inc.
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

//! Serialization of S3 XML request bodies.

use xml::escape::escape_str_attribute;
use xml::writer::{EmitterConfig, EventWriter, XmlEvent};

use crate::s3::error::ValidationErr;

/// Writes an S3 XML request body through an XML encoder, so caller-supplied
/// text can only ever become element text.
///
/// Element text is escaped with the entity table S3 requires for object keys in
/// XML requests: `'` `"` `&` `<` `>` as named entities and `\r` `\n` as character
/// references. A raw `\r` would otherwise be read back as `\n` by the server's
/// parser and name a different key. Text holding a character XML 1.0 cannot
/// represent at all fails [`Self::finish`] instead of being sent altered.
///
/// See <https://docs.aws.amazon.com/AmazonS3/latest/userguide/object-keys.html#object-key-xml-related-constraints>.
pub(crate) struct XmlBody {
    writer: EventWriter<Vec<u8>>,
    error: Option<ValidationErr>,
}

impl XmlBody {
    pub(crate) fn new() -> Self {
        let mut config = EmitterConfig::new();
        config.write_document_declaration = false;
        config.perform_escaping = false;
        config.normalize_empty_elements = false;
        Self {
            writer: EventWriter::new_with_config(Vec::new(), config),
            error: None,
        }
    }

    /// Opens element `name`.
    pub(crate) fn start(&mut self, name: &str) -> &mut Self {
        self.write(XmlEvent::start_element(name).into());
        self
    }

    /// Opens element `name` in the S3 namespace.
    pub(crate) fn start_s3(&mut self, name: &str) -> &mut Self {
        self.write(
            XmlEvent::start_element(name)
                .default_ns("http://s3.amazonaws.com/doc/2006-03-01/")
                .into(),
        );
        self
    }

    /// Closes the innermost open element.
    pub(crate) fn end(&mut self) -> &mut Self {
        self.write(XmlEvent::end_element().into());
        self
    }

    /// Writes `text` as the content of the current element.
    pub(crate) fn text(&mut self, text: &str) -> &mut Self {
        if let Some(c) = text.chars().find(|&c| !is_xml_char(c)) {
            self.error
                .get_or_insert(ValidationErr::InvalidXmlCharacter {
                    value: text.to_owned(),
                    character: c,
                });
            return self;
        }
        let escaped = escape_str_attribute(text);
        self.write(XmlEvent::characters(&escaped));
        self
    }

    /// Writes `<name>text</name>`.
    pub(crate) fn element(&mut self, name: &str, text: &str) -> &mut Self {
        self.start(name).text(text).end()
    }

    /// Writes `<name>text</name>` when `text` is set.
    pub(crate) fn optional(&mut self, name: &str, text: Option<&str>) -> &mut Self {
        if let Some(text) = text {
            self.element(name, text);
        }
        self
    }

    /// The serialized body, or the first value that cannot be written as XML.
    pub(crate) fn finish(self) -> Result<String, ValidationErr> {
        if let Some(err) = self.error {
            return Err(err);
        }
        String::from_utf8(self.writer.into_inner())
            .map_err(|e| ValidationErr::xml_error_with_source("request body is not UTF-8", e))
    }

    fn write(&mut self, event: XmlEvent<'_>) {
        if self.error.is_none()
            && let Err(e) = self.writer.write(event)
        {
            self.error = Some(ValidationErr::xml_error_with_source(
                "writing the request body",
                e,
            ));
        }
    }
}

/// Whether `c` is a `Char` in the XML 1.0 grammar (Rust `char` already excludes
/// surrogates).
fn is_xml_char(c: char) -> bool {
    matches!(c, '\t' | '\n' | '\r' | '\u{20}'..='\u{FFFD}' | '\u{10000}'..)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn read_back(xml: &str, name: &str) -> String {
        let root = xmltree::Element::parse(xml.as_bytes()).unwrap();
        root.get_child(name)
            .unwrap()
            .get_text()
            .unwrap()
            .into_owned()
    }

    #[test]
    fn text_survives_a_round_trip_through_an_xml_parser() {
        for key in [
            "plain",
            "R&D notes.md",
            "a<b>c",
            "it's \"quoted\"",
            "cr\rlf\n",
            "tab\tend",
            "☃/ü",
        ] {
            let mut body = XmlBody::new();
            body.start("Object").element("Key", key).end();
            let xml = body.finish().unwrap();
            assert_eq!(read_back(&xml, "Key"), key, "{xml}");
        }
    }

    #[test]
    fn markup_in_text_cannot_add_elements() {
        let mut body = XmlBody::new();
        body.start("Object")
            .element("Key", "a</Key><Key>other")
            .end();
        let xml = body.finish().unwrap();
        let root = xmltree::Element::parse(xml.as_bytes()).unwrap();
        assert_eq!(root.children.len(), 1, "{xml}");
        assert_eq!(read_back(&xml, "Key"), "a</Key><Key>other");
    }

    #[test]
    fn the_spec_entities_are_used() {
        let mut body = XmlBody::new();
        body.element("Key", "'\"&<>\r\n");
        assert_eq!(
            body.finish().unwrap(),
            "<Key>&apos;&quot;&amp;&lt;&gt;&#xD;&#xA;</Key>"
        );
    }

    #[test]
    fn a_character_xml_cannot_carry_is_refused() {
        for key in ["nul\0", "bell\u{7}", "vt\u{b}", "\u{fffe}"] {
            let mut body = XmlBody::new();
            body.start("Object").element("Key", key).end();
            assert!(
                matches!(
                    body.finish(),
                    Err(ValidationErr::InvalidXmlCharacter { .. })
                ),
                "{key:?}"
            );
        }
    }

    #[test]
    fn empty_text_writes_an_open_and_close_tag() {
        let mut body = XmlBody::new();
        body.element("Prefix", "");
        assert_eq!(body.finish().unwrap(), "<Prefix></Prefix>");
    }
}
