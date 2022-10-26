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

use crate::s3::error::Error;
use crate::s3::utils::{
    from_iso8601utc, get_default_text, get_option_text, get_text, to_iso8601utc, UtcTime,
};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt;
use xmltree::Element;

#[derive(Clone, Debug, Default)]
pub struct Item {
    pub name: String,
    pub last_modified: Option<UtcTime>,
    pub etag: Option<String>, // except DeleteMarker
    pub owner_id: Option<String>,
    pub owner_name: Option<String>,
    pub size: Option<usize>, // except DeleteMarker
    pub storage_class: Option<String>,
    pub is_latest: bool,            // except ListObjects V1/V2
    pub version_id: Option<String>, // except ListObjects V1/V2
    pub user_metadata: Option<HashMap<String, String>>,
    pub is_prefix: bool,
    pub is_delete_marker: bool,
    pub encoding_type: Option<String>,
}

#[derive(Clone, Debug)]
pub struct Bucket {
    pub name: String,
    pub creation_date: UtcTime,
}

#[derive(Clone, Debug)]
pub struct Part {
    pub number: u16,
    pub etag: String,
}

#[derive(Clone, Debug)]
pub enum RetentionMode {
    GOVERNANCE,
    COMPLIANCE,
}

impl RetentionMode {
    pub fn parse(s: &str) -> Result<RetentionMode, Error> {
        match s {
            "GOVERNANCE" => Ok(RetentionMode::GOVERNANCE),
            "COMPLIANCE" => Ok(RetentionMode::COMPLIANCE),
            _ => Err(Error::InvalidRetentionMode(s.to_string())),
        }
    }
}

impl fmt::Display for RetentionMode {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            RetentionMode::GOVERNANCE => write!(f, "GOVERNANCE"),
            RetentionMode::COMPLIANCE => write!(f, "COMPLIANCE"),
        }
    }
}

#[derive(Clone, Debug)]
pub struct Retention {
    pub mode: RetentionMode,
    pub retain_until_date: UtcTime,
}

pub fn parse_legal_hold(s: &str) -> Result<bool, Error> {
    match s {
        "ON" => Ok(true),
        "OFF" => Ok(false),
        _ => Err(Error::InvalidLegalHold(s.to_string())),
    }
}

#[derive(Clone, Debug, Copy)]
pub struct DeleteObject<'a> {
    pub name: &'a str,
    pub version_id: Option<&'a str>,
}

#[derive(Clone, Debug)]
pub enum CompressionType {
    NONE,
    GZIP,
    BZIP2,
}

impl fmt::Display for CompressionType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            CompressionType::NONE => write!(f, "NONE"),
            CompressionType::GZIP => write!(f, "GZIP"),
            CompressionType::BZIP2 => write!(f, "BZIP2"),
        }
    }
}

#[derive(Clone, Debug)]
pub enum FileHeaderInfo {
    USE,
    IGNORE,
    NONE,
}

impl fmt::Display for FileHeaderInfo {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            FileHeaderInfo::IGNORE => write!(f, "IGNORE"),
            FileHeaderInfo::USE => write!(f, "USE"),
            FileHeaderInfo::NONE => write!(f, "NONE"),
        }
    }
}

#[derive(Clone, Debug)]
pub enum JsonType {
    DOCUMENT,
    LINES,
}

impl fmt::Display for JsonType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            JsonType::DOCUMENT => write!(f, "DOCUMENT"),
            JsonType::LINES => write!(f, "LINES"),
        }
    }
}

#[derive(Clone, Debug)]
pub enum QuoteFields {
    ALWAYS,
    ASNEEDED,
}

impl fmt::Display for QuoteFields {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            QuoteFields::ALWAYS => write!(f, "ALWAYS"),
            QuoteFields::ASNEEDED => write!(f, "ASNEEDED"),
        }
    }
}

#[derive(Clone, Debug, Default)]
pub struct CsvInputSerialization {
    pub compression_type: Option<CompressionType>,
    pub allow_quoted_record_delimiter: bool,
    pub comments: Option<char>,
    pub field_delimiter: Option<char>,
    pub file_header_info: Option<FileHeaderInfo>,
    pub quote_character: Option<char>,
    pub quote_escape_character: Option<char>,
    pub record_delimiter: Option<char>,
}

#[derive(Clone, Debug, Default)]
pub struct JsonInputSerialization {
    pub compression_type: Option<CompressionType>,
    pub json_type: Option<JsonType>,
}

#[derive(Clone, Debug, Default)]
pub struct ParquetInputSerialization;

#[derive(Clone, Debug, Default)]
pub struct CsvOutputSerialization {
    pub field_delimiter: Option<char>,
    pub quote_character: Option<char>,
    pub quote_escape_character: Option<char>,
    pub quote_fields: Option<QuoteFields>,
    pub record_delimiter: Option<char>,
}

#[derive(Clone, Debug, Default)]
pub struct JsonOutputSerialization {
    pub record_delimiter: Option<char>,
}

#[derive(Clone, Debug, Default)]
pub struct SelectRequest<'a> {
    pub expr: &'a str,
    pub csv_input: Option<CsvInputSerialization>,
    pub json_input: Option<JsonInputSerialization>,
    pub parquet_input: Option<ParquetInputSerialization>,
    pub csv_output: Option<CsvOutputSerialization>,
    pub json_output: Option<JsonOutputSerialization>,
    pub request_progress: bool,
    pub scan_start_range: Option<usize>,
    pub scan_end_range: Option<usize>,
}

impl<'a> SelectRequest<'a> {
    pub fn new_csv_input_output(
        expr: &'a str,
        csv_input: CsvInputSerialization,
        csv_output: CsvOutputSerialization,
    ) -> Result<SelectRequest, Error> {
        if expr.is_empty() {
            return Err(Error::InvalidSelectExpression(String::from(
                "select expression cannot be empty",
            )));
        }

        Ok(SelectRequest {
            expr: expr,
            csv_input: Some(csv_input),
            json_input: None,
            parquet_input: None,
            csv_output: Some(csv_output),
            json_output: None,
            request_progress: false,
            scan_start_range: None,
            scan_end_range: None,
        })
    }

    pub fn new_csv_input_json_output(
        expr: &'a str,
        csv_input: CsvInputSerialization,
        json_output: JsonOutputSerialization,
    ) -> Result<SelectRequest, Error> {
        if expr.is_empty() {
            return Err(Error::InvalidSelectExpression(String::from(
                "select expression cannot be empty",
            )));
        }

        Ok(SelectRequest {
            expr: expr,
            csv_input: Some(csv_input),
            json_input: None,
            parquet_input: None,
            csv_output: None,
            json_output: Some(json_output),
            request_progress: false,
            scan_start_range: None,
            scan_end_range: None,
        })
    }

    pub fn new_json_input_output(
        expr: &'a str,
        json_input: JsonInputSerialization,
        json_output: JsonOutputSerialization,
    ) -> Result<SelectRequest, Error> {
        if expr.is_empty() {
            return Err(Error::InvalidSelectExpression(String::from(
                "select expression cannot be empty",
            )));
        }

        Ok(SelectRequest {
            expr: expr,
            csv_input: None,
            json_input: Some(json_input),
            parquet_input: None,
            csv_output: None,
            json_output: Some(json_output),
            request_progress: false,
            scan_start_range: None,
            scan_end_range: None,
        })
    }

    pub fn new_parquet_input_csv_output(
        expr: &'a str,
        parquet_input: ParquetInputSerialization,
        csv_output: CsvOutputSerialization,
    ) -> Result<SelectRequest, Error> {
        if expr.is_empty() {
            return Err(Error::InvalidSelectExpression(String::from(
                "select expression cannot be empty",
            )));
        }

        Ok(SelectRequest {
            expr: expr,
            csv_input: None,
            json_input: None,
            parquet_input: Some(parquet_input),
            csv_output: Some(csv_output),
            json_output: None,
            request_progress: false,
            scan_start_range: None,
            scan_end_range: None,
        })
    }

    pub fn new_parquet_input_json_output(
        expr: &'a str,
        parquet_input: ParquetInputSerialization,
        json_output: JsonOutputSerialization,
    ) -> Result<SelectRequest, Error> {
        if expr.is_empty() {
            return Err(Error::InvalidSelectExpression(String::from(
                "select expression cannot be empty",
            )));
        }

        Ok(SelectRequest {
            expr: expr,
            csv_input: None,
            json_input: None,
            parquet_input: Some(parquet_input),
            csv_output: None,
            json_output: Some(json_output),
            request_progress: false,
            scan_start_range: None,
            scan_end_range: None,
        })
    }

    pub fn to_xml(&self) -> String {
        let mut data = String::from("<SelectObjectContentRequest>");

        data.push_str("<Expression>");
        data.push_str(self.expr);
        data.push_str("</Expression>");
        data.push_str("<ExpressionType>SQL</ExpressionType>");

        data.push_str("<InputSerialization>");
        if let Some(c) = &self.csv_input {
            if let Some(v) = &c.compression_type {
                data.push_str("<CompressionType>");
                data.push_str(&v.to_string());
                data.push_str("</CompressionType>");
            }

            data.push_str("<CSV>");
            if c.allow_quoted_record_delimiter {
                data.push_str("<AllowQuotedRecordDelimiter>true</AllowQuotedRecordDelimiter>");
            }
            if let Some(v) = c.comments {
                data.push_str("<Comments>");
                data.push_str(&v.to_string());
                data.push_str("</Comments>");
            }
            if let Some(v) = c.field_delimiter {
                data.push_str("<FieldDelimiter>");
                data.push_str(&v.to_string());
                data.push_str("</FieldDelimiter>");
            }
            if let Some(v) = &c.file_header_info {
                data.push_str("<FileHeaderInfo>");
                data.push_str(&v.to_string());
                data.push_str("</FileHeaderInfo>");
            }
            if let Some(v) = c.quote_character {
                data.push_str("<QuoteCharacter>");
                data.push_str(&v.to_string());
                data.push_str("</QuoteCharacter>");
            }
            if let Some(v) = c.record_delimiter {
                data.push_str("<RecordDelimiter>");
                data.push_str(&v.to_string());
                data.push_str("</RecordDelimiter>");
            }
            data.push_str("</CSV>");
        } else if let Some(j) = &self.json_input {
            if let Some(v) = &j.compression_type {
                data.push_str("<CompressionType>");
                data.push_str(&v.to_string());
                data.push_str("</CompressionType>");
            }
            data.push_str("<JSON>");
            if let Some(v) = &j.json_type {
                data.push_str("<Type>");
                data.push_str(&v.to_string());
                data.push_str("</Type>");
            }
            data.push_str("</JSON>");
        } else if let Some(_) = &self.parquet_input {
            data.push_str("<Parquet></Parquet>");
        }
        data.push_str("</InputSerialization>");

        data.push_str("<OutputSerialization>");
        if let Some(c) = &self.csv_output {
            data.push_str("<CSV>");
            if let Some(v) = c.field_delimiter {
                data.push_str("<FieldDelimiter>");
                data.push_str(&v.to_string());
                data.push_str("</FieldDelimiter>");
            }
            if let Some(v) = c.quote_character {
                data.push_str("<QuoteCharacter>");
                data.push_str(&v.to_string());
                data.push_str("</QuoteCharacter>");
            }
            if let Some(v) = c.quote_escape_character {
                data.push_str("<QuoteEscapeCharacter>");
                data.push_str(&v.to_string());
                data.push_str("</QuoteEscapeCharacter>");
            }
            if let Some(v) = &c.quote_fields {
                data.push_str("<QuoteFields>");
                data.push_str(&v.to_string());
                data.push_str("</QuoteFields>");
            }
            if let Some(v) = c.record_delimiter {
                data.push_str("<RecordDelimiter>");
                data.push_str(&v.to_string());
                data.push_str("</RecordDelimiter>");
            }
            data.push_str("</CSV>");
        } else if let Some(j) = &self.json_output {
            data.push_str("<JSON>");
            if let Some(v) = j.record_delimiter {
                data.push_str("<RecordDelimiter>");
                data.push_str(&v.to_string());
                data.push_str("</RecordDelimiter>");
            }
            data.push_str("</JSON>");
        }
        data.push_str("</OutputSerialization>");

        if self.request_progress {
            data.push_str("<RequestProgress><Enabled>true</Enabled></RequestProgress>");
        }

        if let Some(s) = self.scan_start_range {
            if let Some(e) = self.scan_end_range {
                data.push_str("<ScanRange>");
                data.push_str("<Start>");
                data.push_str(&s.to_string());
                data.push_str("</Start>");
                data.push_str("<End>");
                data.push_str(&e.to_string());
                data.push_str("</End>");
                data.push_str("</ScanRange>");
            }
        }

        data.push_str("</SelectObjectContentRequest>");
        return data;
    }
}

#[derive(Clone, Debug)]
pub struct SelectProgress {
    pub bytes_scanned: usize,
    pub bytes_progressed: usize,
    pub bytes_returned: usize,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct UserIdentity {
    #[serde(alias = "principalId")]
    pub principal_id: Option<String>,
}

pub type OwnerIdentity = UserIdentity;

#[derive(Debug, Deserialize, Serialize)]
pub struct RequestParameters {
    #[serde(alias = "principalId")]
    pub principal_id: Option<String>,
    #[serde(alias = "region")]
    pub region: Option<String>,
    #[serde(alias = "sourceIPAddress")]
    pub source_ip_address: Option<String>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct ResponseElements {
    #[serde(alias = "content-length")]
    pub content_length: Option<String>,
    #[serde(alias = "x-amz-request-id")]
    pub x_amz_request_id: Option<String>,
    #[serde(alias = "x-minio-deployment-id")]
    pub x_minio_deployment_id: Option<String>,
    #[serde(alias = "x-minio-origin-endpoint")]
    pub x_minio_origin_endpoint: Option<String>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct S3Bucket {
    #[serde(alias = "name")]
    pub name: Option<String>,
    #[serde(alias = "arn")]
    pub arn: Option<String>,
    #[serde(alias = "ownerIdentity")]
    pub owner_identity: Option<OwnerIdentity>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct S3Object {
    #[serde(alias = "key")]
    pub key: Option<String>,
    #[serde(alias = "size")]
    pub size: Option<usize>,
    #[serde(alias = "eTag")]
    pub etag: Option<String>,
    #[serde(alias = "contentType")]
    pub content_type: Option<String>,
    #[serde(alias = "userMetadata")]
    pub user_metadata: Option<HashMap<String, String>>,
    #[serde(alias = "sequencer")]
    pub sequencer: Option<String>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct S3 {
    #[serde(alias = "s3SchemaVersion")]
    pub s3_schema_version: Option<String>,
    #[serde(alias = "configurationId")]
    pub configuration_id: Option<String>,
    #[serde(alias = "bucket")]
    pub bucket: Option<S3Bucket>,
    #[serde(alias = "object")]
    pub object: Option<S3Object>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct Source {
    #[serde(alias = "host")]
    pub host: Option<String>,
    #[serde(alias = "port")]
    pub port: Option<String>,
    #[serde(alias = "userAgent")]
    pub user_agent: Option<String>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct NotificationRecord {
    #[serde(alias = "eventVersion")]
    pub event_version: Option<String>,
    #[serde(alias = "eventSource")]
    pub event_source: Option<String>,
    #[serde(alias = "awsRegion")]
    pub aws_region: Option<String>,
    #[serde(alias = "eventTime")]
    pub event_time: Option<String>,
    #[serde(alias = "eventName")]
    pub event_name: Option<String>,
    #[serde(alias = "userIdentity")]
    pub user_identity: Option<UserIdentity>,
    #[serde(alias = "requestParameters")]
    pub request_parameters: Option<RequestParameters>,
    #[serde(alias = "responseElements")]
    pub response_elements: Option<ResponseElements>,
    #[serde(alias = "s3")]
    pub s3: Option<S3>,
    #[serde(alias = "source")]
    pub source: Option<Source>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct NotificationRecords {
    #[serde(alias = "Records")]
    pub records: Vec<NotificationRecord>,
}

#[derive(Clone, Debug)]
pub enum Directive {
    Copy,
    Replace,
}

impl Directive {
    pub fn parse(s: &str) -> Result<Directive, Error> {
        match s {
            "COPY" => Ok(Directive::Copy),
            "REPLACE" => Ok(Directive::Replace),
            _ => Err(Error::InvalidDirective(s.to_string())),
        }
    }
}

impl fmt::Display for Directive {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Directive::Copy => write!(f, "COPY"),
            Directive::Replace => write!(f, "REPLACE"),
        }
    }
}

#[derive(Clone, Debug)]
pub struct SseConfig {
    pub sse_algorithm: String,
    pub kms_master_key_id: Option<String>,
}

impl SseConfig {
    pub fn s3() -> SseConfig {
        SseConfig {
            sse_algorithm: String::from("AES256"),
            kms_master_key_id: None,
        }
    }

    pub fn kms(kms_master_key_id: Option<String>) -> SseConfig {
        SseConfig {
            sse_algorithm: String::from("aws:kms"),
            kms_master_key_id: kms_master_key_id,
        }
    }

    pub fn to_xml(&self) -> String {
        let mut data = String::from(
            "<ServerSideEncryptionConfiguration><Rule><ApplyServerSideEncryptionByDefault>",
        );
        data.push_str("<SSEAlgorithm>");
        data.push_str(&self.sse_algorithm);
        data.push_str("</SSEAlgorithm>");
        if self.kms_master_key_id.is_some() {
            data.push_str("<KMSMasterKeyID>");
            data.push_str(self.kms_master_key_id.as_ref().unwrap());
            data.push_str("</KMSMasterKeyID>");
        }
        data.push_str(
            "</ApplyServerSideEncryptionByDefault></Rule></ServerSideEncryptionConfiguration>",
        );
        return data;
    }
}

#[derive(Clone, Debug)]
pub struct Tag {
    pub key: String,
    pub value: String,
}

#[derive(Clone, Debug)]
pub struct AndOperator {
    pub prefix: Option<String>,
    pub tags: Option<HashMap<String, String>>,
}

#[derive(Clone, Debug)]
pub struct Filter {
    pub and_operator: Option<AndOperator>,
    pub prefix: Option<String>,
    pub tag: Option<Tag>,
}

impl Filter {
    pub fn from_xml(element: &Element) -> Result<Filter, Error> {
        let and_operator = match element.get_child("And") {
            Some(v) => Some(AndOperator {
                prefix: match v.get_child("Prefix") {
                    Some(p) => Some(
                        p.get_text()
                            .ok_or(Error::XmlError(format!("text of <Prefix> tag not found")))?
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
                                .ok_or(Error::XmlError(format!("<Tag> element not found")))?;
                            map.insert(get_text(tag, "Key")?, get_text(tag, "Value")?);
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
                    .ok_or(Error::XmlError(format!("text of <Prefix> tag not found")))?
                    .to_string(),
            ),
            None => None,
        };

        let tag = match element.get_child("Tag") {
            Some(v) => Some(Tag {
                key: get_text(v, "Key")?,
                value: get_text(v, "Value")?,
            }),
            None => None,
        };

        Ok(Filter {
            and_operator: and_operator,
            prefix: prefix,
            tag: tag,
        })
    }

    pub fn validate(&self) -> Result<(), Error> {
        if self.and_operator.is_some() ^ self.prefix.is_some() ^ self.tag.is_some() {
            return Ok(());
        }
        return Err(Error::InvalidFilter);
    }

    pub fn to_xml(&self) -> String {
        let mut data = String::from("<Filter>");
        if self.and_operator.is_some() {
            data.push_str("<And>");
            if self.and_operator.as_ref().unwrap().prefix.is_some() {
                data.push_str("<Prefix>");
                data.push_str(&self.and_operator.as_ref().unwrap().prefix.as_ref().unwrap());
                data.push_str("</Prefix>");
            }
            if self.and_operator.as_ref().unwrap().tags.is_some() {
                for (key, value) in self.and_operator.as_ref().unwrap().tags.as_ref().unwrap() {
                    data.push_str("<Tag>");
                    data.push_str("<Key>");
                    data.push_str(&key);
                    data.push_str("</Key>");
                    data.push_str("<Value>");
                    data.push_str(&value);
                    data.push_str("</Value>");
                    data.push_str("</Tag>");
                }
            }
            data.push_str("</And>");
        }
        if self.prefix.is_some() {
            data.push_str("<Prefix>");
            data.push_str(&self.prefix.as_ref().unwrap());
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

        return data;
    }
}

#[derive(Clone, Debug)]
pub struct LifecycleRule {
    pub abort_incomplete_multipart_upload_days_after_initiation: Option<usize>,
    pub expiration_date: Option<UtcTime>,
    pub expiration_days: Option<usize>,
    pub expiration_expired_object_delete_marker: Option<bool>,
    pub filter: Filter,
    pub id: String,
    pub noncurrent_version_expiration_noncurrent_days: Option<usize>,
    pub noncurrent_version_transition_noncurrent_days: Option<usize>,
    pub noncurrent_version_transition_storage_class: Option<String>,
    pub status: bool,
    pub transition_date: Option<UtcTime>,
    pub transition_days: Option<usize>,
    pub transition_storage_class: Option<String>,
}

impl LifecycleRule {
    pub fn from_xml(element: &Element) -> Result<LifecycleRule, Error> {
        let expiration = element.get_child("Expiration");
        let noncurrent_version_transition = element.get_child("NoncurrentVersionTransition");
        let transition = element.get_child("Transition");

        Ok(LifecycleRule {
            abort_incomplete_multipart_upload_days_after_initiation: match element
                .get_child("AbortIncompleteMultipartUpload")
            {
                Some(v) => {
                    let text = get_text(v, "DaysAfterInitiation")?;
                    Some(text.parse::<usize>()?)
                }
                None => None,
            },
            expiration_date: match expiration {
                Some(v) => {
                    let text = get_text(v, "Date")?;
                    Some(from_iso8601utc(&text)?)
                }
                None => None,
            },
            expiration_days: match expiration {
                Some(v) => {
                    let text = get_text(v, "Days")?;
                    Some(text.parse::<usize>()?)
                }
                None => None,
            },
            expiration_expired_object_delete_marker: match expiration {
                Some(v) => Some(get_text(v, "ExpiredObjectDeleteMarker")?.to_lowercase() == "true"),
                None => None,
            },
            filter: Filter::from_xml(
                element
                    .get_child("Filter")
                    .ok_or(Error::XmlError(format!("<Filter> tag not found")))?,
            )?,
            id: get_default_text(element, "ID"),
            noncurrent_version_expiration_noncurrent_days: match element
                .get_child("NoncurrentVersionExpiration")
            {
                Some(v) => {
                    let text = get_text(v, "NoncurrentDays")?;
                    Some(text.parse::<usize>()?)
                }
                None => None,
            },
            noncurrent_version_transition_noncurrent_days: match noncurrent_version_transition {
                Some(v) => {
                    let text = get_text(v, "NoncurrentDays")?;
                    Some(text.parse::<usize>()?)
                }
                None => None,
            },
            noncurrent_version_transition_storage_class: match noncurrent_version_transition {
                Some(v) => Some(get_text(v, "StorageClass")?),
                None => None,
            },
            status: get_text(element, "Status")?.to_lowercase() == "Enabled",
            transition_date: match transition {
                Some(v) => {
                    let text = get_text(v, "Date")?;
                    Some(from_iso8601utc(&text)?)
                }
                None => None,
            },
            transition_days: match transition {
                Some(v) => {
                    let text = get_text(v, "Days")?;
                    Some(text.parse::<usize>()?)
                }
                None => None,
            },
            transition_storage_class: match transition {
                Some(v) => Some(get_text(v, "StorageClass")?),
                None => None,
            },
        })
    }

    pub fn validate(&self) -> Result<(), Error> {
        if self
            .abort_incomplete_multipart_upload_days_after_initiation
            .is_none()
            && self.expiration_date.is_none()
            && self.expiration_days.is_none()
            && self.expiration_expired_object_delete_marker.is_none()
            && self.noncurrent_version_expiration_noncurrent_days.is_none()
            && self.noncurrent_version_transition_storage_class.is_none()
            && self.transition_date.is_none()
            && self.transition_days.is_none()
            && self.transition_storage_class.is_none()
        {
            return Err(Error::MissingLifecycleAction);
        }

        self.filter.validate()?;

        if self.expiration_expired_object_delete_marker.is_some() {
            if self.expiration_date.is_some() || self.expiration_days.is_some() {
                return Err(Error::InvalidExpiredObjectDeleteMarker);
            }
        } else if self.expiration_date.is_some() && self.expiration_days.is_some() {
            return Err(Error::InvalidDateAndDays(String::from("expiration")));
        }

        if self.transition_date.is_some() && self.transition_days.is_some() {
            return Err(Error::InvalidDateAndDays(String::from("transition")));
        }

        if self.id.len() > 255 {
            return Err(Error::InvalidLifecycleRuleId);
        }

        Ok(())
    }
}

#[derive(Clone, Debug)]
pub struct LifecycleConfig {
    pub rules: Vec<LifecycleRule>,
}

impl LifecycleConfig {
    pub fn from_xml(root: &Element) -> Result<LifecycleConfig, Error> {
        let mut config = LifecycleConfig { rules: Vec::new() };

        if let Some(v) = root.get_child("Rule") {
            for rule in &v.children {
                config.rules.push(LifecycleRule::from_xml(
                    rule.as_element()
                        .ok_or(Error::XmlError(format!("<Rule> tag not found")))?,
                )?);
            }
        }

        return Ok(config);
    }

    pub fn validate(&self) -> Result<(), Error> {
        for rule in &self.rules {
            rule.validate()?;
        }

        Ok(())
    }

    pub fn to_xml(&self) -> String {
        let mut data = String::from("<LifecycleConfiguration>");

        for rule in &self.rules {
            data.push_str("<Rule>");

            if rule
                .abort_incomplete_multipart_upload_days_after_initiation
                .is_some()
            {
                data.push_str("<AbortIncompleteMultipartUpload><DaysAfterInitiation>");
                data.push_str(
                    &rule
                        .abort_incomplete_multipart_upload_days_after_initiation
                        .unwrap()
                        .to_string(),
                );
                data.push_str("</DaysAfterInitiation></AbortIncompleteMultipartUpload>");
            }

            if rule.expiration_date.is_some()
                || rule.expiration_days.is_some()
                || rule.expiration_expired_object_delete_marker.is_some()
            {
                data.push_str("<Expiration>");
                if rule.expiration_date.is_some() {
                    data.push_str("<Date>");
                    data.push_str(&to_iso8601utc(rule.expiration_date.unwrap()));
                    data.push_str("</Date>");
                }
                if rule.expiration_days.is_some() {
                    data.push_str("<Days>");
                    data.push_str(&rule.expiration_days.unwrap().to_string());
                    data.push_str("</Days>");
                }
                if rule.expiration_expired_object_delete_marker.is_some() {
                    data.push_str("<ExpiredObjectDeleteMarker>");
                    data.push_str(
                        &rule
                            .expiration_expired_object_delete_marker
                            .unwrap()
                            .to_string(),
                    );
                    data.push_str("</ExpiredObjectDeleteMarker>");
                }
                data.push_str("</Expiration>");
            }

            data.push_str(&rule.filter.to_xml());

            if !rule.id.is_empty() {
                data.push_str("<ID>");
                data.push_str(&rule.id);
                data.push_str("</ID>");
            }

            if rule.noncurrent_version_expiration_noncurrent_days.is_some() {
                data.push_str("<NoncurrentVersionExpiration><NoncurrentDays>");
                data.push_str(
                    &rule
                        .noncurrent_version_expiration_noncurrent_days
                        .unwrap()
                        .to_string(),
                );
                data.push_str("</NoncurrentDays></NoncurrentVersionExpiration>");
            }

            if rule.noncurrent_version_transition_noncurrent_days.is_some()
                || rule.noncurrent_version_transition_storage_class.is_some()
            {
                data.push_str("<NoncurrentVersionTransition>");
                if rule.noncurrent_version_transition_noncurrent_days.is_some() {
                    data.push_str("<NoncurrentDays>");
                    data.push_str(
                        &rule
                            .noncurrent_version_expiration_noncurrent_days
                            .unwrap()
                            .to_string(),
                    );
                    data.push_str("</NoncurrentDays>");
                }
                if rule.noncurrent_version_transition_storage_class.is_some() {
                    data.push_str("<StorageClass>");
                    data.push_str(
                        &rule
                            .noncurrent_version_transition_storage_class
                            .as_ref()
                            .unwrap(),
                    );
                    data.push_str("</StorageClass>");
                }
                data.push_str("</NoncurrentVersionTransition>");
            }

            data.push_str("<Status>");
            if rule.status {
                data.push_str("Enabled");
            } else {
                data.push_str("Disabled");
            }
            data.push_str("</Status>");

            if rule.transition_date.is_some()
                || rule.transition_days.is_some()
                || rule.transition_storage_class.is_some()
            {
                data.push_str("<Transition>");
                if rule.transition_date.is_some() {
                    data.push_str("<Date>");
                    data.push_str(&to_iso8601utc(rule.transition_date.unwrap()));
                    data.push_str("</Date>");
                }
                if rule.transition_days.is_some() {
                    data.push_str("<Days>");
                    data.push_str(&rule.transition_days.unwrap().to_string());
                    data.push_str("</Days>");
                }
                if rule.transition_storage_class.is_some() {
                    data.push_str("<StorageClass>");
                    data.push_str(&rule.transition_storage_class.as_ref().unwrap());
                    data.push_str("</StorageClass>");
                }
                data.push_str("</Transition>");
            }

            data.push_str("</Rule>");
        }

        data.push_str("</LifecycleConfiguration>");

        return data;
    }
}

fn parse_common_notification_config(
    element: &mut Element,
) -> Result<
    (
        Vec<String>,
        Option<String>,
        Option<PrefixFilterRule>,
        Option<SuffixFilterRule>,
    ),
    Error,
> {
    let mut events = Vec::new();
    loop {
        match element.take_child("Event") {
            Some(v) => events.push(
                v.get_text()
                    .ok_or(Error::XmlError(format!("text of <Event> tag not found")))?
                    .to_string(),
            ),
            _ => break,
        }
    }

    let id = get_option_text(element, "Id");

    let (prefix_filter_rule, suffix_filter_rule) = match element.get_child("Filter") {
        Some(filter) => {
            let mut prefix = None;
            let mut suffix = None;
            let rules = filter
                .get_child("S3Key")
                .ok_or(Error::XmlError(format!("<S3Key> tag not found")))?;
            for rule in &rules.children {
                let v = rule
                    .as_element()
                    .ok_or(Error::XmlError(format!("<FilterRule> tag not found")))?;
                let name = get_text(v, "Name")?;
                let value = get_text(v, "Value")?;
                if PrefixFilterRule::NAME == name {
                    prefix = Some(PrefixFilterRule { value: value });
                } else {
                    suffix = Some(SuffixFilterRule { value: value });
                }
            }
            (prefix, suffix)
        }
        _ => (None, None),
    };

    Ok((events, id, prefix_filter_rule, suffix_filter_rule))
}

fn to_xml_common_notification_config(
    events: &Vec<String>,
    id: &Option<String>,
    prefix_filter_rule: &Option<PrefixFilterRule>,
    suffix_filter_rule: &Option<SuffixFilterRule>,
) -> String {
    let mut data = String::new();

    for event in events {
        data.push_str("<Event>");
        data.push_str(&event);
        data.push_str("</Event>");
    }

    if let Some(v) = id {
        data.push_str("<Id>");
        data.push_str(&v);
        data.push_str("</Id>");
    }

    if prefix_filter_rule.is_some() || suffix_filter_rule.is_some() {
        data.push_str("<Filter><S3Key>");

        if let Some(v) = prefix_filter_rule {
            data.push_str("<FilterRule><Name>prefix</Name>");
            data.push_str("<Value>");
            data.push_str(&v.value);
            data.push_str("</Value></FilterRule>");
        }

        if let Some(v) = suffix_filter_rule {
            data.push_str("<FilterRule><Name>suffix</Name>");
            data.push_str("<Value>");
            data.push_str(&v.value);
            data.push_str("</Value></FilterRule>");
        }

        data.push_str("</S3Key></Filter>");
    }

    return data;
}

#[derive(Clone, Debug)]
pub struct PrefixFilterRule {
    pub value: String,
}

impl PrefixFilterRule {
    pub const NAME: &str = "prefix";
}

#[derive(Clone, Debug)]
pub struct SuffixFilterRule {
    pub value: String,
}

impl SuffixFilterRule {
    pub const NAME: &str = "suffix";
}

#[derive(Clone, Debug)]
pub struct CloudFuncConfig {
    pub events: Vec<String>,
    pub id: Option<String>,
    pub prefix_filter_rule: Option<PrefixFilterRule>,
    pub suffix_filter_rule: Option<SuffixFilterRule>,
    pub cloud_func: String,
}

impl CloudFuncConfig {
    pub fn from_xml(element: &mut Element) -> Result<CloudFuncConfig, Error> {
        let (events, id, prefix_filter_rule, suffix_filter_rule) =
            parse_common_notification_config(element)?;
        Ok(CloudFuncConfig {
            events: events,
            id: id,
            prefix_filter_rule: prefix_filter_rule,
            suffix_filter_rule: suffix_filter_rule,
            cloud_func: get_text(element, "CloudFunction")?,
        })
    }

    pub fn validate(&self) -> Result<(), Error> {
        if self.events.len() != 0 && self.cloud_func != "" {
            return Ok(());
        }

        return Err(Error::InvalidFilter);
    }

    pub fn to_xml(&self) -> String {
        let mut data = String::from("<CloudFunctionConfiguration>");

        data.push_str("<CloudFunction>");
        data.push_str(&self.cloud_func);
        data.push_str("</CloudFunction>");

        data.push_str(&to_xml_common_notification_config(
            &self.events,
            &self.id,
            &self.prefix_filter_rule,
            &self.suffix_filter_rule,
        ));

        data.push_str("</CloudFunctionConfiguration>");

        return data;
    }
}

#[derive(Clone, Debug)]
pub struct QueueConfig {
    pub events: Vec<String>,
    pub id: Option<String>,
    pub prefix_filter_rule: Option<PrefixFilterRule>,
    pub suffix_filter_rule: Option<SuffixFilterRule>,
    pub queue: String,
}

impl QueueConfig {
    pub fn from_xml(element: &mut Element) -> Result<QueueConfig, Error> {
        let (events, id, prefix_filter_rule, suffix_filter_rule) =
            parse_common_notification_config(element)?;
        Ok(QueueConfig {
            events: events,
            id: id,
            prefix_filter_rule: prefix_filter_rule,
            suffix_filter_rule: suffix_filter_rule,
            queue: get_text(element, "Queue")?,
        })
    }

    pub fn validate(&self) -> Result<(), Error> {
        if self.events.len() != 0 && self.queue != "" {
            return Ok(());
        }

        return Err(Error::InvalidFilter);
    }

    pub fn to_xml(&self) -> String {
        let mut data = String::from("<QueueConfiguration>");

        data.push_str("<Queue>");
        data.push_str(&self.queue);
        data.push_str("</Queue>");

        data.push_str(&to_xml_common_notification_config(
            &self.events,
            &self.id,
            &self.prefix_filter_rule,
            &self.suffix_filter_rule,
        ));

        data.push_str("</QueueConfiguration>");

        return data;
    }
}

#[derive(Clone, Debug)]
pub struct TopicConfig {
    pub events: Vec<String>,
    pub id: Option<String>,
    pub prefix_filter_rule: Option<PrefixFilterRule>,
    pub suffix_filter_rule: Option<SuffixFilterRule>,
    pub topic: String,
}

impl TopicConfig {
    pub fn from_xml(element: &mut Element) -> Result<TopicConfig, Error> {
        let (events, id, prefix_filter_rule, suffix_filter_rule) =
            parse_common_notification_config(element)?;
        Ok(TopicConfig {
            events: events,
            id: id,
            prefix_filter_rule: prefix_filter_rule,
            suffix_filter_rule: suffix_filter_rule,
            topic: get_text(element, "Topic")?,
        })
    }

    pub fn validate(&self) -> Result<(), Error> {
        if self.events.len() != 0 && self.topic != "" {
            return Ok(());
        }

        return Err(Error::InvalidFilter);
    }

    pub fn to_xml(&self) -> String {
        let mut data = String::from("<TopicConfiguration>");

        data.push_str("<Topic>");
        data.push_str(&self.topic);
        data.push_str("</Topic>");

        data.push_str(&to_xml_common_notification_config(
            &self.events,
            &self.id,
            &self.prefix_filter_rule,
            &self.suffix_filter_rule,
        ));

        data.push_str("</TopicConfiguration>");

        return data;
    }
}

#[derive(Clone, Debug)]
pub struct NotificationConfig {
    pub cloud_func_config_list: Option<Vec<CloudFuncConfig>>,
    pub queue_config_list: Option<Vec<QueueConfig>>,
    pub topic_config_list: Option<Vec<TopicConfig>>,
}

impl NotificationConfig {
    pub fn from_xml(root: &mut Element) -> Result<NotificationConfig, Error> {
        let mut config = NotificationConfig {
            cloud_func_config_list: None,
            queue_config_list: None,
            topic_config_list: None,
        };

        let mut cloud_func_config_list = Vec::new();
        loop {
            match root.take_child("CloudFunctionConfiguration") {
                Some(mut v) => cloud_func_config_list.push(CloudFuncConfig::from_xml(&mut v)?),
                _ => break,
            }
        }
        if cloud_func_config_list.len() != 0 {
            config.cloud_func_config_list = Some(cloud_func_config_list);
        }

        let mut queue_config_list = Vec::new();
        loop {
            match root.take_child("QueueConfiguration") {
                Some(mut v) => queue_config_list.push(QueueConfig::from_xml(&mut v)?),
                _ => break,
            }
        }
        if queue_config_list.len() != 0 {
            config.queue_config_list = Some(queue_config_list);
        }

        let mut topic_config_list = Vec::new();
        loop {
            match root.take_child("TopicConfiguration") {
                Some(mut v) => topic_config_list.push(TopicConfig::from_xml(&mut v)?),
                _ => break,
            }
        }
        if topic_config_list.len() != 0 {
            config.topic_config_list = Some(topic_config_list);
        }

        return Ok(config);
    }

    pub fn validate(&self) -> Result<(), Error> {
        if let Some(v) = &self.cloud_func_config_list {
            for rule in v {
                rule.validate()?;
            }
        }

        if let Some(v) = &self.queue_config_list {
            for rule in v {
                rule.validate()?;
            }
        }

        if let Some(v) = &self.topic_config_list {
            for rule in v {
                rule.validate()?;
            }
        }

        Ok(())
    }

    pub fn to_xml(&self) -> String {
        let mut data = String::from("<NotificationConfiguration>");

        if let Some(v) = &self.cloud_func_config_list {
            for rule in v {
                data.push_str(&rule.to_xml())
            }
        }

        if let Some(v) = &self.queue_config_list {
            for rule in v {
                data.push_str(&rule.to_xml())
            }
        }

        if let Some(v) = &self.topic_config_list {
            for rule in v {
                data.push_str(&rule.to_xml())
            }
        }

        data.push_str("</NotificationConfiguration>");
        return data;
    }
}

#[derive(Clone, Debug)]
pub struct AccessControlTranslation {
    pub owner: String,
}

impl AccessControlTranslation {
    pub fn new() -> AccessControlTranslation {
        AccessControlTranslation {
            owner: String::from("Destination"),
        }
    }
}

#[derive(Clone, Debug)]
pub struct EncryptionConfig {
    pub replica_kms_key_id: Option<String>,
}

#[derive(Clone, Debug)]
pub struct Metrics {
    pub event_threshold_minutes: Option<i32>,
    pub status: bool,
}

impl Metrics {
    pub fn new(status: bool) -> Metrics {
        Metrics {
            event_threshold_minutes: Some(15),
            status: status,
        }
    }
}

#[derive(Clone, Debug)]
pub struct ReplicationTime {
    pub time_minutes: Option<i32>,
    pub status: bool,
}

impl ReplicationTime {
    pub fn new(status: bool) -> ReplicationTime {
        ReplicationTime {
            time_minutes: Some(15),
            status: status,
        }
    }
}

#[derive(Clone, Debug)]
pub struct Destination {
    pub bucket_arn: String,
    pub access_control_translation: Option<AccessControlTranslation>,
    pub account: Option<String>,
    pub encryption_config: Option<EncryptionConfig>,
    pub metrics: Option<Metrics>,
    pub replication_time: Option<ReplicationTime>,
    pub storage_class: Option<String>,
}

impl Destination {
    pub fn from_xml(element: &Element) -> Result<Destination, Error> {
        Ok(Destination {
            bucket_arn: get_text(element, "Bucket")?,
            access_control_translation: match element.get_child("AccessControlTranslation") {
                Some(v) => Some(AccessControlTranslation {
                    owner: get_text(v, "Owner")?,
                }),
                _ => None,
            },
            account: get_option_text(element, "Account"),
            encryption_config: match element.get_child("EncryptionConfiguration") {
                Some(v) => Some(EncryptionConfig {
                    replica_kms_key_id: get_option_text(v, "ReplicaKmsKeyID"),
                }),
                _ => None,
            },
            metrics: match element.get_child("Metrics") {
                Some(v) => Some(Metrics {
                    event_threshold_minutes: match get_option_text(
                        v.get_child("EventThreshold")
                            .ok_or(Error::XmlError(format!("<Metrics> tag not found")))?,
                        "Minutes",
                    ) {
                        Some(v) => Some(v.parse::<i32>()?),
                        _ => None,
                    },
                    status: get_text(v, "Status")? == "Enabled",
                }),
                _ => None,
            },
            replication_time: match element.get_child("ReplicationTime") {
                Some(v) => Some(ReplicationTime {
                    time_minutes: match get_option_text(v, "Time") {
                        Some(v) => Some(v.parse::<i32>()?),
                        _ => None,
                    },
                    status: get_text(v, "Status")? == "Enabled",
                }),
                _ => None,
            },
            storage_class: get_option_text(element, "StorageClass"),
        })
    }

    pub fn to_xml(&self) -> String {
        let mut data = String::from("<Destination>");

        data.push_str("<Bucket>");
        data.push_str(&self.bucket_arn);
        data.push_str("</Bucket>");

        if let Some(v) = &self.access_control_translation {
            data.push_str("<AccessControlTranslation><Owner>");
            data.push_str(&v.owner);
            data.push_str("</Owner></AccessControlTranslation>");
        }

        if let Some(v) = &self.account {
            data.push_str("<Account>");
            data.push_str(&v);
            data.push_str("</Account>");
        }

        if let Some(c) = &self.encryption_config {
            data.push_str("<EncryptionConfiguration>");
            if let Some(v) = &c.replica_kms_key_id {
                data.push_str("<ReplicaKmsKeyID>");
                data.push_str(&v);
                data.push_str("</ReplicaKmsKeyID>");
            }
            data.push_str("</EncryptionConfiguration>");
        }

        if let Some(m) = &self.metrics {
            data.push_str("<Metrics><EventThreshold>");

            if let Some(v) = m.event_threshold_minutes {
                data.push_str("<Minutes>");
                data.push_str(&v.to_string());
                data.push_str("</Minutes>");
            }

            data.push_str("<Status>");
            data.push_str(match m.status {
                true => "Enabled",
                false => "Disabled",
            });
            data.push_str("</Status>");

            data.push_str("</EventThreshold></Metrics>");
        }

        if let Some(t) = &self.replication_time {
            data.push_str("<ReplicationTime>");

            data.push_str("<Time>");
            if let Some(v) = t.time_minutes {
                data.push_str(&v.to_string());
            }
            data.push_str("</Time>");

            data.push_str("<Status>");
            data.push_str(match t.status {
                true => "Enabled",
                false => "Disabled",
            });
            data.push_str("</Status>");

            data.push_str("</ReplicationTime>");
        }

        if let Some(v) = &self.storage_class {
            data.push_str("<StorageClass>");
            data.push_str(&v);
            data.push_str("</StorageClass>");
        }

        data.push_str("</Destination>");

        return data;
    }
}

#[derive(Clone, Debug)]
pub struct SourceSelectionCriteria {
    pub sse_kms_encrypted_objects_status: Option<bool>,
}

#[derive(Clone, Debug)]
pub struct ReplicationRule {
    pub destination: Destination,
    pub delete_marker_replication_status: Option<bool>,
    pub existing_object_replication_status: Option<bool>,
    pub filter: Option<Filter>,
    pub id: Option<String>,
    pub prefix: Option<String>,
    pub priority: Option<i32>,
    pub source_selection_criteria: Option<SourceSelectionCriteria>,
    pub delete_replication_status: Option<bool>,
    pub status: bool,
}

impl ReplicationRule {
    pub fn from_xml(element: &Element) -> Result<ReplicationRule, Error> {
        Ok(ReplicationRule {
            destination: Destination::from_xml(
                element
                    .get_child("Destination")
                    .ok_or(Error::XmlError(format!("<Destination> tag not found")))?,
            )?,
            delete_marker_replication_status: match element.get_child("DeleteMarkerReplication") {
                Some(v) => Some(get_text(v, "Status")? == "Enabled"),
                _ => None,
            },
            existing_object_replication_status: match element.get_child("ExistingObjectReplication")
            {
                Some(v) => Some(get_text(v, "Status")? == "Enabled"),
                _ => None,
            },
            filter: match element.get_child("Filter") {
                Some(v) => Some(Filter::from_xml(v)?),
                _ => None,
            },
            id: get_option_text(element, "ID"),
            prefix: get_option_text(element, "Prefix"),
            priority: match get_option_text(element, "Priority") {
                Some(v) => Some(v.parse::<i32>()?),
                _ => None,
            },
            source_selection_criteria: match element.get_child("SourceSelectionCriteria") {
                Some(v) => match v.get_child("SseKmsEncryptedObjects") {
                    Some(v) => Some(SourceSelectionCriteria {
                        sse_kms_encrypted_objects_status: Some(get_text(v, "Status")? == "Enabled"),
                    }),
                    _ => Some(SourceSelectionCriteria {
                        sse_kms_encrypted_objects_status: None,
                    }),
                },
                _ => None,
            },
            delete_replication_status: match element.get_child("DeleteReplication") {
                Some(v) => Some(get_text(v, "Status")? == "Enabled"),
                _ => None,
            },
            status: get_text(element, "Status")? == "Enabled",
        })
    }

    pub fn to_xml(&self) -> String {
        let mut data = self.destination.to_xml();

        if let Some(v) = self.delete_marker_replication_status {
            data.push_str("<DeleteMarkerReplication>");
            data.push_str("<Status>");
            data.push_str(match v {
                true => "Enabled",
                false => "Disabled",
            });
            data.push_str("</Status>");
            data.push_str("</DeleteMarkerReplication>");
        }

        if let Some(v) = self.existing_object_replication_status {
            data.push_str("<ExistingObjectReplication>");
            data.push_str("<Status>");
            data.push_str(match v {
                true => "Enabled",
                false => "Disabled",
            });
            data.push_str("</Status>");
            data.push_str("</ExistingObjectReplication>");
        }

        if let Some(v) = &self.filter {
            data.push_str(&v.to_xml())
        }

        if let Some(v) = &self.id {
            data.push_str("<ID>");
            data.push_str(&v);
            data.push_str("</ID>");
        }

        if let Some(v) = &self.prefix {
            data.push_str("<Prefix>");
            data.push_str(&v);
            data.push_str("</Prefix>");
        }

        if let Some(v) = self.priority {
            data.push_str("<Priority>");
            data.push_str(&v.to_string());
            data.push_str("</Priority>");
        }

        if let Some(s) = &self.source_selection_criteria {
            data.push_str("<SourceSelectionCriteria>");
            if let Some(v) = s.sse_kms_encrypted_objects_status {
                data.push_str("<SseKmsEncryptedObjects>");
                data.push_str("<Status>");
                data.push_str(match v {
                    true => "Enabled",
                    false => "Disabled",
                });
                data.push_str("</Status>");
                data.push_str("</SseKmsEncryptedObjects>");
            }
            data.push_str("</SourceSelectionCriteria>");
        }

        if let Some(v) = self.delete_replication_status {
            data.push_str("<DeleteReplication>");
            data.push_str("<Status>");
            data.push_str(match v {
                true => "Enabled",
                false => "Disabled",
            });
            data.push_str("</Status>");
            data.push_str("</DeleteReplication>");
        }

        data.push_str("<Status>");
        data.push_str(match self.status {
            true => "Enabled",
            false => "Disabled",
        });
        data.push_str("</Status>");

        return data;
    }
}

#[derive(Clone, Debug)]
pub struct ReplicationConfig {
    pub role: Option<String>,
    pub rules: Vec<ReplicationRule>,
}

impl ReplicationConfig {
    pub fn from_xml(root: &Element) -> Result<ReplicationConfig, Error> {
        let mut config = ReplicationConfig {
            role: get_option_text(root, "Role"),
            rules: Vec::new(),
        };

        if let Some(v) = root.get_child("Rule") {
            for rule in &v.children {
                config.rules.push(ReplicationRule::from_xml(
                    rule.as_element()
                        .ok_or(Error::XmlError(format!("<Rule> tag not found")))?,
                )?);
            }
        }

        return Ok(config);
    }

    pub fn to_xml(&self) -> String {
        let mut data = String::from("<ReplicationConfiguration>");

        if let Some(v) = &self.role {
            data.push_str("<Status>");
            data.push_str(&v);
            data.push_str("</Status>");
        }

        for rule in &self.rules {
            data.push_str(&rule.to_xml());
        }

        data.push_str("</ReplicationConfiguration>");
        return data;
    }
}

#[derive(Clone, Debug)]
pub struct ObjectLockConfig {
    pub retention_mode: Option<RetentionMode>,
    pub retention_duration_days: Option<i32>,
    pub retention_duration_years: Option<i32>,
}

impl ObjectLockConfig {
    pub fn new(
        mode: RetentionMode,
        days: Option<i32>,
        years: Option<i32>,
    ) -> Result<ObjectLockConfig, Error> {
        if days.is_some() ^ years.is_some() {
            return Ok(ObjectLockConfig {
                retention_mode: Some(mode),
                retention_duration_days: days,
                retention_duration_years: years,
            });
        }

        Err(Error::InvalidObjectLockConfig(format!(
            "only one days or years must be set"
        )))
    }

    pub fn from_xml(root: &Element) -> Result<ObjectLockConfig, Error> {
        let mut config = ObjectLockConfig {
            retention_mode: None,
            retention_duration_days: None,
            retention_duration_years: None,
        };

        if let Some(r) = root.get_child("Rule") {
            let default_retention = r
                .get_child("DefaultRetention")
                .ok_or(Error::XmlError(format!("<DefaultRetention> tag not found")))?;
            config.retention_mode =
                Some(RetentionMode::parse(&get_text(default_retention, "Mode")?)?);

            if let Some(v) = get_option_text(default_retention, "Days") {
                config.retention_duration_days = Some(v.parse::<i32>()?);
            }

            if let Some(v) = get_option_text(default_retention, "Years") {
                config.retention_duration_years = Some(v.parse::<i32>()?);
            }
        }

        return Ok(config);
    }

    pub fn to_xml(&self) -> String {
        let mut data = String::from("<ObjectLockConfiguration>");
        data.push_str("<ObjectLockEnabled>Enabled</ObjectLockEnabled>");
        if let Some(v) = &self.retention_mode {
            data.push_str("<Rule><DefaultRetention>");
            data.push_str("<Mode>");
            data.push_str(&v.to_string());
            data.push_str("</Mode>");
            if let Some(d) = self.retention_duration_days {
                data.push_str("<Days>");
                data.push_str(&d.to_string());
                data.push_str("</Days>");
            }
            if let Some(d) = self.retention_duration_years {
                data.push_str("<Years>");
                data.push_str(&d.to_string());
                data.push_str("</Years>");
            }
            data.push_str("</DefaultRetention></Rule>");
        }
        data.push_str("</ObjectLockConfiguration>");

        return data;
    }
}
