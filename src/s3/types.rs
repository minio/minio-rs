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
use crate::s3::utils::{from_iso8601utc, get_default_text, get_text, to_iso8601utc, UtcTime};
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
    Governance,
    Compliance,
}

impl RetentionMode {
    pub fn parse(s: &str) -> Result<RetentionMode, Error> {
        match s {
            "GOVERNANCE" => Ok(RetentionMode::Governance),
            "COMPLIANCE" => Ok(RetentionMode::Compliance),
            _ => Err(Error::InvalidRetentionMode(s.to_string())),
        }
    }
}

impl fmt::Display for RetentionMode {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            RetentionMode::Governance => write!(f, "GOVERNANCE"),
            RetentionMode::Compliance => write!(f, "COMPLIANCE"),
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
    pub fn parse_xml(element: &Element) -> Result<Filter, Error> {
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
            filter: Filter::parse_xml(
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

        match root.get_child("Rule") {
            Some(v) => {
                for rule in &v.children {
                    config.rules.push(LifecycleRule::from_xml(
                        rule.as_element()
                            .ok_or(Error::XmlError(format!("<Rule> tag not found")))?,
                    )?);
                }
            }
            _ => todo!(),
        };

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

            data.push_str("<Filter>");
            if rule.filter.and_operator.is_some() {
                data.push_str("<And>");
                if rule.filter.and_operator.as_ref().unwrap().prefix.is_some() {
                    data.push_str("<Prefix>");
                    data.push_str(
                        &rule
                            .filter
                            .and_operator
                            .as_ref()
                            .unwrap()
                            .prefix
                            .as_ref()
                            .unwrap(),
                    );
                    data.push_str("</Prefix>");
                }
                if rule.filter.and_operator.as_ref().unwrap().tags.is_some() {
                    for (key, value) in rule
                        .filter
                        .and_operator
                        .as_ref()
                        .unwrap()
                        .tags
                        .as_ref()
                        .unwrap()
                    {
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
            if rule.filter.prefix.is_some() {
                data.push_str("<Prefix>");
                data.push_str(&rule.filter.prefix.as_ref().unwrap());
                data.push_str("</Prefix>");
            }
            if rule.filter.tag.is_some() {
                data.push_str("<Tag>");
                data.push_str("<Key>");
                data.push_str(&rule.filter.tag.as_ref().unwrap().key);
                data.push_str("</Key>");
                data.push_str("<Value>");
                data.push_str(&rule.filter.tag.as_ref().unwrap().value);
                data.push_str("</Value>");
                data.push_str("</Tag>");
            }
            data.push_str("</Filter>");

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
