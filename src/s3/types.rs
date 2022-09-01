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
use crate::s3::utils::UtcTime;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt;

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
