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

use super::client::{DEFAULT_REGION, MinioClient};
use crate::s3::error::{Error, ValidationErr};
use crate::s3::header_constants::*;
use crate::s3::multimap_ext::Multimap;
use crate::s3::segmented_bytes::SegmentedBytes;
use crate::s3::utils::{UtcTime, get_text_option, get_text_result};
use async_trait::async_trait;
use futures_util::Stream;
use http::Method;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;
use typed_builder::TypedBuilder;
use xmltree::Element;

#[derive(Clone, Debug, TypedBuilder)]
/// Generic S3Request
pub struct S3Request {
    #[builder(!default)] // force required
    pub(crate) client: MinioClient,

    #[builder(!default)] // force required
    method: Method,

    #[builder(default, setter(into))]
    region: Option<String>,

    #[builder(default, setter(into))]
    pub(crate) bucket: Option<String>,

    #[builder(default, setter(into))]
    pub(crate) object: Option<String>,

    #[builder(default)]
    pub(crate) query_params: Multimap,

    #[builder(default)]
    headers: Multimap,

    #[builder(default, setter(into))]
    body: Option<Arc<SegmentedBytes>>,

    /// region computed by [`S3Request::execute`]
    #[builder(default, setter(skip))]
    pub(crate) inner_region: String,
}

impl S3Request {
    async fn compute_inner_region(&self) -> Result<String, Error> {
        Ok(match &self.bucket {
            Some(b) => self.client.get_region_cached(b, &self.region).await?,
            None => DEFAULT_REGION.to_string(),
        })
    }

    /// Execute the request, returning the response. Only used in [`S3Api::send()`]
    pub async fn execute(&mut self) -> Result<reqwest::Response, Error> {
        self.inner_region = self.compute_inner_region().await?;
        self.client
            .execute(
                self.method.clone(),
                &self.inner_region,
                &mut self.headers,
                &self.query_params,
                &self.bucket.as_deref(),
                &self.object.as_deref(),
                self.body.as_ref().map(Arc::clone),
            )
            .await
    }
}

/// Trait for converting a request builder into a concrete S3 HTTP request.
///
/// This trait is implemented by all S3 request builders and serves as an
/// intermediate step in the request execution pipeline. It enables the
/// conversion from a strongly typed request builder into a generic
/// [`S3Request`] that can be executed over HTTP.
///
/// The [`S3Api::send`] method uses this trait to convert request builders
/// into executable HTTP requests before sending them to the S3-compatible
/// service.
///
/// # See Also
///
/// * [`S3Api`] - The trait that uses `ToS3Request` as part of its request execution pipeline
/// * [`FromS3Response`] - The counterpart trait for converting HTTP responses into typed responses
///
pub trait ToS3Request: Sized {
    /// Consumes this request builder and returns a [`S3Request`].
    ///
    /// This method transforms the request builder into a concrete HTTP request
    /// that can be executed against an S3-compatible service. The transformation
    /// includes:
    ///
    /// * Setting the appropriate HTTP method (GET, PUT, POST, etc.)
    /// * Building the request URL with path and query parameters
    /// * Adding required headers (authentication, content-type, etc.)
    /// * Attaching the request body, if applicable
    ///
    /// # Returns
    ///
    /// * `Result<S3Request, ValidationErr>` - The executable S3 request on success,
    ///   or an error if the request cannot be built correctly.
    ///
    fn to_s3request(self) -> Result<S3Request, ValidationErr>;
}

/// Trait for converting HTTP responses into strongly typed S3 response objects.
///
/// This trait is implemented by all S3 response types in the SDK and provides
/// a way to parse and validate raw HTTP responses from S3-compatible services.
/// It works as the final step in the request execution pipeline, transforming
/// the HTTP layer response into a domain-specific response object with proper
/// typing and field validation.
///
/// # See Also
///
/// * [`S3Api`] - The trait that uses `FromS3Response` as part of its request execution pipeline
/// * [`ToS3Request`] - The counterpart trait for converting request builders into HTTP requests
#[async_trait]
pub trait FromS3Response: Sized {
    /// Asynchronously converts an HTTP response into a strongly typed S3 response.
    ///
    /// This method takes both the original S3 request and the HTTP response (or error)
    /// that resulted from executing that request. It then parses the response data
    /// and constructs a typed response object that provides convenient access to
    /// the response fields.
    ///
    /// The method handles both successful responses and error responses from the
    /// S3 service, transforming S3-specific errors into appropriate error types.
    ///
    /// # Parameters
    ///
    /// * `s3req` - The original S3 request that was executed
    /// * `resp` - The result of the HTTP request execution, which can be either a
    ///   successful response or an error
    ///
    /// # Returns
    ///
    /// * `Result<Self, Error>` - The typed response object on success, or an error
    ///   if the response cannot be parsed or represents an S3 service error
    ///
    async fn from_s3response(
        s3req: S3Request,
        response: Result<reqwest::Response, Error>,
    ) -> Result<Self, Error>;
}

/// Trait that defines a common interface for all S3 API request builders.
///
/// This trait is implemented by all request builders in the SDK and provides
/// a consistent way to send requests and get typed responses. It works in
/// conjunction with [`ToS3Request`] to convert the builder into a concrete
/// HTTP request and with [`FromS3Response`] to convert the HTTP response back
/// into a strongly typed S3 response object.
///
/// # Type Parameters
///
/// * `S3Response` - The specific response type associated with this request builder.
///   Must implement the [`FromS3Response`] trait.
///
#[async_trait]
pub trait S3Api: ToS3Request {
    /// The response type associated with this request builder.
    ///
    /// Each implementation of `S3Api` defines its own response type that will be
    /// returned by the `send()` method. This type must implement the [`FromS3Response`]
    /// trait to enable conversion from the raw HTTP response.
    type S3Response: FromS3Response;
    /// Sends the S3 API request and returns the corresponding typed response.
    ///
    /// This method consumes the request builder, converts it into a concrete HTTP
    /// request using [`ToS3Request::to_s3request`], executes the request, and then
    /// converts the HTTP response into the appropriate typed response using
    /// [`FromS3Response::from_s3response`].
    ///
    /// # Returns
    ///
    /// * `Result<Self::S3Response, Error>` - The typed S3 response on success,
    ///   or an error if the request failed at any stage.    
    ///
    async fn send(self) -> Result<Self::S3Response, Error> {
        let mut req: S3Request = self.to_s3request()?;
        let resp: Result<reqwest::Response, Error> = req.execute().await;
        Self::S3Response::from_s3response(req, resp).await
    }
}

#[async_trait]
pub trait ToStream: Sized {
    type Item;
    async fn to_stream(self) -> Box<dyn Stream<Item = Result<Self::Item, Error>> + Unpin + Send>;
}

#[derive(Clone, Debug)]
/// Contains information of an item of [list_objects()](crate::s3::client::MinioClient::list_objects) API
pub struct ListEntry {
    pub name: String,
    pub last_modified: Option<UtcTime>,
    pub etag: Option<String>, // except DeleteMarker
    pub owner_id: Option<String>,
    pub owner_name: Option<String>,
    pub size: Option<u64>, // except DeleteMarker
    pub storage_class: Option<String>,
    pub is_latest: bool,            // except ListObjects V1/V2
    pub version_id: Option<String>, // except ListObjects V1/V2
    pub user_metadata: Option<HashMap<String, String>>,
    pub user_tags: Option<HashMap<String, String>>,
    pub is_prefix: bool,
    pub is_delete_marker: bool,
    pub encoding_type: Option<String>,
}

#[derive(Clone, Debug)]
/// Contains the bucket name and creation date
pub struct Bucket {
    pub name: String,
    pub creation_date: UtcTime,
}

#[derive(Clone, Debug)]
/// Contains part number and etag of multipart upload
pub struct Part {
    pub number: u16,
    pub etag: String,
}

#[derive(Clone, Debug)]
pub struct PartInfo {
    pub number: u16,
    pub etag: String,

    pub size: u64,
}

#[derive(PartialEq, Clone, Debug)]
/// Contains retention mode information
pub enum RetentionMode {
    GOVERNANCE,
    COMPLIANCE,
}

impl RetentionMode {
    pub fn parse(s: &str) -> Result<RetentionMode, ValidationErr> {
        if s.eq_ignore_ascii_case("GOVERNANCE") {
            Ok(RetentionMode::GOVERNANCE)
        } else if s.eq_ignore_ascii_case("COMPLIANCE") {
            Ok(RetentionMode::COMPLIANCE)
        } else {
            Err(ValidationErr::InvalidRetentionMode(s.to_string()))
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
/// Contains retention mode and retain until date
pub struct Retention {
    pub mode: RetentionMode,
    pub retain_until_date: UtcTime,
}

/// Parses 'legal hold' string value
pub fn parse_legal_hold(s: &str) -> Result<bool, ValidationErr> {
    if s.eq_ignore_ascii_case("ON") {
        Ok(true)
    } else if s.eq_ignore_ascii_case("OFF") {
        Ok(false)
    } else {
        Err(ValidationErr::InvalidLegalHold(s.to_string()))
    }
}

#[derive(Clone, Debug)]
/// Compression types
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
/// File header information types
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
/// JSON document types
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
/// Quote fields types
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

#[derive(Clone, Debug)]
/// CSV input serialization definitions
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

#[derive(Clone, Debug)]
/// JSON input serialization definitions
pub struct JsonInputSerialization {
    pub compression_type: Option<CompressionType>,
    pub json_type: Option<JsonType>,
}

#[derive(Clone, Debug)]
/// Parquet input serialization definitions
pub struct ParquetInputSerialization;

#[derive(Clone, Debug)]
/// CSV output serialization definitions
pub struct CsvOutputSerialization {
    pub field_delimiter: Option<char>,
    pub quote_character: Option<char>,
    pub quote_escape_character: Option<char>,
    pub quote_fields: Option<QuoteFields>,
    pub record_delimiter: Option<char>,
}

#[derive(Clone, Debug)]
/// JSON output serialization definitions
pub struct JsonOutputSerialization {
    pub record_delimiter: Option<char>,
}

#[derive(Clone, Debug)]
/// Select request for [select_object_content()](crate::s3::client::MinioClient::select_object_content) API
#[derive(Default)]
pub struct SelectRequest {
    pub expr: String,
    pub csv_input: Option<CsvInputSerialization>,
    pub json_input: Option<JsonInputSerialization>,
    pub parquet_input: Option<ParquetInputSerialization>,
    pub csv_output: Option<CsvOutputSerialization>,
    pub json_output: Option<JsonOutputSerialization>,
    pub request_progress: bool,
    pub scan_start_range: Option<usize>,
    pub scan_end_range: Option<usize>,
}

impl SelectRequest {
    pub fn new_csv_input_output(
        expr: &str,
        csv_input: CsvInputSerialization,
        csv_output: CsvOutputSerialization,
    ) -> Result<SelectRequest, ValidationErr> {
        if expr.is_empty() {
            return Err(ValidationErr::InvalidSelectExpression(
                "select expression cannot be empty".into(),
            ));
        }

        Ok(SelectRequest {
            expr: expr.to_string(),
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
        expr: String,
        csv_input: CsvInputSerialization,
        json_output: JsonOutputSerialization,
    ) -> Result<SelectRequest, ValidationErr> {
        if expr.is_empty() {
            return Err(ValidationErr::InvalidSelectExpression(
                "select expression cannot be empty".into(),
            ));
        }

        Ok(SelectRequest {
            expr,
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
        expr: String,
        json_input: JsonInputSerialization,
        json_output: JsonOutputSerialization,
    ) -> Result<SelectRequest, ValidationErr> {
        if expr.is_empty() {
            return Err(ValidationErr::InvalidSelectExpression(
                "select expression cannot be empty".into(),
            ));
        }

        Ok(SelectRequest {
            expr,
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
        expr: String,
        parquet_input: ParquetInputSerialization,
        csv_output: CsvOutputSerialization,
    ) -> Result<SelectRequest, ValidationErr> {
        if expr.is_empty() {
            return Err(ValidationErr::InvalidSelectExpression(
                "select expression cannot be empty".into(),
            ));
        }

        Ok(SelectRequest {
            expr,
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
        expr: String,
        parquet_input: ParquetInputSerialization,
        json_output: JsonOutputSerialization,
    ) -> Result<SelectRequest, ValidationErr> {
        if expr.is_empty() {
            return Err(ValidationErr::InvalidSelectExpression(
                "select expression cannot be empty".into(),
            ));
        }

        Ok(SelectRequest {
            expr,
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
        data.push_str(&self.expr);
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
                data.push(v);
                data.push_str("</Comments>");
            }
            if let Some(v) = c.field_delimiter {
                data.push_str("<FieldDelimiter>");
                data.push(v);
                data.push_str("</FieldDelimiter>");
            }
            if let Some(v) = &c.file_header_info {
                data.push_str("<FileHeaderInfo>");
                data.push_str(&v.to_string());
                data.push_str("</FileHeaderInfo>");
            }
            if let Some(v) = c.quote_character {
                data.push_str("<QuoteCharacter>");
                data.push(v);
                data.push_str("</QuoteCharacter>");
            }
            if let Some(v) = c.record_delimiter {
                data.push_str("<RecordDelimiter>");
                data.push(v);
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
        } else if self.parquet_input.is_some() {
            data.push_str("<Parquet></Parquet>");
        }
        data.push_str("</InputSerialization>");

        data.push_str("<OutputSerialization>");
        if let Some(c) = &self.csv_output {
            data.push_str("<CSV>");
            if let Some(v) = c.field_delimiter {
                data.push_str("<FieldDelimiter>");
                data.push(v);
                data.push_str("</FieldDelimiter>");
            }
            if let Some(v) = c.quote_character {
                data.push_str("<QuoteCharacter>");
                data.push(v);
                data.push_str("</QuoteCharacter>");
            }
            if let Some(v) = c.quote_escape_character {
                data.push_str("<QuoteEscapeCharacter>");
                data.push(v);
                data.push_str("</QuoteEscapeCharacter>");
            }
            if let Some(v) = &c.quote_fields {
                data.push_str("<QuoteFields>");
                data.push_str(&v.to_string());
                data.push_str("</QuoteFields>");
            }
            if let Some(v) = c.record_delimiter {
                data.push_str("<RecordDelimiter>");
                data.push(v);
                data.push_str("</RecordDelimiter>");
            }
            data.push_str("</CSV>");
        } else if let Some(j) = &self.json_output {
            data.push_str("<JSON>");
            if let Some(v) = j.record_delimiter {
                data.push_str("<RecordDelimiter>");
                data.push(v);
                data.push_str("</RecordDelimiter>");
            }
            data.push_str("</JSON>");
        }
        data.push_str("</OutputSerialization>");

        if self.request_progress {
            data.push_str("<RequestProgress><Enabled>true</Enabled></RequestProgress>");
        }

        if let Some(s) = self.scan_start_range
            && let Some(e) = self.scan_end_range
        {
            data.push_str("<ScanRange>");
            data.push_str("<Start>");
            data.push_str(&s.to_string());
            data.push_str("</Start>");
            data.push_str("<End>");
            data.push_str(&e.to_string());
            data.push_str("</End>");
            data.push_str("</ScanRange>");
        }

        data.push_str("</SelectObjectContentRequest>");
        data
    }
}

/// Progress information of [select_object_content()](crate::s3::client::MinioClient::select_object_content) API
#[derive(Clone, Debug)]
pub struct SelectProgress {
    pub bytes_scanned: usize,
    pub bytes_progressed: usize,
    pub bytes_returned: usize,
}

/// User identity contains principal ID
#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct UserIdentity {
    #[serde(alias = "principalId", default)]
    pub principal_id: String,
}

/// Owner identity contains principal ID
pub type OwnerIdentity = UserIdentity;

/// Request parameters contain principal ID, region, and source IP address, but
/// they are represented as a string-to-string map in the MinIO server. So we
/// provide methods to fetch the known fields and a map for underlying
/// representation.
#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct RequestParameters(HashMap<String, String>);

impl RequestParameters {
    pub fn principal_id(&self) -> Option<&String> {
        self.0.get("principalId")
    }

    /// Gets the region for the request
    pub fn region(&self) -> Option<&String> {
        self.0.get("region")
    }

    pub fn source_ip_address(&self) -> Option<&String> {
        self.0.get("sourceIPAddress")
    }

    pub fn get_map(&self) -> &HashMap<String, String> {
        &self.0
    }
}

#[derive(Debug, Deserialize, Serialize, Clone, Default)]
/// Response elements information: they are represented as a string-to-string
/// map in the MinIO server. So we provide methods to fetch the known fields and
/// a map for underlying representation.
pub struct ResponseElements(HashMap<String, String>);

impl ResponseElements {
    pub fn content_length(&self) -> Option<&String> {
        self.0.get(CONTENT_LENGTH)
    }

    pub fn x_amz_request_id(&self) -> Option<&String> {
        self.0.get(X_AMZ_REQUEST_ID)
    }

    pub fn x_minio_deployment_id(&self) -> Option<&String> {
        self.0.get(X_MINIO_DEPLOYMENT_ID)
    }

    pub fn x_amz_id_2(&self) -> Option<&String> {
        self.0.get(X_AMZ_ID_2)
    }

    pub fn x_minio_origin_endpoint(&self) -> Option<&String> {
        self.0.get("x-minio-origin-endpoint")
    }

    pub fn get_map(&self) -> &HashMap<String, String> {
        &self.0
    }
}

#[derive(Debug, Deserialize, Serialize, Clone)]
/// S3 bucket information
pub struct S3Bucket {
    #[serde(alias = "name")]
    pub name: String,
    #[serde(alias = "arn")]
    pub arn: String,
    #[serde(alias = "ownerIdentity")]
    pub owner_identity: OwnerIdentity,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
/// S3 object information
pub struct S3Object {
    #[serde(alias = "key")]
    pub key: String,
    #[serde(alias = "size")]
    pub size: Option<u64>,
    #[serde(alias = "eTag")]
    pub etag: Option<String>,
    #[serde(alias = "contentType")]
    pub content_type: Option<String>,
    #[serde(alias = "userMetadata")]
    pub user_metadata: Option<HashMap<String, String>>,
    #[serde(alias = "versionId", default)]
    pub version_id: String,
    #[serde(alias = "sequencer", default)]
    pub sequencer: String,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
/// S3 definitions for NotificationRecord
pub struct S3 {
    #[serde(alias = "s3SchemaVersion")]
    pub s3_schema_version: String,
    #[serde(alias = "configurationId")]
    pub configuration_id: String,
    #[serde(alias = "bucket")]
    pub bucket: S3Bucket,
    #[serde(alias = "object")]
    pub object: S3Object,
}

#[derive(Debug, Deserialize, Serialize, Clone, Default)]
/// Source information
pub struct Source {
    #[serde(alias = "host", default)]
    pub host: String,
    #[serde(alias = "port")]
    pub port: Option<String>,
    #[serde(alias = "userAgent", default)]
    pub user_agent: String,
}

/// Notification record information
#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct NotificationRecord {
    #[serde(alias = "eventVersion")]
    pub event_version: String,
    #[serde(alias = "eventSource")]
    pub event_source: String,
    #[serde(alias = "awsRegion")]
    pub aws_region: String,
    #[serde(
        alias = "eventTime",
        default,
        with = "crate::s3::utils::aws_date_format"
    )]
    pub event_time: UtcTime,
    #[serde(alias = "eventName")]
    pub event_name: String,
    #[serde(alias = "userIdentity")]
    pub user_identity: UserIdentity,
    #[serde(alias = "requestParameters")]
    pub request_parameters: Option<RequestParameters>,
    #[serde(alias = "responseElements")]
    pub response_elements: ResponseElements,
    #[serde(alias = "s3")]
    pub s3: S3,
    #[serde(alias = "source")]
    pub source: Source,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
/// Contains notification records
pub struct NotificationRecords {
    #[serde(alias = "Records")]
    pub records: Vec<NotificationRecord>,
}

#[derive(Clone, Debug)]
/// Directive types
pub enum Directive {
    Copy,
    Replace,
}

impl Directive {
    pub fn parse(s: &str) -> Result<Directive, ValidationErr> {
        if s.eq_ignore_ascii_case("COPY") {
            Ok(Directive::Copy)
        } else if s.eq_ignore_ascii_case("REPLACE") {
            Ok(Directive::Replace)
        } else {
            Err(ValidationErr::InvalidDirective(s.into()))
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

#[derive(Clone, Debug, Default, Eq, PartialEq)]
/// Server-side information configuration
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
            kms_master_key_id,
        }
    }

    pub fn to_xml(&self) -> String {
        let mut data = String::from(
            "<ServerSideEncryptionConfiguration><Rule><ApplyServerSideEncryptionByDefault>",
        );
        data.push_str("<SSEAlgorithm>");
        data.push_str(&self.sse_algorithm);
        data.push_str("</SSEAlgorithm>");
        if let Some(v) = &self.kms_master_key_id {
            data.push_str("<KMSMasterKeyID>");
            data.push_str(v);
            data.push_str("</KMSMasterKeyID>");
        }

        data.push_str(
            "</ApplyServerSideEncryptionByDefault></Rule></ServerSideEncryptionConfiguration>",
        );
        data
    }
}

#[derive(PartialEq, Clone, Debug)]
/// Contains key and value
pub struct Tag {
    pub key: String,
    pub value: String,
}

#[derive(PartialEq, Clone, Debug)]
/// The 'And' operator contains prefix and tags
pub struct AndOperator {
    pub prefix: Option<String>,
    pub tags: Option<HashMap<String, String>>,
}

#[derive(Clone, Debug, PartialEq, Default)]
/// Filter information
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

#[allow(clippy::type_complexity)]
fn parse_common_notification_config(
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

fn to_xml_common_notification_config(
    events: &Vec<String>,
    id: &Option<String>,
    prefix_filter_rule: &Option<PrefixFilterRule>,
    suffix_filter_rule: &Option<SuffixFilterRule>,
) -> String {
    let mut data = String::new();

    for event in events {
        data.push_str("<Event>");
        data.push_str(event);
        data.push_str("</Event>");
    }

    if let Some(v) = id {
        data.push_str("<Id>");
        data.push_str(v);
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

    data
}

#[derive(PartialEq, Clone, Debug)]
/// Prefix filter rule
pub struct PrefixFilterRule {
    pub value: String,
}

impl PrefixFilterRule {
    pub const NAME: &'static str = "prefix";
}

#[derive(PartialEq, Clone, Debug)]
/// Suffix filter rule
pub struct SuffixFilterRule {
    pub value: String,
}

impl SuffixFilterRule {
    pub const NAME: &'static str = "suffix";
}

#[derive(PartialEq, Clone, Debug)]
/// Cloud function configuration information
pub struct CloudFuncConfig {
    pub events: Vec<String>,
    pub id: Option<String>,
    pub prefix_filter_rule: Option<PrefixFilterRule>,
    pub suffix_filter_rule: Option<SuffixFilterRule>,
    pub cloud_func: String,
}

impl CloudFuncConfig {
    pub fn from_xml(element: &mut Element) -> Result<CloudFuncConfig, ValidationErr> {
        let (events, id, prefix_filter_rule, suffix_filter_rule) =
            parse_common_notification_config(element)?;
        Ok(CloudFuncConfig {
            events,
            id,
            prefix_filter_rule,
            suffix_filter_rule,
            cloud_func: get_text_result(element, "CloudFunction")?,
        })
    }

    pub fn validate(&self) -> Result<(), ValidationErr> {
        if !self.events.is_empty() && !self.cloud_func.is_empty() {
            return Ok(());
        }

        Err(ValidationErr::InvalidFilter(self.to_xml()))
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

        data
    }
}

#[derive(PartialEq, Clone, Debug)]
/// Queue configuration information
pub struct QueueConfig {
    pub events: Vec<String>,
    pub id: Option<String>,
    pub prefix_filter_rule: Option<PrefixFilterRule>,
    pub suffix_filter_rule: Option<SuffixFilterRule>,
    pub queue: String,
}

impl QueueConfig {
    pub fn from_xml(element: &mut Element) -> Result<QueueConfig, ValidationErr> {
        let (events, id, prefix_filter_rule, suffix_filter_rule) =
            parse_common_notification_config(element)?;
        Ok(QueueConfig {
            events,
            id,
            prefix_filter_rule,
            suffix_filter_rule,
            queue: get_text_result(element, "Queue")?,
        })
    }

    pub fn validate(&self) -> Result<(), ValidationErr> {
        if !self.events.is_empty() && !self.queue.is_empty() {
            return Ok(());
        }

        Err(ValidationErr::InvalidFilter(self.to_xml()))
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

        data
    }
}

#[derive(PartialEq, Clone, Debug)]
/// Topic configuration information
pub struct TopicConfig {
    pub events: Vec<String>,
    pub id: Option<String>,
    pub prefix_filter_rule: Option<PrefixFilterRule>,
    pub suffix_filter_rule: Option<SuffixFilterRule>,
    pub topic: String,
}

impl TopicConfig {
    pub fn from_xml(element: &mut Element) -> Result<TopicConfig, ValidationErr> {
        let (events, id, prefix_filter_rule, suffix_filter_rule) =
            parse_common_notification_config(element)?;
        Ok(TopicConfig {
            events,
            id,
            prefix_filter_rule,
            suffix_filter_rule,
            topic: get_text_result(element, "Topic")?,
        })
    }

    pub fn validate(&self) -> Result<(), ValidationErr> {
        if !self.events.is_empty() && !self.topic.is_empty() {
            return Ok(());
        }

        Err(ValidationErr::InvalidFilter(self.to_xml()))
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

        data
    }
}

#[derive(PartialEq, Clone, Debug, Default)]
/// Notification configuration information
pub struct NotificationConfig {
    pub cloud_func_config_list: Option<Vec<CloudFuncConfig>>,
    pub queue_config_list: Option<Vec<QueueConfig>>,
    pub topic_config_list: Option<Vec<TopicConfig>>,
}

impl NotificationConfig {
    pub fn from_xml(root: &mut Element) -> Result<NotificationConfig, ValidationErr> {
        let mut config = NotificationConfig {
            cloud_func_config_list: None,
            queue_config_list: None,
            topic_config_list: None,
        };

        let mut cloud_func_config_list = Vec::new();
        while let Some(mut v) = root.take_child("CloudFunctionConfiguration") {
            cloud_func_config_list.push(CloudFuncConfig::from_xml(&mut v)?);
        }
        if !cloud_func_config_list.is_empty() {
            config.cloud_func_config_list = Some(cloud_func_config_list);
        }

        let mut queue_config_list = Vec::new();
        while let Some(mut v) = root.take_child("QueueConfiguration") {
            queue_config_list.push(QueueConfig::from_xml(&mut v)?);
        }
        if !queue_config_list.is_empty() {
            config.queue_config_list = Some(queue_config_list);
        }

        let mut topic_config_list = Vec::new();
        while let Some(mut v) = root.take_child("TopicConfiguration") {
            topic_config_list.push(TopicConfig::from_xml(&mut v)?);
        }
        if !topic_config_list.is_empty() {
            config.topic_config_list = Some(topic_config_list);
        }

        Ok(config)
    }

    pub fn validate(&self) -> Result<(), ValidationErr> {
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
        data
    }
}

#[derive(PartialEq, Clone, Debug)]
/// Access control translation information
pub struct AccessControlTranslation {
    pub owner: String,
}

impl AccessControlTranslation {
    pub fn new() -> Self {
        Self {
            owner: "Destination".to_string(),
        }
    }
}

impl Default for AccessControlTranslation {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(PartialEq, Clone, Debug)]
/// Encryption configuration information
pub struct EncryptionConfig {
    pub replica_kms_key_id: Option<String>,
}

#[derive(PartialEq, Clone, Debug)]
/// Metrics information
pub struct Metrics {
    pub event_threshold_minutes: Option<i32>,
    pub status: bool,
}

impl Metrics {
    pub fn new(status: bool) -> Self {
        Self {
            event_threshold_minutes: Some(15),
            status,
        }
    }
}

#[derive(PartialEq, Clone, Debug)]
/// Replication time information
pub struct ReplicationTime {
    pub time_minutes: Option<i32>,
    pub status: bool,
}

impl ReplicationTime {
    pub fn new(status: bool) -> Self {
        Self {
            time_minutes: Some(15),
            status,
        }
    }
}

#[derive(PartialEq, Clone, Debug, Default)]
/// Destination information
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
    pub fn from_xml(element: &Element) -> Result<Destination, ValidationErr> {
        Ok(Destination {
            bucket_arn: get_text_result(element, "Bucket")?,
            access_control_translation: match element.get_child("AccessControlTranslation") {
                Some(v) => Some(AccessControlTranslation {
                    owner: get_text_result(v, "Owner")?,
                }),
                _ => None,
            },
            account: get_text_option(element, "Account"),
            encryption_config: element.get_child("EncryptionConfiguration").map(|v| {
                EncryptionConfig {
                    replica_kms_key_id: get_text_option(v, "ReplicaKmsKeyID"),
                }
            }),
            metrics: match element.get_child("Metrics") {
                Some(v) => Some(Metrics {
                    event_threshold_minutes: match get_text_option(
                        v.get_child("EventThreshold")
                            .ok_or(ValidationErr::xml_error("<EventThreshold> tag not found"))?,
                        "Minutes",
                    ) {
                        Some(v) => Some(v.parse::<i32>()?),
                        _ => None,
                    },
                    status: get_text_result(v, "Status")? == "Enabled",
                }),
                _ => None,
            },
            replication_time: match element.get_child("ReplicationTime") {
                Some(v) => Some(ReplicationTime {
                    time_minutes: match get_text_option(v, "Time") {
                        Some(v) => Some(v.parse::<i32>()?),
                        _ => None,
                    },
                    status: get_text_result(v, "Status")? == "Enabled",
                }),
                _ => None,
            },
            storage_class: get_text_option(element, "StorageClass"),
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
            data.push_str(v);
            data.push_str("</Account>");
        }

        if let Some(c) = &self.encryption_config {
            data.push_str("<EncryptionConfiguration>");
            if let Some(v) = &c.replica_kms_key_id {
                data.push_str("<ReplicaKmsKeyID>");
                data.push_str(v);
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
            data.push_str(v);
            data.push_str("</StorageClass>");
        }

        data.push_str("</Destination>");

        data
    }
}

#[derive(PartialEq, Clone, Debug)]
/// Source selection criteria information
pub struct SourceSelectionCriteria {
    pub sse_kms_encrypted_objects_status: Option<bool>,
}

#[derive(PartialEq, Clone, Debug, Default)]
/// Replication rule information
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
    pub fn from_xml(element: &Element) -> Result<ReplicationRule, ValidationErr> {
        Ok(ReplicationRule {
            destination: Destination::from_xml(
                element
                    .get_child("Destination")
                    .ok_or(ValidationErr::xml_error("<Destination> tag not found"))?,
            )?,
            delete_marker_replication_status: match element.get_child("DeleteMarkerReplication") {
                Some(v) => Some(get_text_result(v, "Status")? == "Enabled"),
                _ => None,
            },
            existing_object_replication_status: match element.get_child("ExistingObjectReplication")
            {
                Some(v) => Some(get_text_result(v, "Status")? == "Enabled"),
                _ => None,
            },
            filter: match element.get_child("Filter") {
                Some(v) => Some(Filter::from_xml(v)?),
                _ => None,
            },
            id: get_text_option(element, "ID"),
            prefix: get_text_option(element, "Prefix"),
            priority: match get_text_option(element, "Priority") {
                Some(v) => Some(v.parse::<i32>()?),
                _ => None,
            },
            source_selection_criteria: match element.get_child("SourceSelectionCriteria") {
                Some(v) => match v.get_child("SseKmsEncryptedObjects") {
                    Some(v) => Some(SourceSelectionCriteria {
                        sse_kms_encrypted_objects_status: Some(
                            get_text_result(v, "Status")? == "Enabled",
                        ),
                    }),
                    _ => Some(SourceSelectionCriteria {
                        sse_kms_encrypted_objects_status: None,
                    }),
                },
                _ => None,
            },
            delete_replication_status: match element.get_child("DeleteReplication") {
                Some(v) => Some(get_text_result(v, "Status")? == "Enabled"),
                _ => None,
            },
            status: get_text_result(element, "Status")? == "Enabled",
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
            data.push_str(v);
            data.push_str("</ID>");
        }

        if let Some(v) = &self.prefix {
            data.push_str("<Prefix>");
            data.push_str(v);
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

        data
    }
}

#[derive(PartialEq, Clone, Debug, Default)]
/// Replication configuration information
pub struct ReplicationConfig {
    pub role: Option<String>,
    pub rules: Vec<ReplicationRule>,
}

impl ReplicationConfig {
    pub fn from_xml(root: &Element) -> Result<ReplicationConfig, ValidationErr> {
        let mut config = ReplicationConfig {
            role: get_text_option(root, "Role"),
            rules: Vec::new(),
        };

        if let Some(v) = root.get_child("Rule") {
            for rule in &v.children {
                config.rules.push(ReplicationRule::from_xml(
                    rule.as_element()
                        .ok_or(ValidationErr::xml_error("<Rule> tag not found"))?,
                )?);
            }
        }

        Ok(config)
    }

    pub fn to_xml(&self) -> String {
        let mut data = String::from("<ReplicationConfiguration>");

        if let Some(v) = &self.role {
            data.push_str("<Status>");
            data.push_str(v);
            data.push_str("</Status>");
        }

        for rule in &self.rules {
            data.push_str(&rule.to_xml());
        }

        data.push_str("</ReplicationConfiguration>");
        data
    }
}

#[derive(Clone, Debug, Default)]
/// Object lock configuration information
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
    ) -> Result<Self, ValidationErr> {
        if days.is_some() ^ years.is_some() {
            return Ok(Self {
                retention_mode: Some(mode),
                retention_duration_days: days,
                retention_duration_years: years,
            });
        }

        Err(ValidationErr::InvalidObjectLockConfig(
            "only one field 'days' or 'years' must be set".into(),
        ))
    }

    pub fn from_xml(root: &Element) -> Result<ObjectLockConfig, ValidationErr> {
        let mut config = ObjectLockConfig {
            retention_mode: None,
            retention_duration_days: None,
            retention_duration_years: None,
        };

        if let Some(r) = root.get_child("Rule") {
            let default_retention = r
                .get_child("DefaultRetention")
                .ok_or(ValidationErr::xml_error("<DefaultRetention> tag not found"))?;
            config.retention_mode = Some(RetentionMode::parse(&get_text_result(
                default_retention,
                "Mode",
            )?)?);

            if let Some(v) = get_text_option(default_retention, "Days") {
                config.retention_duration_days = Some(v.parse::<i32>()?);
            }

            if let Some(v) = get_text_option(default_retention, "Years") {
                config.retention_duration_years = Some(v.parse::<i32>()?);
            }
        }

        Ok(config)
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

        data
    }
}
