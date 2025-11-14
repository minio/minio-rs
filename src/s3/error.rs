use crate::s3::minio_error_response::MinioErrorResponse;
use thiserror::Error;

// Client side validation issues like invalid url or bucket name
#[derive(Error, Debug)]
pub enum ValidationErr {
    /// The specified bucket is not valid
    #[error("Invalid bucket name: '{name}' - {reason}")]
    InvalidBucketName { name: String, reason: String },

    /// No Bucket name was provided
    #[error("No bucket name provided")]
    MissingBucketName,

    /// Error while parsing time from string
    #[error("Time parse error: {0}")]
    TimeParseError(#[from] chrono::ParseError),

    /// Error while parsing a URL from string
    #[error("Invalid URL: {0}")]
    InvalidUrl(#[from] http::uri::InvalidUri),

    /// Error while performing IO operations
    #[error("IO error: {0}")]
    IOError(#[from] std::io::Error),

    #[error("XML parse error: {0}")]
    XmlParseError(#[from] xmltree::ParseError),

    #[error("HTTP error: {0}")]
    HttpError(#[from] reqwest::Error),

    #[error("String error: {message}")]
    StrError {
        message: String,
        #[source]
        source: Option<Box<dyn std::error::Error + Send + Sync>>,
    },

    #[error("Integer parsing error: {0}")]
    IntError(#[from] std::num::ParseIntError),

    #[error("Boolean parsing error: {0}")]
    BoolError(#[from] std::str::ParseBoolError),

    #[error("Failed to parse as UTF-8: {0}")]
    Utf8Error(#[from] std::str::Utf8Error),

    #[error("JSON error: {0}")]
    JsonError(#[from] serde_json::Error),

    #[error("XML error: {message}")]
    XmlError {
        message: String,
        #[source]
        source: Option<Box<dyn std::error::Error + Send + Sync>>,
    },

    #[error("Invalid object name: {0}")]
    InvalidObjectName(String),

    #[error("Invalid upload ID: {0}")]
    InvalidUploadId(String),

    #[error("Invalid part number: {0}")]
    InvalidPartNumber(String),

    #[error("Invalid user metadata: {0}")]
    InvalidUserMetadata(String),

    #[error("Invalid boolean value: {0}")]
    InvalidBooleanValue(String),

    #[error("Invalid integer value: {message}")]
    InvalidIntegerValue {
        message: String,
        #[source]
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[error("Empty parts: {0}")]
    EmptyParts(String),

    #[error("Invalid retention mode: {0}")]
    InvalidRetentionMode(String),

    #[error("Invalid retention configuration: {0}")]
    InvalidRetentionConfig(String),

    #[error("Part size {0} is not supported; minimum allowed 5MiB")]
    InvalidMinPartSize(u64),

    #[error("Part size {0} is not supported; maximum allowed 5GiB")]
    InvalidMaxPartSize(u64),

    #[error("Object size {0} is not supported; maximum allowed 5TiB")]
    InvalidObjectSize(u64),

    #[error("Valid part size must be provided when object size is unknown")]
    MissingPartSize,

    #[error(
        "Object size {object_size} and part size {part_size} make more than {part_count} parts for upload"
    )]
    InvalidPartCount {
        object_size: u64,
        part_size: u64,
        part_count: u16,
    },

    #[error("Too many parts for upload: {0} parts; maximum allowed is MAX_MULTIPART_COUNT parts")]
    TooManyParts(u64),

    #[error("{}", sse_tls_required_message(.0))]
    SseTlsRequired(Option<String>),

    #[error("Too much data in the stream - exceeds {0} bytes")]
    TooMuchData(u64),

    #[error("Not enough data in the stream; expected: {expected}, got: {got} bytes")]
    InsufficientData { expected: u64, got: u64 },

    #[error("Invalid legal hold: {0}")]
    InvalidLegalHold(String),

    #[error("Invalid select expression: {0}")]
    InvalidSelectExpression(String),

    #[error("Invalid header value type: {0}")]
    InvalidHeaderValueType(u8),

    #[error("Invalid base URL: {0}")]
    InvalidBaseUrl(String),

    #[error("URL build error: {0}")]
    UrlBuildError(String),

    #[error("Region must be {bucket_region}, but passed {region}")]
    RegionMismatch {
        bucket_region: String,
        region: String,
    },

    #[error("{crc_type} CRC mismatch; expected: {expected}, got: {got}")]
    CrcMismatch {
        crc_type: String,
        expected: u32,
        got: u32,
    },

    #[error("Unknown event type: {0}")]
    UnknownEventType(String),

    /// Error returned by the S3 Select API
    #[error("Error code: {error_code}, error message: {error_message}")]
    SelectError {
        error_code: String,
        error_message: String,
    },

    /// Error returned when the S3 API is not supported by AWS S3
    #[error("{0} API is not supported in Amazon AWS S3")]
    UnsupportedAwsApi(String),

    #[error("{}", format_s3_object_error(.bucket, .object, .version.as_deref(), "InvalidComposeSourceOffset", &format!("offset {offset} is beyond object size {object_size}")))]
    InvalidComposeSourceOffset {
        bucket: String,
        object: String,
        version: Option<String>,
        offset: u64,
        object_size: u64,
    },

    #[error("{}", format_s3_object_error(.bucket, .object, .version.as_deref(), "InvalidComposeSourceLength", &format!("length {length} is beyond object size {object_size}")))]
    InvalidComposeSourceLength {
        bucket: String,
        object: String,
        version: Option<String>,
        length: u64,
        object_size: u64,
    },

    #[error("{}", format_s3_object_error(.bucket, .object, .version.as_deref(), "InvalidComposeSourceSize", &format!("compose size {compose_size} is beyond object size {object_size}")))]
    InvalidComposeSourceSize {
        bucket: String,
        object: String,
        version: Option<String>,
        compose_size: u64,
        object_size: u64,
    },

    #[error("Invalid directive: {0}")]
    InvalidDirective(String),

    #[error("Invalid copy directive: {0}")]
    InvalidCopyDirective(String),

    #[error("{}", format_s3_object_error(.bucket, .object, .version.as_deref(), "InvalidComposeSourcePartSize", &format!("compose size {size} must be greater than {expected_size}")))]
    InvalidComposeSourcePartSize {
        bucket: String,
        object: String,
        version: Option<String>,
        size: u64,
        expected_size: u64,
    },

    #[error("{}", format_s3_object_error(.bucket, .object, .version.as_deref(), "InvalidComposeSourceMultipart", &format!("size {size} for multipart split upload of {size}, last part size is less than {expected_size}")))]
    InvalidComposeSourceMultipart {
        bucket: String,
        object: String,
        version: Option<String>,
        size: u64,
        expected_size: u64,
    },

    #[error("Compose sources create more than allowed multipart count {0}")]
    InvalidMultipartCount(u64),

    #[error("Only one of And, Prefix or Tag must be provided: {0}")]
    InvalidFilter(String),

    #[error("Invalid versioning status: {0}")]
    InvalidVersioningStatus(String),

    #[error("Post policy error: {0}")]
    PostPolicyError(String),

    #[error("Invalid object lock config: {0}")]
    InvalidObjectLockConfig(String),

    #[error("Tag decoding failed: {error_message} on input '{input}'")]
    TagDecodingError {
        input: String,
        error_message: String,
    },

    #[error("Content length is unknown")]
    ContentLengthUnknown,

    #[error("{name} interceptor failed: {source}")]
    Hook {
        source: Box<dyn std::error::Error + Send + Sync>,
        name: String,
    },

    #[error("Invalid UTF-8: {source} while {context}")]
    InvalidUtf8 {
        #[source]
        source: std::string::FromUtf8Error,
        context: String,
    },

    #[error("Invalid JSON: {source} while {context}")]
    InvalidJson {
        #[source]
        source: serde_json::Error,
        context: String,
    },

    #[error("Invalid YAML: {message}")]
    InvalidYaml { message: String },

    #[error("Invalid configuration: {message}")]
    InvalidConfig { message: String },

    #[error("Invalid warehouse name: {0}")]
    InvalidWarehouseName(String),

    #[error("Invalid namespace name: {0}")]
    InvalidNamespaceName(String),

    #[error("Invalid table name: {0}")]
    InvalidTableName(String),

    #[error("Invalid table name: {0}")]
    InvalidTableName(String),
}

impl From<reqwest::header::ToStrError> for ValidationErr {
    fn from(err: reqwest::header::ToStrError) -> Self {
        ValidationErr::StrError {
            message: "The provided value has an invalid encoding".into(),
            source: Some(Box::new(err)),
        }
    }
}

// Some convenience methods for creating ValidationErr instances
impl ValidationErr {
    pub fn xml_error(message: impl Into<String>) -> Self {
        Self::XmlError {
            message: message.into(),
            source: None,
        }
    }
    pub fn xml_error_with_source(
        message: impl Into<String>,
        source: impl Into<Box<dyn std::error::Error + Send + Sync>>,
    ) -> Self {
        Self::XmlError {
            message: message.into(),
            source: Some(source.into()),
        }
    }
}

// IO errors from accessing local files
#[derive(Error, Debug)]
pub enum IoError {
    /// Error while performing IO operations
    #[error("IO error: {0}")]
    IOError(#[from] std::io::Error),
}

// IO errors on the network like network time out
#[derive(Error, Debug)]
pub enum NetworkError {
    #[error("Server failed with HTTP status code {0}")]
    ServerError(u16),

    #[error("Request error: {0}")]
    ReqwestError(#[from] reqwest::Error),
}

// Server response errors like bucket does not exist, etc.
// This would include any server sent validation errors.
#[derive(Error, Debug)]
pub enum S3ServerError {
    /// S3 Errors as returned by the S3 server
    #[error("S3 error: {0}")]
    S3Error(#[from] Box<MinioErrorResponse>), // NOTE: Boxing to prevent: "warning: large size difference between variants"

    #[error(
        "Invalid server response received; {message}; HTTP status code: {http_status_code}; content-type: {content_type}"
    )]
    InvalidServerResponse {
        message: String,
        http_status_code: u16,
        content_type: String,
    },

    #[error("HTTP error: status={0}, body={1}")]
    HttpError(u16, String),
}

// Top-level Minio client error
#[derive(Error, Debug)]
pub enum Error {
    #[error("S3 server error occurred")]
    S3Server(#[from] S3ServerError),

    #[error("Drive IO error occurred")]
    DriveIo(#[from] IoError),

    #[error("Network error occurred")]
    Network(#[from] NetworkError),

    #[error("Validation error occurred")]
    Validation(#[from] ValidationErr),

    #[error("Tables error occurred")]
    TablesError(#[from] Box<dyn std::error::Error + Send + Sync>),
}

// region message helpers

// Helper functions for formatting error messages with Option<String>
fn sse_tls_required_message(prefix: &Option<String>) -> String {
    match prefix {
        Some(p) => format!("{p} SSE operation must be performed over a secure connection",),
        None => "SSE operation must be performed over a secure connection".to_string(),
    }
}

fn format_s3_object_error(
    bucket: &str,
    object: &str,
    version: Option<&str>,
    error_type: &str,
    details: &str,
) -> String {
    let version_str = match &version.map(String::from) {
        Some(v) => format!("?versionId={v}"),
        None => String::new(),
    };
    format!("source {bucket}/{object}{version_str}: {error_type} {details}")
}

// endregion message helpers
