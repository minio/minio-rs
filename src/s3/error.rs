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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_validation_err_invalid_bucket_name() {
        let err = ValidationErr::InvalidBucketName {
            name: "My Bucket".to_string(),
            reason: "contains spaces".to_string(),
        };
        assert_eq!(
            err.to_string(),
            "Invalid bucket name: 'My Bucket' - contains spaces"
        );
    }

    #[test]
    fn test_validation_err_missing_bucket_name() {
        let err = ValidationErr::MissingBucketName;
        assert_eq!(err.to_string(), "No bucket name provided");
    }

    #[test]
    fn test_validation_err_invalid_object_name() {
        let err = ValidationErr::InvalidObjectName("invalid\0name".to_string());
        assert_eq!(err.to_string(), "Invalid object name: invalid\0name");
    }

    #[test]
    fn test_validation_err_invalid_upload_id() {
        let err = ValidationErr::InvalidUploadId("bad_upload_id".to_string());
        assert_eq!(err.to_string(), "Invalid upload ID: bad_upload_id");
    }

    #[test]
    fn test_validation_err_invalid_part_number() {
        let err = ValidationErr::InvalidPartNumber("0".to_string());
        assert_eq!(err.to_string(), "Invalid part number: 0");
    }

    #[test]
    fn test_validation_err_invalid_user_metadata() {
        let err = ValidationErr::InvalidUserMetadata("x-amz-meta-\0".to_string());
        assert_eq!(err.to_string(), "Invalid user metadata: x-amz-meta-\0");
    }

    #[test]
    fn test_validation_err_invalid_boolean_value() {
        let err = ValidationErr::InvalidBooleanValue("maybe".to_string());
        assert_eq!(err.to_string(), "Invalid boolean value: maybe");
    }

    #[test]
    fn test_validation_err_invalid_min_part_size() {
        let err = ValidationErr::InvalidMinPartSize(1024);
        assert_eq!(
            err.to_string(),
            "Part size 1024 is not supported; minimum allowed 5MiB"
        );
    }

    #[test]
    fn test_validation_err_invalid_max_part_size() {
        let err = ValidationErr::InvalidMaxPartSize(6_000_000_000);
        assert_eq!(
            err.to_string(),
            "Part size 6000000000 is not supported; maximum allowed 5GiB"
        );
    }

    #[test]
    fn test_validation_err_invalid_object_size() {
        let err = ValidationErr::InvalidObjectSize(10_000_000_000_000_000);
        assert_eq!(
            err.to_string(),
            "Object size 10000000000000000 is not supported; maximum allowed 5TiB"
        );
    }

    #[test]
    fn test_validation_err_missing_part_size() {
        let err = ValidationErr::MissingPartSize;
        assert_eq!(
            err.to_string(),
            "Valid part size must be provided when object size is unknown"
        );
    }

    #[test]
    fn test_validation_err_invalid_part_count() {
        let err = ValidationErr::InvalidPartCount {
            object_size: 100_000_000,
            part_size: 1_000_000,
            part_count: 10000,
        };
        let msg = err.to_string();
        assert!(msg.contains("100000000"));
        assert!(msg.contains("1000000"));
        assert!(msg.contains("10000"));
    }

    #[test]
    fn test_validation_err_too_many_parts() {
        let err = ValidationErr::TooManyParts(20000);
        assert!(err.to_string().contains("20000"));
        assert!(err.to_string().contains("maximum allowed"));
    }

    #[test]
    fn test_validation_err_sse_tls_required_no_prefix() {
        let err = ValidationErr::SseTlsRequired(None);
        assert_eq!(
            err.to_string(),
            "SSE operation must be performed over a secure connection"
        );
    }

    #[test]
    fn test_validation_err_sse_tls_required_with_prefix() {
        let err = ValidationErr::SseTlsRequired(Some("Server-side encryption".to_string()));
        let msg = err.to_string();
        assert!(msg.contains("Server-side encryption"));
        assert!(msg.contains("SSE operation"));
    }

    #[test]
    fn test_validation_err_too_much_data() {
        let err = ValidationErr::TooMuchData(5_000_000_000);
        assert_eq!(
            err.to_string(),
            "Too much data in the stream - exceeds 5000000000 bytes"
        );
    }

    #[test]
    fn test_validation_err_insufficient_data() {
        let err = ValidationErr::InsufficientData {
            expected: 1000,
            got: 500,
        };
        assert_eq!(
            err.to_string(),
            "Not enough data in the stream; expected: 1000, got: 500 bytes"
        );
    }

    #[test]
    fn test_validation_err_invalid_legal_hold() {
        let err = ValidationErr::InvalidLegalHold("MAYBE".to_string());
        assert_eq!(err.to_string(), "Invalid legal hold: MAYBE");
    }

    #[test]
    fn test_validation_err_invalid_select_expression() {
        let err = ValidationErr::InvalidSelectExpression("SELECT * FORM s3object".to_string());
        assert_eq!(
            err.to_string(),
            "Invalid select expression: SELECT * FORM s3object"
        );
    }

    #[test]
    fn test_validation_err_invalid_header_value_type() {
        let err = ValidationErr::InvalidHeaderValueType(42);
        assert_eq!(err.to_string(), "Invalid header value type: 42");
    }

    #[test]
    fn test_validation_err_invalid_base_url() {
        let err = ValidationErr::InvalidBaseUrl("not a url".to_string());
        assert_eq!(err.to_string(), "Invalid base URL: not a url");
    }

    #[test]
    fn test_validation_err_url_build_error() {
        let err = ValidationErr::UrlBuildError("missing scheme".to_string());
        assert_eq!(err.to_string(), "URL build error: missing scheme");
    }

    #[test]
    fn test_validation_err_region_mismatch() {
        let err = ValidationErr::RegionMismatch {
            bucket_region: "us-west-2".to_string(),
            region: "us-east-1".to_string(),
        };
        let msg = err.to_string();
        assert!(msg.contains("us-west-2"));
        assert!(msg.contains("us-east-1"));
    }

    #[test]
    fn test_validation_err_crc_mismatch() {
        let err = ValidationErr::CrcMismatch {
            crc_type: "CRC32".to_string(),
            expected: 0x12345678,
            got: 0x87654321,
        };
        let msg = err.to_string();
        assert!(msg.contains("CRC32"));
        assert!(msg.contains("expected"));
        assert!(msg.contains("got"));
    }

    #[test]
    fn test_validation_err_unknown_event_type() {
        let err = ValidationErr::UnknownEventType("s3:ObjectCreated:Complex".to_string());
        assert_eq!(
            err.to_string(),
            "Unknown event type: s3:ObjectCreated:Complex"
        );
    }

    #[test]
    fn test_validation_err_select_error() {
        let err = ValidationErr::SelectError {
            error_code: "InvalidSQL".to_string(),
            error_message: "Syntax error in SELECT".to_string(),
        };
        let msg = err.to_string();
        assert!(msg.contains("InvalidSQL"));
        assert!(msg.contains("Syntax error"));
    }

    #[test]
    fn test_validation_err_unsupported_aws_api() {
        let err = ValidationErr::UnsupportedAwsApi("AppendObject".to_string());
        assert!(err.to_string().contains("AppendObject"));
        assert!(err.to_string().contains("Amazon AWS S3"));
    }

    #[test]
    fn test_validation_err_invalid_directive() {
        let err = ValidationErr::InvalidDirective("COPY-ALL".to_string());
        assert_eq!(err.to_string(), "Invalid directive: COPY-ALL");
    }

    #[test]
    fn test_validation_err_invalid_copy_directive() {
        let err = ValidationErr::InvalidCopyDirective("REPLACE-METADATA".to_string());
        assert_eq!(err.to_string(), "Invalid copy directive: REPLACE-METADATA");
    }

    #[test]
    fn test_validation_err_invalid_filter() {
        let err = ValidationErr::InvalidFilter("And and Prefix both provided".to_string());
        assert_eq!(
            err.to_string(),
            "Only one of And, Prefix or Tag must be provided: And and Prefix both provided"
        );
    }

    #[test]
    fn test_validation_err_invalid_versioning_status() {
        let err = ValidationErr::InvalidVersioningStatus("PAUSED".to_string());
        assert_eq!(err.to_string(), "Invalid versioning status: PAUSED");
    }

    #[test]
    fn test_validation_err_post_policy_error() {
        let err = ValidationErr::PostPolicyError("Missing required field: bucket".to_string());
        assert_eq!(
            err.to_string(),
            "Post policy error: Missing required field: bucket"
        );
    }

    #[test]
    fn test_validation_err_invalid_object_lock_config() {
        let err = ValidationErr::InvalidObjectLockConfig("Retention without Mode".to_string());
        assert_eq!(
            err.to_string(),
            "Invalid object lock config: Retention without Mode"
        );
    }

    #[test]
    fn test_validation_err_tag_decoding_error() {
        let err = ValidationErr::TagDecodingError {
            input: "invalid%ZZtag".to_string(),
            error_message: "Invalid percent encoding".to_string(),
        };
        let msg = err.to_string();
        assert!(msg.contains("invalid%ZZtag"));
        assert!(msg.contains("Invalid percent encoding"));
    }

    #[test]
    fn test_validation_err_content_length_unknown() {
        let err = ValidationErr::ContentLengthUnknown;
        assert_eq!(err.to_string(), "Content length is unknown");
    }

    #[test]
    fn test_validation_err_invalid_utf8() {
        let invalid_bytes = vec![0xFF, 0xFE];
        let err = ValidationErr::InvalidUtf8 {
            source: String::from_utf8(invalid_bytes).unwrap_err(),
            context: "parsing header value".to_string(),
        };
        let msg = err.to_string();
        assert!(msg.contains("Invalid UTF-8"));
        assert!(msg.contains("parsing header value"));
    }

    #[test]
    fn test_validation_err_invalid_json() {
        let json_err = serde_json::from_str::<serde_json::Value>("{invalid").unwrap_err();
        let err = ValidationErr::InvalidJson {
            source: json_err,
            context: "deserializing response".to_string(),
        };
        let msg = err.to_string();
        assert!(msg.contains("Invalid JSON"));
        assert!(msg.contains("deserializing response"));
    }

    #[test]
    fn test_validation_err_invalid_yaml() {
        let err = ValidationErr::InvalidYaml {
            message: "Unexpected token at line 5".to_string(),
        };
        assert_eq!(err.to_string(), "Invalid YAML: Unexpected token at line 5");
    }

    #[test]
    fn test_validation_err_invalid_config() {
        let err = ValidationErr::InvalidConfig {
            message: "Missing required parameter 'endpoint'".to_string(),
        };
        assert_eq!(
            err.to_string(),
            "Invalid configuration: Missing required parameter 'endpoint'"
        );
    }

    #[test]
    fn test_validation_err_invalid_warehouse_name() {
        let err = ValidationErr::InvalidWarehouseName("warehouse-1!".to_string());
        assert_eq!(err.to_string(), "Invalid warehouse name: warehouse-1!");
    }

    #[test]
    fn test_validation_err_invalid_namespace_name() {
        let err = ValidationErr::InvalidNamespaceName("default ".to_string());
        assert_eq!(err.to_string(), "Invalid namespace name: default ");
    }

    #[test]
    fn test_validation_err_invalid_table_name() {
        let err = ValidationErr::InvalidTableName("my_table?".to_string());
        assert_eq!(err.to_string(), "Invalid table name: my_table?");
    }

    #[test]
    fn test_validation_err_empty_parts() {
        let err = ValidationErr::EmptyParts("No parts provided for compose".to_string());
        assert_eq!(
            err.to_string(),
            "Empty parts: No parts provided for compose"
        );
    }

    #[test]
    fn test_validation_err_invalid_retention_mode() {
        let err = ValidationErr::InvalidRetentionMode("PERMANENT".to_string());
        assert_eq!(err.to_string(), "Invalid retention mode: PERMANENT");
    }

    #[test]
    fn test_validation_err_invalid_retention_config() {
        let err = ValidationErr::InvalidRetentionConfig(
            "Retain until must be specified with retention mode".to_string(),
        );
        assert!(err.to_string().contains("Retain until"));
    }

    #[test]
    fn test_validation_err_compose_source_offset() {
        let err = ValidationErr::InvalidComposeSourceOffset {
            bucket: "mybucket".to_string(),
            object: "myobject".to_string(),
            version: Some("v123".to_string()),
            offset: 5000,
            object_size: 4000,
        };
        let msg = err.to_string();
        assert!(msg.contains("mybucket"));
        assert!(msg.contains("myobject"));
        assert!(msg.contains("5000"));
    }

    #[test]
    fn test_validation_err_compose_source_length() {
        let err = ValidationErr::InvalidComposeSourceLength {
            bucket: "mybucket".to_string(),
            object: "myobject".to_string(),
            version: None,
            length: 3000,
            object_size: 2000,
        };
        let msg = err.to_string();
        assert!(msg.contains("mybucket"));
        assert!(msg.contains("myobject"));
        assert!(!msg.contains("versionId"));
    }

    #[test]
    fn test_validation_err_compose_source_size() {
        let err = ValidationErr::InvalidComposeSourceSize {
            bucket: "b1".to_string(),
            object: "o1".to_string(),
            version: None,
            compose_size: 10_000,
            object_size: 5000,
        };
        assert!(err.to_string().contains("b1/o1"));
    }

    #[test]
    fn test_validation_err_compose_source_part_size() {
        let err = ValidationErr::InvalidComposeSourcePartSize {
            bucket: "b".to_string(),
            object: "o".to_string(),
            version: None,
            size: 1_000_000,
            expected_size: 5_242_880,
        };
        assert!(err.to_string().contains("b/o"));
    }

    #[test]
    fn test_validation_err_compose_source_multipart() {
        let err = ValidationErr::InvalidComposeSourceMultipart {
            bucket: "b".to_string(),
            object: "o".to_string(),
            version: None,
            size: 100_000_000,
            expected_size: 5_242_880,
        };
        assert!(err.to_string().contains("b/o"));
    }

    #[test]
    fn test_validation_err_invalid_multipart_count() {
        let err = ValidationErr::InvalidMultipartCount(11000);
        assert!(err.to_string().contains("11000"));
        assert!(err.to_string().contains("multipart count"));
    }

    #[test]
    fn test_io_error_creation() {
        let std_err = std::io::Error::new(std::io::ErrorKind::NotFound, "file not found");
        let io_err = IoError::IOError(std_err);
        assert!(io_err.to_string().contains("file not found"));
    }

    #[test]
    fn test_network_error_server_error() {
        let err = NetworkError::ServerError(500);
        assert_eq!(err.to_string(), "Server failed with HTTP status code 500");
    }

    #[test]
    fn test_validation_err_xml_error() {
        let err = ValidationErr::xml_error("Missing required element 'Bucket'");
        let msg = err.to_string();
        assert!(msg.contains("XML error"));
        assert!(msg.contains("Missing required element"));
    }

    #[test]
    fn test_validation_err_str_error() {
        let err = ValidationErr::StrError {
            message: "Connection refused".to_string(),
            source: None,
        };
        assert_eq!(err.to_string(), "String error: Connection refused");
    }

    #[test]
    fn test_error_hierarchy() {
        let validation_err = ValidationErr::MissingBucketName;
        let error: Error = validation_err.into();
        assert!(matches!(error, Error::Validation(_)));
    }

    #[test]
    fn test_format_s3_object_error_without_version() {
        let msg = format_s3_object_error("mybucket", "myobject", None, "TestError", "test details");
        assert_eq!(msg, "source mybucket/myobject: TestError test details");
    }

    #[test]
    fn test_format_s3_object_error_with_version() {
        let msg = format_s3_object_error(
            "mybucket",
            "myobject",
            Some("v123"),
            "TestError",
            "test details",
        );
        assert_eq!(
            msg,
            "source mybucket/myobject?versionId=v123: TestError test details"
        );
    }
}
