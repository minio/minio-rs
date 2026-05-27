// MinIO Rust Library for Amazon S3 Compatible Cloud Storage
// Copyright 2025 MinIO, Inc.
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

//! Error types for S3 Tables / Iceberg operations

use crate::s3::error::{NetworkError, ValidationErr};
use serde::Deserialize;
use std::error::Error as StdError;
use std::fmt;

/// S3 Tables parameter validation error
///
/// Provides context about which parameter failed validation and why.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct S3TablesValidationErr {
    /// The parameter that failed validation (e.g., "warehouse_name", "namespace")
    pub parameter: &'static str,
    /// The value that was provided (if available)
    pub value: Option<String>,
    /// The reason validation failed
    pub reason: String,
}

impl S3TablesValidationErr {
    /// Creates a new validation error
    pub fn new(parameter: &'static str, reason: impl Into<String>) -> Self {
        Self {
            parameter,
            value: None,
            reason: reason.into(),
        }
    }

    /// Creates a new validation error with the invalid value included
    pub fn with_value(
        parameter: &'static str,
        value: impl Into<String>,
        reason: impl Into<String>,
    ) -> Self {
        Self {
            parameter,
            value: Some(value.into()),
            reason: reason.into(),
        }
    }
}

impl fmt::Display for S3TablesValidationErr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.value {
            Some(value) => write!(
                f,
                "invalid {}: '{}' - {}",
                self.parameter, value, self.reason
            ),
            None => write!(f, "invalid {}: {}", self.parameter, self.reason),
        }
    }
}

impl StdError for S3TablesValidationErr {}

impl From<S3TablesValidationErr> for ValidationErr {
    fn from(err: S3TablesValidationErr) -> Self {
        ValidationErr::StrError {
            message: err.to_string(),
            source: None,
        }
    }
}

/// Tables-specific errors
///
/// Represents all error conditions that can occur during Tables operations.
#[derive(Debug)]
pub enum TablesError {
    // Warehouse errors
    /// Warehouse not found
    WarehouseNotFound {
        /// Name of the warehouse that was not found
        warehouse: String,
        /// HTTP status code from server response
        status_code: u16,
        /// Original error type from server (e.g., "IcebergWarehouseNotFound")
        error_type: String,
        /// Full original message from server
        original_message: String,
    },
    /// Warehouse already exists
    WarehouseAlreadyExists {
        /// Name of the warehouse that already exists
        warehouse: String,
        /// HTTP status code from server response
        status_code: u16,
        /// Original error type from server (e.g., "IcebergWarehouseAlreadyExists")
        error_type: String,
        /// Full original message from server
        original_message: String,
    },
    /// Invalid warehouse name
    WarehouseNameInvalid {
        /// The invalid warehouse name
        warehouse: String,
        /// Reason why the name is invalid
        cause: String,
    },

    // Namespace errors
    /// Namespace not found
    NamespaceNotFound {
        /// Name of the namespace that was not found
        namespace: String,
        /// HTTP status code from server response
        status_code: u16,
        /// Original error type from server (e.g., "NoSuchNamespaceException")
        error_type: String,
        /// Full original message from server
        original_message: String,
    },
    /// Namespace already exists
    NamespaceAlreadyExists {
        /// Name of the namespace that already exists
        namespace: String,
        /// HTTP status code from server response
        status_code: u16,
        /// Original error type from server (e.g., "AlreadyExistsException")
        error_type: String,
        /// Full original message from server
        original_message: String,
    },
    /// Invalid namespace name
    NamespaceNameInvalid {
        /// The invalid namespace name
        namespace: String,
        /// Reason why the name is invalid
        cause: String,
    },
    /// Namespace is not empty and cannot be deleted
    NamespaceNotEmpty {
        /// Name of the namespace that is not empty
        namespace: String,
        /// HTTP status code from server response
        status_code: u16,
        /// Original error type from server (e.g., "NamespaceNotEmptyException")
        error_type: String,
        /// Full original message from server
        original_message: String,
    },

    // Table errors
    /// Table not found
    TableNotFound {
        /// Name of the table that was not found
        table: String,
        /// HTTP status code from server response
        status_code: u16,
        /// Original error type from server (e.g., "NoSuchTableException")
        error_type: String,
        /// Full original message from server
        original_message: String,
    },
    /// Table already exists
    TableAlreadyExists {
        /// Name of the table that already exists
        table: String,
        /// HTTP status code from server response
        status_code: u16,
        /// Original error type from server (e.g., "AlreadyExistsException")
        error_type: String,
        /// Full original message from server
        original_message: String,
    },
    /// Invalid table name
    TableNameInvalid {
        /// The invalid table name
        table: String,
        /// Reason why the name is invalid
        cause: String,
    },

    // View errors
    /// View not found
    ViewNotFound {
        /// Name of the view that was not found
        view: String,
        /// HTTP status code from server response
        status_code: u16,
        /// Original error type from server (e.g., "NoSuchViewException")
        error_type: String,
        /// Full original message from server
        original_message: String,
    },
    /// View already exists
    ViewAlreadyExists {
        /// Name of the view that already exists
        view: String,
        /// HTTP status code from server response
        status_code: u16,
        /// Original error type from server (e.g., "AlreadyExistsException")
        error_type: String,
        /// Full original message from server
        original_message: String,
    },

    // Operation errors
    /// Bad request - invalid parameters or malformed request
    BadRequest {
        /// Description of what was invalid
        message: String,
        /// HTTP status code from server response
        status_code: u16,
        /// Original error type from server (e.g., "BadRequestException")
        error_type: String,
        /// Full original message from server
        original_message: String,
    },
    /// Commit operation failed (client-side use only; server errors use ServerError)
    CommitFailed {
        /// Description of why the commit failed
        message: String,
        /// HTTP status code from server response
        status_code: u16,
        /// Original error type from server (e.g., "CommitFailedException")
        error_type: String,
        /// Full original message from server
        original_message: String,
    },
    /// Commit conflict - requirements not met (client-side use only; server errors use ServerError)
    CommitConflict {
        /// Description of the conflict
        message: String,
    },
    /// Multi-table transaction failed (client-side use only; server errors use ServerError)
    TransactionFailed {
        /// Description of why the transaction failed
        message: String,
    },

    // Wrapped errors
    /// Network error during request
    Network(NetworkError),
    /// Validation error for request parameters
    Validation(S3TablesValidationErr),
    /// Generic error with custom message
    Generic(String),
    /// Orphaned metadata - table/namespace metadata references missing S3 files
    OrphanedMetadata {
        /// Description of what has orphaned metadata
        description: String,
    },
    /// Server API error with preserved HTTP status code
    ServerError {
        /// HTTP status code from the server response
        status_code: u16,
        /// Error type identifier (e.g., "BadRequestException")
        error_type: String,
        /// Error message from the server
        message: String,
    },
}

impl fmt::Display for TablesError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TablesError::WarehouseNotFound { warehouse, .. } => {
                write!(f, "Warehouse not found: {warehouse}")
            }
            TablesError::WarehouseAlreadyExists { warehouse, .. } => {
                write!(f, "Warehouse already exists: {warehouse}")
            }
            TablesError::WarehouseNameInvalid { warehouse, cause } => {
                write!(f, "Invalid warehouse name '{warehouse}': {cause}")
            }
            TablesError::NamespaceNotFound { namespace, .. } => {
                write!(f, "Namespace not found: {namespace}")
            }
            TablesError::NamespaceAlreadyExists { namespace, .. } => {
                write!(f, "Namespace already exists: {namespace}")
            }
            TablesError::NamespaceNameInvalid { namespace, cause } => {
                write!(f, "Invalid namespace name '{namespace}': {cause}")
            }
            TablesError::NamespaceNotEmpty { namespace, .. } => {
                write!(f, "Namespace is not empty: {namespace}")
            }
            TablesError::TableNotFound { table, .. } => {
                write!(f, "Table not found: {table}")
            }
            TablesError::TableAlreadyExists { table, .. } => {
                write!(f, "Table already exists: {table}")
            }
            TablesError::TableNameInvalid { table, cause } => {
                write!(f, "Invalid table name '{table}': {cause}")
            }
            TablesError::ViewNotFound { view, .. } => {
                write!(f, "View not found: {view}")
            }
            TablesError::ViewAlreadyExists { view, .. } => {
                write!(f, "View already exists: {view}")
            }
            TablesError::BadRequest { message, .. } => {
                write!(f, "Bad request: {message}")
            }
            TablesError::CommitFailed { message, .. } => {
                write!(f, "Commit failed: {message}")
            }
            TablesError::CommitConflict { message } => {
                write!(f, "Commit conflict: {message}")
            }
            TablesError::TransactionFailed { message } => {
                write!(f, "Transaction failed: {message}")
            }
            TablesError::Network(err) => write!(f, "Network error: {err}"),
            TablesError::Validation(err) => write!(f, "Validation error: {err}"),
            TablesError::Generic(msg) => write!(f, "{msg}"),
            TablesError::OrphanedMetadata { description } => {
                write!(f, "Orphaned metadata: {description}")
            }
            TablesError::ServerError {
                status_code,
                error_type,
                message,
            } => {
                write!(f, "Server error {status_code} ({error_type}): {message}")
            }
        }
    }
}

impl std::error::Error for TablesError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            TablesError::Network(err) => Some(err),
            TablesError::Validation(err) => Some(err),
            _ => None,
        }
    }
}

impl TablesError {
    /// Returns the HTTP status code associated with this error.
    ///
    /// For server-sourced errors (with preserved status codes), returns the actual status code from the server response.
    /// For client-side errors, returns a canonical status code or 0 if no HTTP response was received.
    pub fn status_code(&self) -> u16 {
        match self {
            // Server-sourced errors preserve the actual HTTP status code from the server
            TablesError::ServerError { status_code, .. }
            | TablesError::WarehouseNotFound { status_code, .. }
            | TablesError::WarehouseAlreadyExists { status_code, .. }
            | TablesError::NamespaceNotFound { status_code, .. }
            | TablesError::NamespaceAlreadyExists { status_code, .. }
            | TablesError::NamespaceNotEmpty { status_code, .. }
            | TablesError::TableNotFound { status_code, .. }
            | TablesError::TableAlreadyExists { status_code, .. }
            | TablesError::ViewNotFound { status_code, .. }
            | TablesError::ViewAlreadyExists { status_code, .. }
            | TablesError::BadRequest { status_code, .. }
            | TablesError::CommitFailed { status_code, .. } => *status_code,

            // Client-side commit/transaction errors (not from server)
            TablesError::CommitConflict { .. } | TablesError::TransactionFailed { .. } => 0,

            // Client-side validation errors -> 400
            TablesError::WarehouseNameInvalid { .. }
            | TablesError::NamespaceNameInvalid { .. }
            | TablesError::TableNameInvalid { .. } => 400,

            // Server-side failures -> 500
            TablesError::Generic(_) | TablesError::OrphanedMetadata { .. } => 500,

            // Client-side errors - no HTTP request was made
            TablesError::Validation(_) => 0,
            TablesError::Network(_) => 503,
        }
    }
}

impl From<NetworkError> for TablesError {
    fn from(err: NetworkError) -> Self {
        TablesError::Network(err)
    }
}

impl From<S3TablesValidationErr> for TablesError {
    fn from(err: S3TablesValidationErr) -> Self {
        TablesError::Validation(err)
    }
}

/// Tables API error response format
///
/// The MinIO Tables API returns errors in this JSON structure.
#[derive(Debug, Deserialize)]
pub struct TablesErrorResponse {
    /// Error details
    pub error: ErrorModel,
}

/// Error model from Tables API
#[derive(Debug, Deserialize)]
pub struct ErrorModel {
    /// HTTP status code
    pub code: i32,
    /// Human-readable error message
    pub message: String,
    /// Optional stack trace (for debugging)
    #[serde(default)]
    pub stack: Vec<String>,
    /// Error type identifier (e.g., "WarehouseNotFoundException")
    #[serde(rename = "type")]
    pub error_type: String,
}

impl fmt::Display for TablesErrorResponse {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "Tables API error ({}): {}",
            self.error.error_type, self.error.message
        )
    }
}

impl StdError for TablesErrorResponse {}

impl From<TablesErrorResponse> for TablesError {
    /// Convert server error response to TablesError.
    ///
    /// Follows the Apache Iceberg RCK (REST Compatibility Kit) approach:
    /// - Primary dispatch is based on HTTP status code
    /// - error.type() is used for disambiguation in certain cases
    ///
    /// All server-sourced error variants preserve the original status code, error type, and message
    /// for debugging and logging purposes.
    ///
    /// Reference: iceberg/core/src/main/java/org/apache/iceberg/rest/ErrorHandlers.java
    fn from(resp: TablesErrorResponse) -> Self {
        let error_type = resp.error.error_type.as_str();
        let message = resp.error.message;
        let status_code = resp.error.code as u16;

        // RCK approach: dispatch primarily by HTTP status code
        match status_code {
            // 400 Bad Request
            400 => {
                if error_type == "NamespaceNotEmptyException" {
                    TablesError::NamespaceNotEmpty {
                        namespace: extract_resource_name(&message).unwrap_or_default(),
                        status_code,
                        error_type: error_type.to_string(),
                        original_message: message,
                    }
                } else {
                    TablesError::BadRequest {
                        message: message.clone(),
                        status_code,
                        error_type: error_type.to_string(),
                        original_message: message,
                    }
                }
            }

            // 404 Not Found - use error_type to disambiguate
            404 => {
                match error_type {
                    // MinIO-specific warehouse errors
                    "IcebergWarehouseNotFound" => TablesError::WarehouseNotFound {
                        warehouse: extract_resource_name(&message).unwrap_or_default(),
                        status_code,
                        error_type: error_type.to_string(),
                        original_message: message,
                    },
                    // Standard Iceberg errors
                    "NoSuchNamespaceException" => TablesError::NamespaceNotFound {
                        namespace: extract_resource_name(&message).unwrap_or_default(),
                        status_code,
                        error_type: error_type.to_string(),
                        original_message: message,
                    },
                    "NoSuchTableException" => TablesError::TableNotFound {
                        table: extract_resource_name(&message).unwrap_or_default(),
                        status_code,
                        error_type: error_type.to_string(),
                        original_message: message,
                    },
                    "NoSuchViewException" => TablesError::ViewNotFound {
                        view: extract_resource_name(&message).unwrap_or_default(),
                        status_code,
                        error_type: error_type.to_string(),
                        original_message: message,
                    },
                    // Default 404 - treat as generic not found, preserve details
                    _ => TablesError::ServerError {
                        status_code,
                        error_type: error_type.to_string(),
                        message,
                    },
                }
            }

            // 409 Conflict - use error_type and message to disambiguate
            409 => match error_type {
                "IcebergWarehouseAlreadyExists" => TablesError::WarehouseAlreadyExists {
                    warehouse: extract_resource_name(&message).unwrap_or_default(),
                    status_code,
                    error_type: error_type.to_string(),
                    original_message: message,
                },
                "CommitFailedException" => TablesError::CommitFailed {
                    message: message.clone(),
                    status_code,
                    error_type: error_type.to_string(),
                    original_message: message,
                },
                "NamespaceNotEmptyException" => TablesError::NamespaceNotEmpty {
                    namespace: extract_resource_name(&message).unwrap_or_default(),
                    status_code,
                    error_type: error_type.to_string(),
                    original_message: message,
                },
                // AlreadyExistsException or unknown - infer from message
                _ => infer_already_exists_error(&message, status_code, error_type),
            },

            // All other status codes (including 5xx)
            _ => TablesError::ServerError {
                status_code,
                error_type: error_type.to_string(),
                message,
            },
        }
    }
}

/// Infer the specific "already exists" error type from message content.
fn infer_already_exists_error(message: &str, status_code: u16, error_type: &str) -> TablesError {
    let name = extract_resource_name(message).unwrap_or_default();
    if message.contains("Table") {
        TablesError::TableAlreadyExists {
            table: name,
            status_code,
            error_type: error_type.to_string(),
            original_message: message.to_string(),
        }
    } else if message.contains("View") {
        TablesError::ViewAlreadyExists {
            view: name,
            status_code,
            error_type: error_type.to_string(),
            original_message: message.to_string(),
        }
    } else if message.contains("Namespace") {
        TablesError::NamespaceAlreadyExists {
            namespace: name,
            status_code,
            error_type: error_type.to_string(),
            original_message: message.to_string(),
        }
    } else if message.contains("warehouse") {
        TablesError::WarehouseAlreadyExists {
            warehouse: name,
            status_code,
            error_type: error_type.to_string(),
            original_message: message.to_string(),
        }
    } else {
        TablesError::ServerError {
            status_code,
            error_type: error_type.to_string(),
            message: message.to_string(),
        }
    }
}

/// Extract resource name from error message.
/// Tries to find the resource identifier in common message formats.
fn extract_resource_name(message: &str) -> Option<String> {
    // Try common patterns like "Table already exists: namespace.table"
    // or "Namespace does not exist: namespace_name"
    if let Some(pos) = message.rfind(": ") {
        return Some(message[pos + 2..].trim().to_string());
    }
    // Try pattern like "The specified warehouse already exists."
    // In this case, return empty string (name not in message)
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_display() {
        let err = TablesError::WarehouseNotFound {
            warehouse: "test-warehouse".to_string(),
            status_code: 404,
            error_type: "IcebergWarehouseNotFound".to_string(),
            original_message: "Warehouse not found: test-warehouse".to_string(),
        };
        assert_eq!(err.to_string(), "Warehouse not found: test-warehouse");

        let err = TablesError::CommitFailed {
            message: "Requirements not met".to_string(),
            status_code: 409,
            error_type: "CommitFailedException".to_string(),
            original_message: "Requirements not met".to_string(),
        };
        assert_eq!(err.to_string(), "Commit failed: Requirements not met");
    }

    #[test]
    fn test_namespace_not_empty_error() {
        let err = TablesError::NamespaceNotEmpty {
            namespace: "test-namespace".to_string(),
            status_code: 400,
            error_type: "NamespaceNotEmptyException".to_string(),
            original_message: "Namespace is not empty: test-namespace".to_string(),
        };
        assert_eq!(err.to_string(), "Namespace is not empty: test-namespace");
    }

    #[test]
    fn test_status_code() {
        // Not found errors preserve status code from server
        assert_eq!(
            TablesError::WarehouseNotFound {
                warehouse: "wh".into(),
                status_code: 404,
                error_type: "IcebergWarehouseNotFound".into(),
                original_message: "Warehouse not found: wh".into(),
            }
            .status_code(),
            404
        );
        assert_eq!(
            TablesError::NamespaceNotFound {
                namespace: "ns".into(),
                status_code: 404,
                error_type: "NoSuchNamespaceException".into(),
                original_message: "Namespace does not exist: ns".into(),
            }
            .status_code(),
            404
        );
        assert_eq!(
            TablesError::TableNotFound {
                table: "t".into(),
                status_code: 404,
                error_type: "NoSuchTableException".into(),
                original_message: "Table does not exist: t".into(),
            }
            .status_code(),
            404
        );
        assert_eq!(
            TablesError::ViewNotFound {
                view: "v".into(),
                status_code: 404,
                error_type: "NoSuchViewException".into(),
                original_message: "View does not exist: v".into(),
            }
            .status_code(),
            404
        );

        // Conflict errors preserve status code from server
        assert_eq!(
            TablesError::WarehouseAlreadyExists {
                warehouse: "wh".into(),
                status_code: 409,
                error_type: "IcebergWarehouseAlreadyExists".into(),
                original_message: "Warehouse already exists: wh".into(),
            }
            .status_code(),
            409
        );

        // Bad request errors preserve status code from server
        assert_eq!(
            TablesError::BadRequest {
                message: "".into(),
                status_code: 400,
                error_type: "BadRequestException".into(),
                original_message: "".into(),
            }
            .status_code(),
            400
        );
        assert_eq!(
            TablesError::NamespaceNotEmpty {
                namespace: "ns".into(),
                status_code: 400,
                error_type: "NamespaceNotEmptyException".into(),
                original_message: "Namespace is not empty: ns".into(),
            }
            .status_code(),
            400
        );

        // Server-sourced commit errors preserve status code from server
        assert_eq!(
            TablesError::CommitFailed {
                message: "conflict".into(),
                status_code: 409,
                error_type: "CommitFailedException".into(),
                original_message: "conflict".into(),
            }
            .status_code(),
            409
        );

        // Client-side commit/transaction errors return 0
        assert_eq!(
            TablesError::CommitConflict { message: "".into() }.status_code(),
            0
        );
        assert_eq!(
            TablesError::TransactionFailed { message: "".into() }.status_code(),
            0
        );

        // Server errors -> 500
        assert_eq!(TablesError::Generic("".into()).status_code(), 500);

        // ServerError preserves actual status code from server
        assert_eq!(
            TablesError::ServerError {
                status_code: 409,
                error_type: "CommitFailedException".into(),
                message: "conflict".into(),
            }
            .status_code(),
            409
        );
        assert_eq!(
            TablesError::ServerError {
                status_code: 500,
                error_type: "InternalError".into(),
                message: "internal error".into(),
            }
            .status_code(),
            500
        );
        assert_eq!(
            TablesError::ServerError {
                status_code: 418,
                error_type: "TeapotException".into(),
                message: "I'm a teapot".into(),
            }
            .status_code(),
            418
        );

        // Client-side errors
        assert_eq!(
            TablesError::Validation(S3TablesValidationErr::new("field", "invalid")).status_code(),
            0
        );
    }
}
