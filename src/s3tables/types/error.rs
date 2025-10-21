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
use std::fmt;

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
    },
    /// Warehouse already exists
    WarehouseAlreadyExists {
        /// Name of the warehouse that already exists
        warehouse: String,
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
    },
    /// Namespace already exists
    NamespaceAlreadyExists {
        /// Name of the namespace that already exists
        namespace: String,
    },
    /// Invalid namespace name
    NamespaceNameInvalid {
        /// The invalid namespace name
        namespace: String,
        /// Reason why the name is invalid
        cause: String,
    },

    // Table errors
    /// Table not found
    TableNotFound {
        /// Name of the table that was not found
        table: String,
    },
    /// Table already exists
    TableAlreadyExists {
        /// Name of the table that already exists
        table: String,
    },
    /// Invalid table name
    TableNameInvalid {
        /// The invalid table name
        table: String,
        /// Reason why the name is invalid
        cause: String,
    },

    // Operation errors
    /// Bad request - invalid parameters or malformed request
    BadRequest {
        /// Description of what was invalid
        message: String,
    },
    /// Commit operation failed
    CommitFailed {
        /// Description of why the commit failed
        message: String,
    },
    /// Commit conflict - requirements not met
    CommitConflict {
        /// Description of the conflict
        message: String,
    },
    /// Multi-table transaction failed
    TransactionFailed {
        /// Description of why the transaction failed
        message: String,
    },

    // Wrapped errors
    /// Network error during request
    Network(NetworkError),
    /// Validation error for request parameters
    Validation(ValidationErr),
    /// Generic error with custom message
    Generic(String),
}

impl fmt::Display for TablesError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TablesError::WarehouseNotFound { warehouse } => {
                write!(f, "Warehouse not found: {warehouse}")
            }
            TablesError::WarehouseAlreadyExists { warehouse } => {
                write!(f, "Warehouse already exists: {warehouse}")
            }
            TablesError::WarehouseNameInvalid { warehouse, cause } => {
                write!(f, "Invalid warehouse name '{warehouse}': {cause}")
            }
            TablesError::NamespaceNotFound { namespace } => {
                write!(f, "Namespace not found: {namespace}")
            }
            TablesError::NamespaceAlreadyExists { namespace } => {
                write!(f, "Namespace already exists: {namespace}")
            }
            TablesError::NamespaceNameInvalid { namespace, cause } => {
                write!(f, "Invalid namespace name '{namespace}': {cause}")
            }
            TablesError::TableNotFound { table } => {
                write!(f, "Table not found: {table}")
            }
            TablesError::TableAlreadyExists { table } => {
                write!(f, "Table already exists: {table}")
            }
            TablesError::TableNameInvalid { table, cause } => {
                write!(f, "Invalid table name '{table}': {cause}")
            }
            TablesError::BadRequest { message } => {
                write!(f, "Bad request: {message}")
            }
            TablesError::CommitFailed { message } => {
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

impl From<NetworkError> for TablesError {
    fn from(err: NetworkError) -> Self {
        TablesError::Network(err)
    }
}

impl From<ValidationErr> for TablesError {
    fn from(err: ValidationErr) -> Self {
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

impl From<TablesErrorResponse> for TablesError {
    fn from(resp: TablesErrorResponse) -> Self {
        let error_type = resp.error.error_type.as_str();
        let message = resp.error.message.clone();

        // Map error types to specific variants
        // Support both AWS-style "Exception" suffix and Iceberg-style names
        match error_type {
            "WarehouseNotFoundException" | "IcebergWarehouseNotFound" => {
                TablesError::WarehouseNotFound {
                    warehouse: extract_resource_name(&message)
                        .unwrap_or_else(|| "unknown".to_string()),
                }
            }
            "WarehouseAlreadyExistsException" | "IcebergWarehouseAlreadyExists" => {
                TablesError::WarehouseAlreadyExists {
                    warehouse: extract_resource_name(&message)
                        .unwrap_or_else(|| "unknown".to_string()),
                }
            }
            "WarehouseNameInvalidException" | "IcebergWarehouseNameInvalid" => {
                TablesError::WarehouseNameInvalid {
                    warehouse: extract_resource_name(&message)
                        .unwrap_or_else(|| "unknown".to_string()),
                    cause: message,
                }
            }
            "NamespaceNotFoundException" | "IcebergNamespaceNotFound" => {
                TablesError::NamespaceNotFound {
                    namespace: extract_resource_name(&message)
                        .unwrap_or_else(|| "unknown".to_string()),
                }
            }
            "NamespaceAlreadyExistsException" | "IcebergNamespaceAlreadyExists" => {
                TablesError::NamespaceAlreadyExists {
                    namespace: extract_resource_name(&message)
                        .unwrap_or_else(|| "unknown".to_string()),
                }
            }
            "NamespaceNameInvalidException" | "IcebergNamespaceNameInvalid" => {
                TablesError::NamespaceNameInvalid {
                    namespace: extract_resource_name(&message)
                        .unwrap_or_else(|| "unknown".to_string()),
                    cause: message,
                }
            }
            "TableNotFoundException" | "IcebergTableNotFound" => TablesError::TableNotFound {
                table: extract_resource_name(&message).unwrap_or_else(|| "unknown".to_string()),
            },
            "TableAlreadyExistsException" | "IcebergTableAlreadyExists" => {
                TablesError::TableAlreadyExists {
                    table: extract_resource_name(&message).unwrap_or_else(|| "unknown".to_string()),
                }
            }
            "TableNameInvalidException" | "IcebergTableNameInvalid" => {
                TablesError::TableNameInvalid {
                    table: extract_resource_name(&message).unwrap_or_else(|| "unknown".to_string()),
                    cause: message,
                }
            }
            "CommitFailedException" | "IcebergCommitFailed" => {
                TablesError::CommitFailed { message }
            }
            "CommitConflictException" | "IcebergCommitConflict" => {
                TablesError::CommitConflict { message }
            }
            "TransactionFailedException" | "IcebergTransactionFailed" => {
                TablesError::TransactionFailed { message }
            }
            "BadRequestException" | "IcebergBadRequest" => TablesError::BadRequest { message },
            _ => TablesError::Generic(message),
        }
    }
}

/// Extract resource name from error message
///
/// Attempts to extract the resource name from error messages like
/// "Warehouse 'my-warehouse' not found"
fn extract_resource_name(message: &str) -> Option<String> {
    // Look for text between single quotes
    if let Some(start) = message.find('\'')
        && let Some(end) = message[start + 1..].find('\'')
    {
        return Some(message[start + 1..start + 1 + end].to_string());
    }
    // Look for text between double quotes
    if let Some(start) = message.find('"')
        && let Some(end) = message[start + 1..].find('"')
    {
        return Some(message[start + 1..start + 1 + end].to_string());
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extract_resource_name() {
        assert_eq!(
            extract_resource_name("Warehouse 'my-warehouse' not found"),
            Some("my-warehouse".to_string())
        );
        assert_eq!(
            extract_resource_name("Table \"users\" already exists"),
            Some("users".to_string())
        );
        assert_eq!(extract_resource_name("No quotes here"), None);
    }

    #[test]
    fn test_error_display() {
        let err = TablesError::WarehouseNotFound {
            warehouse: "test-warehouse".to_string(),
        };
        assert_eq!(err.to_string(), "Warehouse not found: test-warehouse");

        let err = TablesError::CommitFailed {
            message: "Requirements not met".to_string(),
        };
        assert_eq!(err.to_string(), "Commit failed: Requirements not met");
    }
}
