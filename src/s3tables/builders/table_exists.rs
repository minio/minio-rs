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

//! Builder for TableExists operation
//!
//! Iceberg REST API: `HEAD /v1/{prefix}/namespaces/{namespace}/tables/{table}`
//! Spec: <https://github.com/apache/iceberg/blob/main/open-api/rest-catalog-open-api.yaml#L762>

use crate::s3::error::ValidationErr;
use crate::s3tables::client::TablesClient;
use crate::s3tables::response::TableExistsResponse;
use crate::s3tables::types::{TablesApi, TablesRequest, ToTablesRequest};
use crate::s3tables::utils::{Namespace, TableName, WarehouseName, table_path};
use http::Method;
use typed_builder::TypedBuilder;

/// Argument builder for TableExists operation
///
/// Checks if a table exists in a namespace.
///
/// # Example
///
/// ```no_run
/// use minio::s3::{MinioClient, creds::StaticProvider, http::BaseUrl};
/// use minio::s3tables::{TablesClient, TablesApi};
/// use minio::s3tables::utils::{Namespace, TableName, WarehouseName};
/// use minio::s3::types::S3Api;
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// let base_url = "http://localhost:9000/".parse::<BaseUrl>()?;
/// let provider = StaticProvider::new("minioadmin", "minioadmin", None);
/// let client = MinioClient::new(base_url, Some(provider), None, None)?;
/// let tables = TablesClient::new(client);
///
/// tables
///     .table_exists(
///         WarehouseName::try_from("my-warehouse")?,
///         Namespace::single("my-namespace")?,
///         TableName::new("my-table")?,
///     )
///     .build()
///     .send()
///     .await?;
/// # Ok(())
/// # }
/// ```
#[derive(Clone, Debug, TypedBuilder)]
pub struct TableExists {
    #[builder(!default)]
    client: TablesClient,
    #[builder(!default)]
    warehouse_name: WarehouseName,
    #[builder(!default)]
    namespace: Namespace,
    #[builder(!default)]
    table_name: TableName,
}

impl TablesApi for TableExists {
    type TablesResponse = TableExistsResponse;
}

/// Builder type for TableExists
pub type TableExistsBldr = TableExistsBuilder<(
    (TablesClient,),
    (WarehouseName,),
    (Namespace,),
    (TableName,),
)>;

impl ToTablesRequest for TableExists {
    fn to_tables_request(self) -> Result<TablesRequest, ValidationErr> {
        Ok(TablesRequest::builder()
            .client(self.client)
            .method(Method::HEAD)
            .path(table_path(
                &self.warehouse_name,
                &self.namespace,
                &self.table_name,
            ))
            .body(None)
            .build())
    }
}
