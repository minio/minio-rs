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

//! Builder for RegisterTable operation
//!
//! Iceberg REST API: `POST /v1/{prefix}/namespaces/{namespace}/register`
//! Spec: <https://github.com/apache/iceberg/blob/main/open-api/rest-catalog-open-api.yaml#L559>

use crate::s3::error::ValidationErr;
use crate::s3::header_constants::IDEMPOTENCY_KEY;
use crate::s3::multimap_ext::{Multimap, MultimapExt};
use crate::s3tables::client::TablesClient;
use crate::s3tables::response::RegisterTableResponse;
use crate::s3tables::types::{TablesApi, TablesRequest, ToTablesRequest};
use crate::s3tables::utils::{
    MetadataLocation, Namespace, TableName, WarehouseName, namespace_path,
};
use http::Method;
use serde::Serialize;
use typed_builder::TypedBuilder;

/// Argument builder for RegisterTable operation
///
/// Registers an existing Iceberg table by referencing its metadata location.
#[derive(Clone, Debug, TypedBuilder)]
pub struct RegisterTable {
    #[builder(!default)]
    client: TablesClient,
    #[builder(!default)]
    warehouse_name: WarehouseName,
    #[builder(!default)]
    namespace: Namespace,
    #[builder(!default)]
    table_name: TableName,
    #[builder(!default)]
    metadata_location: MetadataLocation,
    /// Idempotency key for safe request retries (UUID format)
    #[builder(default, setter(into, strip_option))]
    idempotency_key: Option<String>,
}

/// Request body for RegisterTable
#[derive(Serialize)]
struct RegisterTableRequest {
    name: String,
    #[serde(rename = "metadata-location")]
    metadata_location: String,
}

impl TablesApi for RegisterTable {
    type TablesResponse = RegisterTableResponse;
}

/// Builder type for RegisterTable
pub type RegisterTableBldr = RegisterTableBuilder<(
    (TablesClient,),
    (WarehouseName,),
    (Namespace,),
    (TableName,),
    (MetadataLocation,),
    (),
)>;

impl ToTablesRequest for RegisterTable {
    fn to_tables_request(self) -> Result<TablesRequest, ValidationErr> {
        let mut headers = Multimap::new();

        // Add Idempotency-Key header if specified
        if let Some(key) = self.idempotency_key {
            headers.add(IDEMPOTENCY_KEY, key);
        }

        let request_body = RegisterTableRequest {
            name: self.table_name.into_inner(),
            metadata_location: self.metadata_location.into_inner(),
        };

        Ok(TablesRequest::builder()
            .client(self.client)
            .method(Method::POST)
            .path(format!(
                "{}/register",
                namespace_path(&self.warehouse_name, &self.namespace)
            ))
            .headers(headers)
            .body(Some(serde_json::to_vec(&request_body)?))
            .build())
    }
}
