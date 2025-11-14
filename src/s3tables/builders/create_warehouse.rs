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

//! Builder for CreateWarehouse operation

use crate::s3::error::ValidationErr;
use crate::s3tables::client::TablesClient;
use crate::s3tables::response::CreateWarehouseResponse;
use crate::s3tables::types::{TablesApi, TablesRequest, ToTablesRequest};
use http::Method;
use serde::Serialize;
use typed_builder::TypedBuilder;

/// Argument builder for CreateWarehouse operation
///
/// Creates a new warehouse (table bucket) in the Tables catalog.
///
/// # Example
///
/// ```no_run
/// use minio::s3::{MinioClient, creds::StaticProvider, http::BaseUrl};
/// use minio::s3tables::{TablesClient, TablesApi};
/// use minio::s3::types::S3Api;
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// let base_url = "http://localhost:9000/".parse::<BaseUrl>()?;
/// let provider = StaticProvider::new("minioadmin", "minioadmin", None);
/// let client = MinioClient::new(base_url, Some(provider), None, None)?;
/// let tables = TablesClient::new(client);
///
/// let response = tables
///     .create_warehouse("analytics")
///     .upgrade_existing(true)
///     .build()
///     .send()
///     .await?;
///
/// println!("Created warehouse: {}", response.name()?);
/// # Ok(())
/// # }
/// ```
#[derive(Clone, Debug, TypedBuilder)]
pub struct CreateWarehouse {
    #[builder(!default)]
    client: TablesClient,
    #[builder(!default, setter(into))]
    warehouse_name: String,
    #[builder(default = false)]
    upgrade_existing: bool,
}

/// Request body for CreateWarehouse
#[derive(Serialize)]
struct CreateWarehouseRequest {
    name: String,
    #[serde(rename = "upgrade-existing", skip_serializing_if = "is_false")]
    upgrade_existing: bool,
}

fn is_false(b: &bool) -> bool {
    !*b
}

impl TablesApi for CreateWarehouse {
    type TablesResponse = CreateWarehouseResponse;
}

/// Builder type for CreateWarehouse
pub type CreateWarehouseBldr = CreateWarehouseBuilder<((TablesClient,), (String,), ())>;

impl ToTablesRequest for CreateWarehouse {
    fn to_tables_request(self) -> Result<TablesRequest, ValidationErr> {
        // Validate warehouse name
        if self.warehouse_name.is_empty() {
            return Err(ValidationErr::InvalidWarehouseName(
                "warehouse name cannot be empty".to_string(),
            ));
        }

        // TODO: Add more validation (length, characters, etc.)

        let request_body = CreateWarehouseRequest {
            name: self.warehouse_name,
            upgrade_existing: self.upgrade_existing,
        };

        let body = serde_json::to_vec(&request_body).map_err(|e| {
            ValidationErr::InvalidWarehouseName(format!("JSON serialization failed: {e}"))
        })?;

        Ok(TablesRequest {
            client: self.client,
            method: Method::POST,
            path: "/warehouses".to_string(),
            query_params: Default::default(),
            headers: Default::default(),
            body: Some(body),
        })
    }
}
