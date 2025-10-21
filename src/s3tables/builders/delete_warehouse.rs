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

//! Builder for DeleteWarehouse operation

use crate::s3::error::ValidationErr;
use crate::s3::multimap_ext::{Multimap, MultimapExt};
use crate::s3tables::client::TablesClient;
use crate::s3tables::response::DeleteWarehouseResponse;
use crate::s3tables::types::{TablesApi, TablesRequest, ToTablesRequest};
use http::Method;
use typed_builder::TypedBuilder;

/// Argument builder for DeleteWarehouse operation
///
/// Deletes a warehouse (table bucket) from the catalog.
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
/// // Delete warehouse and its underlying bucket
/// tables
///     .delete_warehouse("my-warehouse")
///     .build()
///     .send()
///     .await?;
///
/// // Delete warehouse but keep the bucket
/// tables
///     .delete_warehouse("my-warehouse")
///     .preserve_bucket(true)
///     .build()
///     .send()
///     .await?;
/// # Ok(())
/// # }
/// ```
#[derive(Clone, Debug, TypedBuilder)]
pub struct DeleteWarehouse {
    #[builder(!default)]
    client: TablesClient,
    #[builder(!default, setter(into))]
    warehouse_name: String,
    #[builder(default = false)]
    preserve_bucket: bool,
}

impl TablesApi for DeleteWarehouse {
    type TablesResponse = DeleteWarehouseResponse;
}

/// Builder type for DeleteWarehouse
pub type DeleteWarehouseBldr = DeleteWarehouseBuilder<((TablesClient,), (String,), ())>;

impl ToTablesRequest for DeleteWarehouse {
    fn to_tables_request(self) -> Result<TablesRequest, ValidationErr> {
        if self.warehouse_name.is_empty() {
            return Err(ValidationErr::InvalidWarehouseName(
                "warehouse name cannot be empty".to_string(),
            ));
        }

        let mut query_params = Multimap::new();
        if self.preserve_bucket {
            query_params.add("preserve-bucket", "true");
        }

        Ok(TablesRequest {
            client: self.client,
            method: Method::DELETE,
            path: format!("/warehouses/{}", self.warehouse_name),
            query_params,
            headers: Default::default(),
            body: None,
        })
    }
}
