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
//!
//! AWS S3 Tables API: `DELETE /buckets/{tableBucketARN}`
//! Spec: <https://docs.aws.amazon.com/AmazonS3/latest/API/API_s3tables_DeleteTableBucket.html>

use crate::s3::error::ValidationErr;
use crate::s3::header_constants::{FORCE, PRESERVE_BUCKET};
use crate::s3::multimap_ext::{Multimap, MultimapExt};
use crate::s3tables::client::TablesClient;
use crate::s3tables::response::DeleteWarehouseResponse;
use crate::s3tables::types::{TablesApi, TablesRequest, ToTablesRequest};
use crate::s3tables::utils::WarehouseName;
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
/// use minio::s3tables::utils::WarehouseName;
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
///     .delete_warehouse(WarehouseName::try_from("my-warehouse")?)
///     .build()
///     .send()
///     .await?;
///
/// // Delete warehouse but keep the bucket
/// tables
///     .delete_warehouse(WarehouseName::try_from("my-warehouse")?)
///     .preserve_bucket(true)
///     .build()
///     .send()
///     .await?;
///
/// // Force delete warehouse with stale metadata
/// tables
///     .delete_warehouse(WarehouseName::try_from("zombie-warehouse")?)
///     .force(true)
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
    #[builder(!default)]
    warehouse_name: WarehouseName,
    #[builder(default = false)]
    preserve_bucket: bool,
    #[builder(default = false)]
    force: bool,
}

impl TablesApi for DeleteWarehouse {
    type TablesResponse = DeleteWarehouseResponse;
}

/// Builder type for DeleteWarehouse
pub type DeleteWarehouseBldr = DeleteWarehouseBuilder<((TablesClient,), (WarehouseName,), (), ())>;

impl ToTablesRequest for DeleteWarehouse {
    fn to_tables_request(self) -> Result<TablesRequest, ValidationErr> {
        let mut query_params = Multimap::new();
        if self.preserve_bucket {
            query_params.add(PRESERVE_BUCKET, "true");
        }
        if self.force {
            query_params.add(FORCE, "true");
        }

        Ok(TablesRequest::builder()
            .client(self.client)
            .method(Method::DELETE)
            .path(format!("/warehouses/{}", self.warehouse_name.as_str()))
            .query_params(query_params)
            .body(None)
            .build())
    }
}
