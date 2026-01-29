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

//! Builder for ListWarehouses operation
//!
//! AWS S3 Tables API: `GET /buckets`
//! Spec: <https://docs.aws.amazon.com/AmazonS3/latest/API/API_s3tables_ListTableBuckets.html>

use crate::s3::error::ValidationErr;
use crate::s3::header_constants::{PAGE_SIZE, PAGE_TOKEN};
use crate::s3::multimap_ext::{Multimap, MultimapExt};
use crate::s3tables::client::TablesClient;
use crate::s3tables::response::ListWarehousesResponse;
use crate::s3tables::types::{TablesApi, TablesRequest, ToTablesRequest};
use crate::s3tables::utils::PageSize;
use http::Method;
use typed_builder::TypedBuilder;

/// Argument builder for ListWarehouses operation
///
/// Lists all warehouses (table buckets) in the Tables catalog.
///
/// # Example
///
/// ```no_run
/// use minio::s3::{MinioClient, creds::StaticProvider, http::BaseUrl};
/// use minio::s3tables::{TablesClient, TablesApi};
/// use minio::s3tables::utils::PageSize;
/// use minio::s3::types::S3Api;
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// let base_url = "http://localhost:9000/".parse::<BaseUrl>()?;
/// let provider = StaticProvider::new("minioadmin", "minioadmin", None);
/// let client = MinioClient::new(base_url, Some(provider), None, None)?;
/// let tables = TablesClient::new(client);
///
/// let response = tables
///     .list_warehouses()
///     .page_size(PageSize::new(100)?)
///     .build()
///     .send()
///     .await?;
///
/// for warehouse in response.warehouses()? {
///     println!("Warehouse: {}", warehouse);
/// }
/// # Ok(())
/// # }
/// ```
#[derive(Clone, Debug, TypedBuilder)]
pub struct ListWarehouses {
    #[builder(!default)]
    client: TablesClient,
    #[builder(default, setter(strip_option))]
    page_size: Option<PageSize>,
    #[builder(default, setter(into, strip_option))]
    page_token: Option<crate::s3tables::types::ContinuationToken>,
}

impl TablesApi for ListWarehouses {
    type TablesResponse = ListWarehousesResponse;
}

/// Builder type for ListWarehouses
pub type ListWarehousesBldr = ListWarehousesBuilder<((TablesClient,), (), ())>;

impl ToTablesRequest for ListWarehouses {
    fn to_tables_request(self) -> Result<TablesRequest, ValidationErr> {
        let mut query_params = Multimap::new();

        if let Some(size) = self.page_size {
            query_params.add(PAGE_SIZE, size.to_string());
        }

        if let Some(token) = self.page_token {
            query_params.add(PAGE_TOKEN, token.as_str());
        }

        Ok(TablesRequest::builder()
            .client(self.client)
            .method(Method::GET)
            .path("/warehouses".to_string())
            .query_params(query_params)
            .body(None)
            .build())
    }
}
