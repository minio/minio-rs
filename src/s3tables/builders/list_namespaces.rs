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

//! Builder for ListNamespaces operation
//!
//! Iceberg REST API: `GET /v1/{prefix}/namespaces`
//! Spec: <https://github.com/apache/iceberg/blob/main/open-api/rest-catalog-open-api.yaml#L195>

use crate::s3::error::ValidationErr;
use crate::s3::header_constants::{PAGE_SIZE, PAGE_TOKEN, PARENT};
use crate::s3::multimap_ext::{Multimap, MultimapExt};
use crate::s3tables::client::TablesClient;
use crate::s3tables::response::ListNamespacesResponse;
use crate::s3tables::types::{TablesApi, TablesRequest, ToTablesRequest};
use crate::s3tables::utils::{Namespace, PageSize, WarehouseName};
use http::Method;
use typed_builder::TypedBuilder;

/// Argument builder for ListNamespaces operation
///
/// Lists namespaces within a warehouse, optionally filtered by parent namespace.
///
/// # Example
///
/// ```no_run
/// use minio::s3::{MinioClient, creds::StaticProvider, http::BaseUrl};
/// use minio::s3tables::{TablesClient, TablesApi};
/// use minio::s3tables::utils::{Namespace, PageSize, WarehouseName};
/// use minio::s3::types::S3Api;
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// let base_url = "http://localhost:9000/".parse::<BaseUrl>()?;
/// let provider = StaticProvider::new("minioadmin", "minioadmin", None);
/// let client = MinioClient::new(base_url, Some(provider), None, None)?;
/// let tables = TablesClient::new(client);
///
/// // List all namespaces
/// let response = tables
///     .list_namespaces(WarehouseName::try_from("my-warehouse")?)
///     .build()
///     .send()
///     .await?;
///
/// for namespace in response.namespaces()? {
///     println!("Namespace: {:?}", namespace);
/// }
///
/// // List namespaces under a parent
/// let response = tables
///     .list_namespaces(WarehouseName::try_from("my-warehouse")?)
///     .parent(Namespace::single("analytics")?)
///     .page_size(PageSize::new(50)?)
///     .build()
///     .send()
///     .await?;
/// # Ok(())
/// # }
/// ```
#[derive(Clone, Debug, TypedBuilder)]
pub struct ListNamespaces {
    #[builder(!default)]
    client: TablesClient,
    #[builder(!default)]
    warehouse_name: WarehouseName,
    #[builder(default, setter(strip_option))]
    parent: Option<Namespace>,
    #[builder(default, setter(strip_option))]
    page_size: Option<PageSize>,
    #[builder(default, setter(into, strip_option))]
    page_token: Option<crate::s3tables::types::ContinuationToken>,
}

impl TablesApi for ListNamespaces {
    type TablesResponse = ListNamespacesResponse;
}

/// Builder type for ListNamespaces
pub type ListNamespacesBldr =
    ListNamespacesBuilder<((TablesClient,), (WarehouseName,), (), (), ())>;

impl ToTablesRequest for ListNamespaces {
    fn to_tables_request(self) -> Result<TablesRequest, ValidationErr> {
        let mut query_params = Multimap::new();

        if let Some(parent) = self.parent {
            query_params.add(PARENT, parent.as_slice().join("\u{001F}"));
        }

        if let Some(size) = self.page_size {
            query_params.add(PAGE_SIZE, size.to_string());
        }

        if let Some(token) = self.page_token {
            query_params.add(PAGE_TOKEN, token.as_str());
        }

        Ok(TablesRequest::builder()
            .client(self.client)
            .method(Method::GET)
            .path(format!("/{}/namespaces", self.warehouse_name.as_str()))
            .query_params(query_params)
            .body(None)
            .build())
    }
}
