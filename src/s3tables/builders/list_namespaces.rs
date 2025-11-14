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

use crate::s3::error::ValidationErr;
use crate::s3::multimap_ext::{Multimap, MultimapExt};
use crate::s3tables::client::TablesClient;
use crate::s3tables::response::ListNamespacesResponse;
use crate::s3tables::types::{TablesApi, TablesRequest, ToTablesRequest};
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
///     .list_namespaces("my-warehouse")
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
///     .list_namespaces("my-warehouse")
///     .parent(vec!["analytics".to_string()])
///     .max_list(50)
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
    #[builder(!default, setter(into))]
    warehouse_name: String,
    #[builder(default, setter(into, strip_option))]
    parent: Option<Vec<String>>,
    #[builder(default, setter(into, strip_option))]
    max_list: Option<i32>,
    #[builder(default, setter(into, strip_option))]
    page_token: Option<String>,
}

impl TablesApi for ListNamespaces {
    type TablesResponse = ListNamespacesResponse;
}

/// Builder type for ListNamespaces
pub type ListNamespacesBldr = ListNamespacesBuilder<((TablesClient,), (String,), (), (), ())>;

impl ToTablesRequest for ListNamespaces {
    fn to_tables_request(self) -> Result<TablesRequest, ValidationErr> {
        if self.warehouse_name.is_empty() {
            return Err(ValidationErr::InvalidWarehouseName(
                "warehouse name cannot be empty".to_string(),
            ));
        }

        let mut query_params = Multimap::new();

        if let Some(parent) = self.parent {
            if parent.is_empty() {
                return Err(ValidationErr::InvalidNamespaceName(
                    "parent namespace cannot be empty".to_string(),
                ));
            }
            query_params.add("parent", parent.join("\u{001F}"));
        }

        if let Some(max) = self.max_list {
            if max <= 0 {
                return Err(ValidationErr::InvalidNamespaceName(
                    "max-list must be positive".to_string(),
                ));
            }
            query_params.add("max-list", max.to_string());
        }

        if let Some(token) = self.page_token {
            query_params.add("page-token", token);
        }

        Ok(TablesRequest {
            client: self.client,
            method: Method::GET,
            path: format!("/{}/namespaces", self.warehouse_name),
            query_params,
            headers: Default::default(),
            body: None,
        })
    }
}
