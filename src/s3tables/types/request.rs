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

//! Request types and traits for S3 Tables operations

use crate::s3::error::{Error, ValidationErr};
use typed_builder::TypedBuilder;

/// Request structure for Tables API operations
#[derive(Clone, Debug, TypedBuilder)]
pub struct TablesRequest {
    /// Client reference
    #[builder(!default)]
    pub client: crate::s3tables::TablesClient,
    /// HTTP method
    #[builder(!default)]
    pub method: http::Method,
    /// Request path (relative to base path)
    #[builder(!default, setter(into))]
    pub path: String,
    /// Query parameters
    #[builder(default)]
    pub query_params: crate::s3::multimap_ext::Multimap,
    /// Request headers
    #[builder(default)]
    pub headers: crate::s3::multimap_ext::Multimap,
    /// Request body
    #[builder(default)]
    pub body: Option<Vec<u8>>,
}

impl TablesRequest {
    /// Execute the Tables API request
    ///
    /// # Errors
    ///
    /// Returns `Error` if the HTTP request fails or the server returns an error.
    pub(crate) async fn execute(&mut self) -> Result<reqwest::Response, Error> {
        let full_path = format!("{}{}", self.client.base_path(), self.path);

        self.client
            .execute_tables(
                self.method.clone(),
                full_path,
                &mut self.headers,
                &self.query_params,
                self.body.take(),
            )
            .await
    }
}

/// Convert builder to TablesRequest
pub trait ToTablesRequest {
    /// Convert this builder into a TablesRequest
    ///
    /// # Errors
    ///
    /// Returns `ValidationErr` if the request parameters are invalid.
    fn to_tables_request(self) -> Result<TablesRequest, ValidationErr>;
}

/// Execute Tables API operation
pub trait TablesApi: ToTablesRequest {
    /// Response type for this operation
    type TablesResponse: FromTablesResponse;

    /// Send the request and await the response
    ///
    /// # Errors
    ///
    /// Returns `Error` if the request fails or the response cannot be parsed.
    fn send(self) -> impl std::future::Future<Output = Result<Self::TablesResponse, Error>> + Send
    where
        Self: Sized + Send,
    {
        async {
            let mut request: TablesRequest = self.to_tables_request()?;
            let response: Result<reqwest::Response, Error> = request.execute().await;
            Self::TablesResponse::from_table_response(request, response).await
        }
    }
}

/// Parse response from Tables API
#[async_trait::async_trait]
pub trait FromTablesResponse: Sized {
    /// Parse the response from a TablesRequest
    ///
    /// # Errors
    ///
    /// Returns `Error` if the response cannot be parsed or contains an error.
    async fn from_table_response(
        request: TablesRequest,
        response: Result<reqwest::Response, Error>,
    ) -> Result<Self, Error>;
}
