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

use crate::madmin::madmin_client::MadminClient;
use crate::madmin::response::ListUsersResponse;
use crate::madmin::types::{MadminApi, MadminRequest, ToMadminRequest};
use crate::s3::error::Error;
use crate::s3::multimap_ext::Multimap;
use http::Method;
use typed_builder::TypedBuilder;

/// Argument builder for the List Users admin API operation.
///
/// This struct constructs the parameters required for the [`MadminClient::list_users`](crate::madmin::madmin_client::MadminClient::list_users) method.
#[derive(Clone, Debug, TypedBuilder)]
#[builder(doc)]
pub struct ListUsers {
    #[builder(!default)]
    client: MadminClient,
    #[builder(
        default,
        setter(into, doc = "Optional extra HTTP headers to include in the request")
    )]
    extra_headers: Option<Multimap>,
    #[builder(
        default,
        setter(
            into,
            doc = "Optional extra query parameters to include in the request"
        )
    )]
    extra_query_params: Option<Multimap>,
}

/// Builder type for [`ListUsers`].
pub type ListUsersBldr = ListUsersBuilder<((MadminClient,), (), ())>;

impl ToMadminRequest for ListUsers {
    fn to_madmin_request(self) -> Result<MadminRequest, Error> {
        Ok(MadminRequest::builder()
            .client(self.client)
            .method(Method::GET)
            .path("/list-users")
            .query_params(self.extra_query_params.unwrap_or_default())
            .headers(self.extra_headers.unwrap_or_default())
            .build())
    }
}

impl MadminApi for ListUsers {
    type MadminResponse = ListUsersResponse;
}
