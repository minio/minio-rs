use crate::madmin::madmin_client::MadminClient;
use crate::madmin::response::RemoveTierResponse;
use crate::madmin::types::{MadminApi, MadminRequest, TierName, ToMadminRequest};
use crate::s3::error::Error;
use crate::s3::multimap_ext::{Multimap, MultimapExt};
use http::Method;
use typed_builder::TypedBuilder;

#[derive(Clone, Debug, TypedBuilder)]
#[builder(doc)]
pub struct RemoveTier {
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
    #[builder(!default, setter(into, doc = "Name of the tier to remove"))]
    tier_name: TierName,
    #[builder(default = false)]
    force: bool,
}

/// Builder type for [`RemoveTier`].
pub type RemoveTierBldr = RemoveTierBuilder<((MadminClient,), (), (), (TierName,), ())>;

impl ToMadminRequest for RemoveTier {
    fn to_madmin_request(self) -> Result<MadminRequest, Error> {
        let mut query_params = self.extra_query_params.unwrap_or_default();
        query_params.add("force", self.force.to_string());
        Ok(MadminRequest::builder()
            .client(self.client)
            .method(Method::DELETE)
            .path(format!("/tier/{}", self.tier_name))
            .query_params(query_params)
            .headers(self.extra_headers.unwrap_or_default())
            .api_version(3)
            .build())
    }
}

impl MadminApi for RemoveTier {
    type MadminResponse = RemoveTierResponse;
}
