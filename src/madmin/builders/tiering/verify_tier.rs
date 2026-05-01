use crate::madmin::madmin_client::MadminClient;
use crate::madmin::response::VerifyTierResponse;
use crate::madmin::types::{MadminApi, MadminRequest, TierName, ToMadminRequest};
use crate::s3::error::Error;
use crate::s3::multimap_ext::Multimap;
use http::Method;
use typed_builder::TypedBuilder;

#[derive(Clone, Debug, TypedBuilder)]
#[builder(doc)]
pub struct VerifyTier {
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
    #[builder(!default, setter(into, doc = "Name of the tier to verify"))]
    tier_name: TierName,
}

/// Builder type for [`VerifyTier`].
pub type VerifyTierBldr = VerifyTierBuilder<((MadminClient,), (), (), (TierName,))>;

impl ToMadminRequest for VerifyTier {
    fn to_madmin_request(self) -> Result<MadminRequest, Error> {
        Ok(MadminRequest::builder()
            .client(self.client)
            .method(Method::GET)
            .path(format!("/tier/{}", self.tier_name))
            .query_params(self.extra_query_params.unwrap_or_default())
            .headers(self.extra_headers.unwrap_or_default())
            .api_version(3)
            .build())
    }
}

impl MadminApi for VerifyTier {
    type MadminResponse = VerifyTierResponse;
}
