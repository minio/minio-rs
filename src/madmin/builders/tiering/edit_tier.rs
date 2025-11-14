use crate::madmin::madmin_client::MadminClient;
use crate::madmin::response::EditTierResponse;
use crate::madmin::types::tier::TierCreds;
use crate::madmin::types::{MadminApi, MadminRequest, TierName, ToMadminRequest};
use crate::s3::error::Error;
use crate::s3::multimap_ext::Multimap;
use crate::s3::segmented_bytes::SegmentedBytes;
use http::Method;
use std::sync::Arc;
use typed_builder::TypedBuilder;

#[derive(Clone, Debug, TypedBuilder)]
#[builder(doc)]
pub struct EditTier {
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
    #[builder(!default, setter(into, doc = "Name of the tier to edit"))]
    tier_name: TierName,
    #[builder(!default)]
    creds: TierCreds,
}

/// Builder type for [`EditTier`].
pub type EditTierBldr = EditTierBuilder<((MadminClient,), (), (), (TierName,), (TierCreds,))>;

impl ToMadminRequest for EditTier {
    fn to_madmin_request(self) -> Result<MadminRequest, Error> {
        let data = serde_json::to_vec(&self.creds)
            .map_err(|e| Error::Validation(crate::s3::error::ValidationErr::JsonError(e)))?;
        Ok(MadminRequest::builder()
            .client(self.client)
            .method(Method::POST)
            .path(format!("/tier/{}", self.tier_name))
            .query_params(self.extra_query_params.unwrap_or_default())
            .headers(self.extra_headers.unwrap_or_default())
            .body(Some(Arc::new(SegmentedBytes::from(bytes::Bytes::from(
                data,
            )))))
            .api_version(3)
            .build())
    }
}

impl MadminApi for EditTier {
    type MadminResponse = EditTierResponse;
}
