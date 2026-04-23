use crate::madmin::madmin_client::MadminClient;
use crate::madmin::response::AddTierResponse;
use crate::madmin::types::tier::TierConfig;
use crate::madmin::types::{MadminApi, MadminRequest, ToMadminRequest};
use crate::s3::error::Error;
use crate::s3::multimap_ext::{Multimap, MultimapExt};
use crate::s3::segmented_bytes::SegmentedBytes;
use http::Method;
use std::sync::Arc;
use typed_builder::TypedBuilder;

#[derive(Clone, Debug, TypedBuilder)]
#[builder(doc)]
pub struct AddTier {
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
    #[builder(!default)]
    config: TierConfig,
    #[builder(default = false)]
    force: bool,
}

impl ToMadminRequest for AddTier {
    fn to_madmin_request(self) -> Result<MadminRequest, Error> {
        let data = serde_json::to_vec(&self.config)
            .map_err(|e| Error::Validation(crate::s3::error::ValidationErr::JsonError(e)))?;
        let mut query_params = self.extra_query_params.unwrap_or_default();
        query_params.add("force", self.force.to_string());

        Ok(MadminRequest::builder()
            .client(self.client)
            .method(Method::PUT)
            .path("/tier")
            .query_params(query_params)
            .headers(self.extra_headers.unwrap_or_default())
            .body(Some(Arc::new(SegmentedBytes::from(bytes::Bytes::from(
                data,
            )))))
            .api_version(3)
            .build())
    }
}

impl MadminApi for AddTier {
    type MadminResponse = AddTierResponse;
}
