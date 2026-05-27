//TODO no copyright?

use crate::madmin::types::{FromMadminResponse, MadminRequest};
use crate::s3::error::Error;
use async_trait::async_trait;

#[derive(Debug, Clone)]
pub struct CancelServerUpdateResponse;

//TODO did you forget to refactor from_madmin_response here?
#[async_trait]
impl FromMadminResponse for CancelServerUpdateResponse {
    async fn from_madmin_response(
        _request: MadminRequest,
        response: Result<reqwest::Response, Error>,
    ) -> Result<Self, Error> {
        response?;
        Ok(CancelServerUpdateResponse)
    }
}
