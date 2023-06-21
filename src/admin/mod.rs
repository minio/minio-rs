pub mod args;
pub mod error;
pub mod response;
pub mod types;
pub mod utils;

use crate::s3::client::Client;
use args::*;
use error::Error;
use hyper::http::Method;
use response::*;
use types::Quota;
use utils::{merge, Multimap};

#[derive(Clone, Debug)]
pub struct AdminClient<'a> {
    pub client: &'a Client<'a>,
}

/// Methods not possible to port:
///     - add_user (uses DARE, not easy to port)
impl<'a> AdminClient<'a> {
    pub async fn get_bucket_quota(
        &self,
        args: &GetBucketQuotaArgs<'_>,
    ) -> Result<GetBucketQuotaResponse, Error> {
        let mut headers = Multimap::new();
        if let Some(v) = &args.extra_headers {
            merge(&mut headers, v);
        }
        let mut query_params = Multimap::new();
        query_params.insert("bucket".into(), args.bucket_name.into());

        let resp = self
            .client
            .execute(
                Method::GET,
                &"us-east-1".into(),
                &mut headers,
                &query_params,
                "minio/admin/v3/get-bucket-quota".into(),
                None,
                None,
            )
            .await?;

        let headers = resp.headers().clone();
        let body = resp.bytes().await.unwrap().to_vec();
        let quota: Quota = serde_json::from_str(&String::from_utf8(body).unwrap())?;

        Ok(GetBucketQuotaResponse {
            headers,
            bucket_name: args.bucket_name.into(),
            quota,
        })
    }
}

impl<'a> AdminClient<'a> {
    pub async fn clear_bucket_quota(
        &self,
        args: &ClearBucketQuotaArgs<'_>,
    ) -> Result<ClearBucketQuotaResponse, Error> {
        let mut headers = Multimap::new();
        if let Some(v) = &args.extra_headers {
            merge(&mut headers, v);
        }
        let mut query_params = Multimap::new();
        query_params.insert("bucket".into(), args.bucket_name.into());

        let resp = self
            .client
            .execute(
                Method::DELETE,
                &"us-east-1".into(),
                &mut headers,
                &query_params,
                "minio/admin/v3/clear-bucket-quota".into(),
                None,
                None,
            )
            .await?;

        let headers = resp.headers().clone();

        Ok(ClearBucketQuotaResponse{
            headers,
            bucket_name: args.bucket_name.into(),
        })
    }

    pub async fn set_bucket_quota(
        &self,
        args: &SetBucketQuotaArgs<'_>,
    ) -> Result<SetBucketQuotaResponse, Error> {
        let mut headers = Multimap::new();
        if let Some(v) = &args.extra_headers {
            merge(&mut headers, v);
        }
        let mut query_params = Multimap::new();
        query_params.insert("bucket".into(), args.bucket_name.into());

        let mut query_params = Multimap::new();
        query_params.insert("bucket".into(), args.bucket_name.into());

        let data = serde_json::to_string(&args.quota)?;

        let resp = self
            .client
            .execute(
                Method::PUT,
                &"us-east-1".into(),
                &mut headers,
                &query_params,
                "minio/admin/v3/set-bucket-quota".into(),
                None,
                Some(data.as_bytes()),
            )
            .await?;

        let headers = resp.headers().clone();

        Ok(SetBucketQuotaResponse {
            headers,
            bucket_name: args.bucket_name.into(),
        })
    }
}
