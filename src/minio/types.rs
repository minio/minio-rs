use bytes::Bytes;
use futures::stream::Stream;
use hyper::header::{
    HeaderMap, HeaderValue, CACHE_CONTROL, CONTENT_DISPOSITION, CONTENT_ENCODING, CONTENT_LANGUAGE,
    CONTENT_LENGTH, CONTENT_TYPE, ETAG, EXPIRES,
};
use hyper::{body::Body, Response};
use roxmltree;
use std::string;

pub struct Region(String);

impl Region {
    pub fn new(s: &str) -> Region {
        Region(s.to_string())
    }

    pub fn empty() -> Region {
        Region::new("")
    }

    pub fn to_string(&self) -> String {
        self.0.clone()
    }
}

#[derive(Debug)]
pub enum Err {
    InvalidUrl(String),
    InvalidEnv(String),
    HttpErr(http::Error),
    HyperErr(hyper::Error),
    FailStatusCodeErr(hyper::StatusCode, Bytes),
    Utf8DecodingErr(string::FromUtf8Error),
    XmlParseErr(roxmltree::Error),
    MissingRequiredParams,
    RawSvcErr(hyper::StatusCode, Response<Body>),
}

pub struct GetObjectResp {
    pub user_metadata: Vec<(String, String)>,
    pub object_size: u64,
    pub etag: String,

    // standard headers
    pub content_type: Option<String>,
    pub content_language: Option<String>,
    pub expires: Option<String>,
    pub cache_control: Option<String>,
    pub content_disposition: Option<String>,
    pub content_encoding: Option<String>,

    resp: Response<Body>,
}

impl GetObjectResp {
    pub fn new(r: Response<Body>) -> Result<GetObjectResp, Err> {
        let h = r.headers();

        let cl_opt = hv2s(h.get(CONTENT_LENGTH)).and_then(|l| l.parse::<u64>().ok());
        let etag_opt = hv2s(h.get(ETAG));
        match (cl_opt, etag_opt) {
            (Some(cl), Some(etag)) => Ok(GetObjectResp {
                user_metadata: extract_user_meta(h),
                object_size: cl,
                etag: etag,

                content_type: hv2s(h.get(CONTENT_TYPE)),
                content_language: hv2s(h.get(CONTENT_LANGUAGE)),
                expires: hv2s(h.get(EXPIRES)),
                cache_control: hv2s(h.get(CACHE_CONTROL)),
                content_disposition: hv2s(h.get(CONTENT_DISPOSITION)),
                content_encoding: hv2s(h.get(CONTENT_ENCODING)),

                resp: r,
            }),
            _ => Err(Err::MissingRequiredParams),
        }
    }

    // Consumes GetObjectResp
    pub fn get_object_stream(self) -> impl Stream<Item = hyper::Chunk, Error = Err> {
        self.resp.into_body().map_err(|err| Err::HyperErr(err))
    }
}

fn hv2s(o: Option<&HeaderValue>) -> Option<String> {
    o.and_then(|v| v.to_str().ok()).map(|x| x.to_string())
}

fn extract_user_meta(h: &HeaderMap) -> Vec<(String, String)> {
    h.iter()
        .map(|(k, v)| (k.as_str(), v.to_str()))
        .filter(|(k, v)| k.to_lowercase().starts_with("x-amz-meta-") && v.is_ok())
        .map(|(k, v)| (k.to_string(), v.unwrap_or("").to_string()))
        .collect()
}
