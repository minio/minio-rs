use bytes::Bytes;
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
    RawSvcErr(hyper::StatusCode, Response<Body>),
}
