mod api;
mod sign;

use bytes::Bytes;
use futures::future::{self, Future};
use futures::stream::Stream;
use http;
use hyper::header::{HeaderName, HeaderValue};
use hyper::{body::Body, client, header, header::HeaderMap, Method, Uri};
use hyper_tls::HttpsConnector;
use std::collections::HashMap;
use std::env;
use std::sync::Arc;
use std::{string, string::String};
use time;
use time::Tm;

#[derive(Debug, Clone)]
pub struct Credentials {
    access_key: String,
    secret_key: String,
}

impl Credentials {
    pub fn new(ak: &str, sk: &str) -> Credentials {
        Credentials {
            access_key: ak.to_string(),
            secret_key: sk.to_string(),
        }
    }

    pub fn from_env() -> Result<Credentials, Err> {
        let (ak, sk) = (env::var("MINIO_ACCESS_KEY"), env::var("MINIO_SECRET_KEY"));
        match (ak, sk) {
            (Ok(ak), Ok(sk)) => Ok(Credentials::new(ak.as_str(), sk.as_str())),
            _ => Err(Err::InvalidEnv(
                "Missing MINIO_ACCESS_KEY or MINIO_SECRET_KEY environment variables".to_string(),
            )),
        }
    }
}

pub type Region = String;

#[derive(Debug)]
pub enum Err {
    InvalidUrl(String),
    InvalidEnv(String),
    HttpErr(http::Error),
    HyperErr(hyper::Error),
    FailStatusCodeErr(hyper::StatusCode, Bytes),
    Utf8DecodingErr(string::FromUtf8Error),
}

trait ApiClient {
    fn make_req(&self, req: http::Request<Body>) -> client::ResponseFuture;
}

struct HttpApiClient {
    c: client::Client<client::HttpConnector, Body>,
}

impl ApiClient for HttpApiClient {
    fn make_req(&self, req: http::Request<Body>) -> client::ResponseFuture {
        self.c.request(req)
    }
}

struct HttpsApiClient {
    c: client::Client<HttpsConnector<client::HttpConnector>, Body>,
}

impl ApiClient for HttpsApiClient {
    fn make_req(&self, req: http::Request<Body>) -> client::ResponseFuture {
        self.c.request(req)
    }
}

pub struct Client {
    server: Uri,
    region: Region,
    conn_client: Arc<Box<dyn ApiClient + Send + Sync>>,
    pub credentials: Option<Credentials>,
}

impl Client {
    pub fn new(server: &str) -> Result<Client, Err> {
        let v = server.parse::<Uri>();
        match v {
            Ok(s) => {
                if s.host().is_none() {
                    Err(Err::InvalidUrl("no host specified!".to_string()))
                } else if s.scheme_str() != Some("http") && s.scheme_str() != Some("https") {
                    Err(Err::InvalidUrl("invalid scheme!".to_string()))
                } else {
                    Ok(Client {
                        server: s.clone(),
                        region: String::from(""),
                        conn_client: if s.scheme_str() == Some("http") {
                            Arc::new(Box::new(HttpApiClient {
                                c: client::Client::new(),
                            }))
                        } else {
                            let https = HttpsConnector::new(4).unwrap();
                            Arc::new(Box::new(HttpsApiClient {
                                c: client::Client::builder().build::<_, hyper::Body>(https),
                            }))
                        },
                        credentials: None,
                    })
                }
            }
            Err(err) => Err(Err::InvalidUrl(err.to_string())),
        }
    }

    pub fn set_credentials(&mut self, credentials: Credentials) {
        self.credentials = Some(credentials);
    }

    pub fn set_region(&mut self, r: Region) {
        self.region = r;
    }

    fn add_host_header(&self, h: &mut HeaderMap) {
        let host_val = match self.server.port_part() {
            Some(port) => format!("{}:{}", self.server.host().unwrap_or(""), port),
            None => self.server.host().unwrap_or("").to_string(),
        };
        match header::HeaderValue::from_str(&host_val) {
            Ok(v) => {
                h.insert(header::HOST, v);
            }
            _ => {}
        }
    }

    pub fn get_play_client() -> Client {
        Client {
            server: "https://play.min.io:9000".parse::<Uri>().unwrap(),
            region: String::from(""),
            conn_client: {
                let https = HttpsConnector::new(4).unwrap();
                Arc::new(Box::new(HttpsApiClient {
                    c: client::Client::builder().build::<_, hyper::Body>(https),
                }))
            },
            credentials: Some(Credentials::new(
                "Q3AM3UQ867SPQQA43P2F",
                "zuf+tfteSlswRu7BJ86wekitnifILbZam1KYY3TG",
            )),
        }
    }

    pub fn get_bucket_location(&self, b: &str) -> impl Future<Item = String, Error = Err> {
        let mut qp = HashMap::new();
        qp.insert("location".to_string(), None);
        let mut hmap = HeaderMap::new();

        self.add_host_header(&mut hmap);
        let body_hash_hdr = (
            HeaderName::from_static("x-amz-content-sha256"),
            HeaderValue::from_static("UNSIGNED-PAYLOAD"),
        );
        hmap.insert(body_hash_hdr.0.clone(), body_hash_hdr.1.clone());
        let s3_req = S3Req {
            method: Method::GET,
            bucket: Some(b.to_string()),
            object: None,
            headers: hmap,
            query: qp,
            body: Body::empty(),
            ts: time::now_utc(),
        };

        let sign_hdrs = sign::sign_v4(&s3_req, &self);
        println!("signout: {:?}", sign_hdrs);
        let req_result = api::mk_request(&s3_req, &self, &sign_hdrs);
        let conn_client = Arc::clone(&self.conn_client);
        future::result(req_result)
            .map_err(|e| Err::HttpErr(e))
            .and_then(move |req| {
                conn_client
                    .make_req(req)
                    .map_err(|e| Err::HyperErr(e))
                    .and_then(|resp| {
                        let st = resp.status();
                        resp.into_body()
                            .concat2()
                            .map_err(|err| Err::HyperErr(err))
                            .map(move |chunk| (st, chunk.into_bytes()))
                    })
                    .and_then(|(code, body)| {
                        if code.is_success() {
                            b2s(body)
                        } else {
                            Err(Err::FailStatusCodeErr(code, body))
                        }
                    })
            })
    }
}

fn b2s(b: Bytes) -> Result<String, Err> {
    match String::from_utf8(b.iter().map(|x| x.clone()).collect::<Vec<u8>>()) {
        Err(e) => Err(Err::Utf8DecodingErr(e)),
        Ok(s) => Ok(s),
    }
}

pub struct S3Req {
    method: Method,
    bucket: Option<String>,
    object: Option<String>,
    headers: HeaderMap,
    query: HashMap<String, Option<String>>,
    body: Body,
    ts: Tm,
}

impl S3Req {
    fn mk_path(&self) -> String {
        let mut res: String = String::from("/");
        if let Some(s) = &self.bucket {
            res.push_str(&s);
            res.push_str("/");
            if let Some(o) = &self.object {
                res.push_str(&o);
            }
        };
        res
    }

    fn mk_query(&self) -> String {
        self.query
            .iter()
            .map(|(x, y)| match y {
                Some(v) => format!("{}={}", x, v),
                None => x.to_string(),
            })
            .collect::<Vec<String>>()
            .join("&")
    }
}
