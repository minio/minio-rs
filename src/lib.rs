/*
 * MinIO Rust Library for Amazon S3 Compatible Cloud Storage
 * Copyright 2019 MinIO, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

pub mod minio;

#[cfg(test)]
mod tests {
    use futures::{future::Future, stream::Stream};
    use hyper::rt;
    use log::debug;


    use super::*;

    fn get_local_default_server() -> minio::Client {
        match minio::Client::new("http://localhost:9000") {
            Ok(mut c) => {
                c.set_credentials(minio::Credentials::new("minio", "minio123"));
                c
            }
            Err(_) => panic!("could not make local client"),
        }
    }

    #[test]
    fn test_lib_functions() {
        println!("test func");
        rt::run(rt::lazy(|| {
            let c = minio::Client::get_play_client();
            let bucket_name = "aaaa";

            c.put_object_req(bucket_name, "hhhhhhhhhh", vec![], "object content".as_bytes().to_vec())
                .and_then(|g| {
                    print!("object: {} {} {:?}", g.object_size, g.etag, g.content_type);
                    g.get_object_stream().concat2()
                })
                .map(|c| {
                    println!("{:?}", c);
                })
                .map_err(|c| {
                    println!("{:?}", c);
                })
                .map(|_| {})
        }));

        rt::run(rt::lazy(|| {
            let c = minio::Client::get_play_client();
            let bucket = "aaaa";

            c.get_object_req(bucket, "hhhhhhhhhh", vec![])
                .and_then(|g| {
                    debug!("object: {} {} {:?}", g.object_size, g.etag, g.content_type);
                    g.get_object_stream().concat2()
                })
                .map(|c| debug!("get obj res: {:?}", c))
                .map_err(|c| debug!("err res: {:?}", c))
                .map(|_| {})
        }));

        rt::run(rt::lazy(|| {
            let c = minio::Client::get_play_client();
            let bucket = "aaaa";

            c.delete_bucket(bucket)
                .map(|_| debug!("Deleted!"))
                .map_err(|err| debug!("del err: {:?}", err))
                .map(|_| {})
        }));
    }
}
