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
use futures::{future::Future, stream::Stream};
use hyper::rt;

use minio::BucketInfo;

mod minio;

fn get_local_default_server() -> minio::Client {
    match minio::Client::new("http://localhost:9000") {
        Ok(mut c) => {
            c.set_credentials(minio::Credentials::new("minio", "minio123"));
            c
        }
        Err(_) => panic!("could not make local client"),
    }
}

fn main() {
    rt::run(rt::lazy(|| {
        // let c = get_local_default_server();
        let c = minio::Client::get_play_client();
        let bucket = "000";

        let region_req = c
            .get_bucket_location(bucket)
            .map(|res| println!("{}", res.to_string()))
            .map_err(|err| println!("{:?}", err));

        let del_req = c
            .delete_bucket(bucket)
            .map(|_| println!("Deleted!"))
            .map_err(|err| println!("del err: {:?}", err));

        let buc_exists_req = c
            .bucket_exists(bucket)
            .map(move |e| println!("Bucket {} exists: {}", bucket, e))
            .map_err(|err| println!("exists err: {:?}", err));

        let make_bucket_req = c
            .make_bucket(bucket)
            .map(move |_| println!("Bucket {} created", bucket))
            .map_err(move |err| println!("Bucket create for {} failed with {:?}", bucket, err));

        let download_req = c
            .get_object_req(bucket, "issue", vec![])
            .and_then(|g| {
                println!("issue: {} {} {:?}", g.object_size, g.etag, g.content_type);
                g.get_object_stream().concat2()
            })
            .map(|c| println!("get obj res: {:?}", c))
            .map_err(|c| println!("err res: {:?}", c));

        let upload_req = c
            .put_object_req(bucket, "issue", vec![], "object content".as_bytes().to_vec())
            .and_then(|g| {
                println!("issue: {} {} {:?}", g.object_size, g.etag, g.content_type);
                g.get_object_stream().concat2()
            })
            .map(|c| println!("get obj res: {:?}", c))
            .map_err(|c| println!("err res: {:?}", c));

        let list_buckets_req = c
            .list_buckets()
            .map(|buckets| {
                println!(
                    "{:?}",
                    buckets
                        .iter()
                        .map(|bucket: &BucketInfo| bucket.name.clone())
                        .collect::<Vec<String>>()
                )
            })
            .map_err(|err| println!("{:?}", err));

        let list_objects_req = c
            .list_objects(bucket, None, None, None, None)
            .map(|l_obj_resp| println!("{:?} {:?}", l_obj_resp, l_obj_resp.object_infos.len()))
            .map_err(|err| println!("{:?}", err));

        del_req
            .join5(make_bucket_req, region_req, buc_exists_req, download_req)
            .map(|_| ())
            .and_then(|_| list_buckets_req)
            .then(|_| list_objects_req)
    }));
}
