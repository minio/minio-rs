mod minio;

use futures::{future::Future, stream::Stream};
use hyper::rt;

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
        let bucket = "yyy";

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

        let download_req = c
            .get_object_req(bucket, "issue", vec![])
            .and_then(|g| {
                println!("issue: {} {} {:?}", g.object_size, g.etag, g.content_type);
                g.get_object_stream().concat2()
            })
            .map(|c| println!("get obj res: {:?}", c))
            .map_err(|c| println!("err res: {:?}", c));

        del_req
            .join4(region_req, buc_exists_req, download_req)
            .map(|_| ())
    }));
}
