mod minio;

use futures::future::Future;
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

        del_req.join3(region_req, buc_exists_req).map(|_| ())
    }));
}
