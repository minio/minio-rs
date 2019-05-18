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
        c.get_bucket_location("txp")
            .map(|res| println!("{}", res))
            .map_err(|err| println!("{:?}", err))
    }));
}
