use crate::madmin::builders::ServerUpdate;
use crate::madmin::madmin_client::MadminClient;

impl MadminClient {
    pub fn server_update(&self) -> ServerUpdate {
        ServerUpdate::builder().client(self.clone()).build()
    }
}
