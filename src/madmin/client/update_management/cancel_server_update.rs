use crate::madmin::builders::CancelServerUpdate;
use crate::madmin::madmin_client::MadminClient;

impl MadminClient {
    pub fn cancel_server_update(&self) -> CancelServerUpdate {
        CancelServerUpdate::builder().client(self.clone()).build()
    }
}
