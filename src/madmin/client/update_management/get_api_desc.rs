use crate::madmin::builders::GetAPIDesc;
use crate::madmin::madmin_client::MadminClient;

impl MadminClient {
    pub fn get_api_desc(&self) -> GetAPIDesc {
        GetAPIDesc::builder().client(self.clone()).build()
    }
}
