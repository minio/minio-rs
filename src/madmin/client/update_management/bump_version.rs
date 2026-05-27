use crate::madmin::builders::BumpVersion;
use crate::madmin::madmin_client::MadminClient;

impl MadminClient {
    pub fn bump_version(&self) -> BumpVersion {
        BumpVersion::builder().client(self.clone()).build()
    }
}
