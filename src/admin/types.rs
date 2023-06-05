pub use byte_unit::Byte;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum QuotaType {
    Soft,
    Hard,
}

#[derive(Default, Clone, Debug, Deserialize, Serialize)]
pub struct Quota {
    pub quota: byte_unit::Byte,
    pub quotatype: Option<QuotaType>,
}
