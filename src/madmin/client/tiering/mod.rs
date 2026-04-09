use crate::madmin::builders::tiering::*;
use crate::madmin::madmin_client::MadminClient;
use crate::madmin::types::TierName;
use crate::madmin::types::tier::{TierConfig, TierCreds};
use crate::s3::error::ValidationErr;

impl MadminClient {
    pub fn add_tier(&self, config: TierConfig) -> AddTier {
        AddTier::builder()
            .client(self.clone())
            .config(config)
            .build()
    }

    pub fn list_tiers(&self) -> ListTiers {
        ListTiers::builder().client(self.clone()).build()
    }

    pub fn edit_tier<T>(
        &self,
        tier_name: T,
        creds: TierCreds,
    ) -> Result<EditTierBldr, ValidationErr>
    where
        T: TryInto<TierName>,
        T::Error: Into<ValidationErr>,
    {
        Ok(EditTier::builder()
            .client(self.clone())
            .tier_name(tier_name.try_into().map_err(Into::into)?)
            .creds(creds))
    }

    pub fn remove_tier<T>(&self, tier_name: T) -> Result<RemoveTierBldr, ValidationErr>
    where
        T: TryInto<TierName>,
        T::Error: Into<ValidationErr>,
    {
        Ok(RemoveTier::builder()
            .client(self.clone())
            .tier_name(tier_name.try_into().map_err(Into::into)?))
    }

    pub fn verify_tier<T>(&self, tier_name: T) -> Result<VerifyTierBldr, ValidationErr>
    where
        T: TryInto<TierName>,
        T::Error: Into<ValidationErr>,
    {
        Ok(VerifyTier::builder()
            .client(self.clone())
            .tier_name(tier_name.try_into().map_err(Into::into)?))
    }

    pub fn tier_stats(&self) -> TierStats {
        TierStats::builder().client(self.clone()).build()
    }
}
