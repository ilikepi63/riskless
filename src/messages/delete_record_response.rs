
use crate::error::RisklessError;

#[derive(Debug)]
pub struct DeleteRecordsResponse {
    pub errors: Vec<String>,
}

impl TryFrom<crate::batch_coordinator::DeleteRecordsResponse> for DeleteRecordsResponse {
    type Error = RisklessError;

    fn try_from(
        value: crate::batch_coordinator::DeleteRecordsResponse,
    ) -> Result<Self, Self::Error> {
        Ok(Self {
            errors: value.errors,
        })
    }
}
