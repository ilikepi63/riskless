use crate::error::RisklessError;

/// A response representing the outcome of a delete record request.
#[derive(Debug)]
pub struct DeleteRecordsResponse {
    /// The errors emitted from the request.
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
