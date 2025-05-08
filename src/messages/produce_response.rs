
use crate::batch_coordinator::CommitBatchResponse;

/// A response representing the outcome of a ProduceRequest.
#[derive(Debug, Clone)]
pub struct ProduceResponse {
    /// The request's unique ID.
    pub request_id: u32,
    /// The error responses from the request.
    pub errors: Vec<String>, // TODO: some way of making this a struct?
}

// TODO: This implementation.
impl From<&CommitBatchResponse> for ProduceResponse {
    fn from(value: &CommitBatchResponse) -> Self {
        ProduceResponse {
            request_id: value.request.request_id,
            errors: value.errors.clone(),
        }
    }
}
