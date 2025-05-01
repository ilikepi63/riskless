use crate::batch_coordinator::CommitBatchResponse;

#[derive(Debug, Clone)]
pub struct ProduceResponse {
    pub request_id: u32,
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
