use crate::batch_coordinator::CommitBatchResponse;

/// A response representing the outcome of a ProduceRequest.
#[derive(Debug, Clone)]
pub struct ProduceResponse {
    /// The request's unique ID.
    pub request_id: u32,
    /// The batches retrieved for this produce request.
    pub batch: CommittedBatch,
    /// The error responses from the request.
    pub errors: Vec<String>, // TODO: some way of making this a struct?
}

/// The Committed Batch for this Produce Response.
#[derive(Debug, Clone)]
pub struct CommittedBatch {
    /// The given topic for the committed batch.
    pub topic: Vec<u8>,
    /// The given partition for batch.
    pub partition: Vec<u8>,
    /// The offset for this record.
    pub offset: u64,
    /// The size retrieved for this record.
    pub size: u32,
}

// TODO: This implementation.
impl From<&CommitBatchResponse> for ProduceResponse {
    fn from(value: &CommitBatchResponse) -> Self {
        ProduceResponse {
            request_id: value.request.request_id,
            errors: value.errors.clone(),
            batch: CommittedBatch {
                topic: value.request.topic_id_partition.0.as_bytes().to_vec(),
                partition: value.request.topic_id_partition.1.clone(),
                offset: value.request.base_offset,
                size: value.request.size,
            },
        }
    }
}
