use bytes::Bytes;

#[derive(Debug, Clone)]
pub struct ConsumeBatch {
    pub topic: String,
    pub partition: u64,
    pub offset: u64,
    pub max_partition_fetch_bytes: u32,
    pub data: Bytes,
}

#[derive(Debug, Clone)]
pub struct ConsumeResponse {
    pub batches: Vec<ConsumeBatch>,
}
