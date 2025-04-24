pub struct ConsumeRequest {
    topic: String,
    partition: u64,
    offset: u64,
}
