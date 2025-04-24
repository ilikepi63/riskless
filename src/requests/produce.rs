pub struct ProduceRequest {
    topic: String, 
    partition: u64,
    data: Vec<u8>
}