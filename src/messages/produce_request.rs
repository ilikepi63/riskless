#[derive(Clone)]
pub struct ProduceRequest {
    pub topic: String,
    pub partition: u64,
    pub data: Vec<u8>,
}
