use super::produce_request::ProduceRequest;

#[derive(Debug, Clone)]
pub struct BatchCoordinate {
    pub topic: String,
    pub partition: u64,
    pub base_offset: u64,
    pub offset: u64,
    pub size: u32,
    pub request: ProduceRequest,
}
