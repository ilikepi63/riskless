
#[derive(Debug, Clone)]
pub struct BatchCoordinate{
    pub topic: String, 
    pub partition: u64, 
    pub base_offset: u64, 
    pub offset: u64
}