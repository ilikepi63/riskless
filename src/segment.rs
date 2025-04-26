use bytes::Bytes;

use crate::{error::RisklessError, messages::produce_request::{ProduceRequest, ProduceRequestCollection}};

pub struct SharedLogSegment {}

impl TryFrom<ProduceRequestCollection> for SharedLogSegment {
    type Error = RisklessError;

    fn try_from(value: ProduceRequestCollection) -> Result<Self, Self::Error> {
        // TODO

        Ok(SharedLogSegment {})
    }
}

impl Into<bytes::Bytes> for SharedLogSegment {
    fn into(self) -> bytes::Bytes {
        // TODO
        Bytes::new()
    }
}
