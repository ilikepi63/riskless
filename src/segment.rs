use bytes::Bytes;

use crate::{error::RisklessError, messages::produce_request::ProduceRequest};

pub struct SharedLogSegment {}

impl TryFrom<&[ProduceRequest]> for SharedLogSegment {
    type Error = RisklessError;

    fn try_from(value: &[ProduceRequest]) -> Result<Self, Self::Error> {
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
