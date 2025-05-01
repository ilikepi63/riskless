use std::{array::TryFromSliceError, num::TryFromIntError};

use thiserror::Error;

pub type RisklessResult<T> = Result<T, RisklessError>;

#[derive(Error, Debug)]
pub enum RisklessError {
    #[error("unknown data store error")]
    Unknown,

    // Inferred
    #[error("Sender Error for Tokio MPSC {0}")]
    TokioMpscSenderError(String),
    #[error("ObjectStore Error")]
    ObjectStoreError(#[from] object_store::Error),
    #[error("TryFromInt Conversion Error")]
    TryFromIntConversionError(#[from] TryFromIntError),
    #[error("TryFromSlice Conversion Error")]
    TryFromSliceConversionrror(#[from] TryFromSliceError),
    #[error("IO Error")]
    IoError(#[from] std::io::Error),
    #[error("Tokio Oneshot Channel Receive Error")]
    TokioOneshotChannelRecvError(#[from] tokio::sync::oneshot::error::RecvError),
    #[error("Uuid Error")]
    UuidError(#[from] uuid::Error),
}

impl<T> From<tokio::sync::mpsc::error::SendError<T>> for RisklessError {
    fn from(value: tokio::sync::mpsc::error::SendError<T>) -> Self {
        RisklessError::TokioMpscSenderError(value.to_string())
    }
}
