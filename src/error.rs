//! Errors.
use std::{array::TryFromSliceError, num::TryFromIntError};

use thiserror::Error;

/// Wrapper around a Result type that uses the RisklessError.
pub type RisklessResult<T> = Result<T, RisklessError>;

/// Riskless Error Library.
#[derive(Error, Debug)]
pub enum RisklessError {
    /// Generic Error for arbitrary errors that are generally not classified but should still convey information.
    #[error("{0}")]
    Generic(String),
    /// Unknown Error emitted when the outcome made an error that is generally unknown.
    /// Important to note that use of this error is discouraged and will be removed in the future. 
    #[error("Unknown Riskless Error")]
    Unknown,

    // Inferred
    /// Error originating from Tokio MPSC Sender Error.
    #[error("Sender Error for Tokio MPSC {0}")]
    TokioMpscSenderError(String),
    /// Error originating from object_store crate.
    #[error("ObjectStore Error")]
    ObjectStoreError(#[from] object_store::Error),
    /// Error emitted when a failure to convert from an integer has occurred.
    #[error("TryFromInt Conversion Error")]
    TryFromIntConversionError(#[from] TryFromIntError),
    /// Error emitted when a failure to convert from a slice has occurred.
    #[error("TryFromSlice Conversion Error")]
    TryFromSliceConversionrror(#[from] TryFromSliceError),
    /// Error originating from a std lib IO Error.
    #[error("IO Error")]
    IoError(#[from] std::io::Error),
    /// Error originating from a failure to receive a message from a tokio oneshot channel.
    #[error("Tokio Oneshot Channel Receive Error")]
    TokioOneshotChannelRecvError(#[from] tokio::sync::oneshot::error::RecvError),
    /// Error originating from the uuid crate.
    #[error("Uuid Error")]
    UuidError(#[from] uuid::Error),

    // SharedLogSegment Errors.
    /// This error is emitted when trying to parse a SharedLogSegment file and the underlying file does not 
    /// have the expected magic number. 
    #[error("Invalid Magic Number: {0}")]
    InvalidMagicNumberError(u32),
    #[error("Unable to Parse Header: {0}")]
    /// Error emitted when a failure to parse the header of a SharedLogSegment File has occurred.
    UnableToPassHeaderError(String),
    /// Error emitted when the version number does not correlate with an expected version number.
    #[error("Invalid Version Number: {0}")]
    InvalidSharedLogSegmentVersionNumber(u32),
}

impl<T> From<tokio::sync::mpsc::error::SendError<T>> for RisklessError {
    fn from(value: tokio::sync::mpsc::error::SendError<T>) -> Self {
        RisklessError::TokioMpscSenderError(value.to_string())
    }
}
