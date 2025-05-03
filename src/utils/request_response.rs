//! Request/Response - ideally just to make code more readable.
//! This is pretty much just a thin wrapper around tokio's oneshot channel
//! with some added coupling for the data that is part of the 'request'.
use crate::error::RisklessResult;
use std::fmt::Debug;

/// Request wraps a generic type as well as a sender for the type.
#[derive(Debug)]
pub struct Request<REQ: Sync + Send + Debug, RES: Sync + Send + Debug>(
    REQ,
    tokio::sync::oneshot::Sender<RES>,
);

impl<REQ: Sync + Send + Debug, RES: Sync + Send + Debug> Request<REQ, RES> {
    /// Creates a new Request from the given generic data type.
    pub fn new(req: REQ) -> (Self, Response<RES>) {
        let (tx, rx) = tokio::sync::oneshot::channel();

        (Self(req, tx), Response::new(rx))
    }

    pub fn inner(&self) -> &REQ {
        &self.0
    }

    /// Respond with the response value.
    pub fn respond(self, val: RES) -> Result<(), RES> {
        self.1.send(val)
    }
}

/// Response wraps the receiver for this value.
pub struct Response<RES>(tokio::sync::oneshot::Receiver<RES>);

impl<RES> Response<RES> {
    /// Creates a new Response given the receiver for this value.
    pub fn new(rx: tokio::sync::oneshot::Receiver<RES>) -> Self {
        Self(rx)
    }

    /// Await this response value.
    pub async fn recv(self) -> RisklessResult<RES> {
        Ok(self.0.await?)
    }
}
