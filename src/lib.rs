// #![deny(missing_docs)]
#![deny(clippy::print_stdout)]
#![deny(clippy::print_stderr)]

pub mod batch_coordinator;
mod broker;
pub mod messages;
mod segment;
pub mod simple_batch_coordinator;

pub use broker::{Broker, BrokerConfiguration};
pub mod error;
