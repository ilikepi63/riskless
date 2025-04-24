use std::sync::Arc;

use object_store::ObjectStore;

use crate::coordinator::{BatchCoordinator, DefaultBatchCoordinator};

pub struct Client {
    config: ClientConfiguration,
}

pub struct ClientConfiguration {
    object_store: Arc<dyn ObjectStore>,
    batch_coordinator: Arc<dyn BatchCoordinator>,
}

impl Default for ClientConfiguration {
    fn default() -> Self {
        Self {
            object_store: Arc::new(object_store::local::LocalFileSystem::new()),
            batch_coordinator: Arc::new(DefaultBatchCoordinator::new()),
        }
    }
}

impl Client {
    pub fn new(config: ClientConfiguration) -> Self {
        Self { config }
    }

    pub fn produce(&self) {}

    pub fn consume(&self) {}
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_works() {}
}
