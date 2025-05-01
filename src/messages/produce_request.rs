use std::collections::{
    HashMap,
    hash_map::{Entry, Values},
};

use crate::{batch_coordinator::TopicIdPartition, error::RisklessResult};

use super::produce_response::ProduceResponse;

#[derive(Debug, Clone)]
pub struct ProduceRequest {
    pub request_id: u32,
    pub topic: String,
    pub partition: u64,
    pub data: Vec<u8>,
}

pub struct ProduceRequestCollection {
    inner: HashMap<TopicIdPartition, Vec<ProduceRequest>>,
    response_senders: HashMap<u32, tokio::sync::oneshot::Sender<ProduceResponse>>,
    size: u64,
}

impl Default for ProduceRequestCollection {
    fn default() -> Self {
        Self::new()
    }
}

impl ProduceRequestCollection {
    pub fn new() -> Self {
        Self {
            inner: HashMap::new(),
            response_senders: HashMap::new(),
            size: 0,
        }
    }

    /// This pulls out the response senders, replacing it with an empty HashMap.
    /// Perhaps this should rather be an option, considering that we might not want
    /// to allocate a new HashMap unnecessarily.
    pub fn extract_response_senders(
        &mut self,
    ) -> HashMap<u32, tokio::sync::oneshot::Sender<ProduceResponse>> {
        let mut hmap = HashMap::new();

        std::mem::swap(&mut hmap, &mut self.response_senders);

        hmap
    }

    pub fn clear(&mut self) {
        self.inner.clear();
        self.size = 0;
    }

    pub fn collect(
        &mut self,
        (req, sender): (
            ProduceRequest,
            tokio::sync::oneshot::Sender<ProduceResponse>,
        ),
    ) -> RisklessResult<()> {

        tracing::info!("Collecting: {} {:#?}", req.request_id, sender);

        self.response_senders.insert(req.request_id, sender);

        let topic_id_partition = TopicIdPartition(req.topic.clone(), req.partition);

        let entry = self.inner.entry(topic_id_partition);

        match entry {
            Entry::Occupied(mut occupied_entry) => {
                self.size += TryInto::<u64>::try_into(req.data.len())?;
                occupied_entry.get_mut().push(req);
            }
            Entry::Vacant(vacant_entry) => {
                self.size += TryInto::<u64>::try_into(req.data.len())?;
                vacant_entry.insert(vec![req]);
            }
        }

        Ok(())
    }

    pub fn size(&self) -> u64 {
        self.size
    }

    pub fn iter_partitions(&mut self) -> Values<'_, TopicIdPartition, Vec<ProduceRequest>> {
        self.inner.values()
    }
}
