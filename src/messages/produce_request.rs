// use std::collections::{
//     HashMap,
//     hash_map::{Entry, Values},
// };

use std::sync::atomic::{AtomicU64, Ordering};

use dashmap::{DashMap, Entry, iter::IterMut};

use crate::{
    batch_coordinator::TopicIdPartition, error::RisklessResult, utils::request_response::Request,
};

use super::produce_response::ProduceResponse;

#[derive(Debug, Clone)]
pub struct ProduceRequest {
    pub request_id: u32,
    pub topic: String,
    pub partition: u64,
    pub data: Vec<u8>,
}

#[derive(Debug)]
pub struct ProduceRequestCollection {
    inner: DashMap<TopicIdPartition, Vec<ProduceRequest>>,
    response_senders: DashMap<u32, Request<ProduceRequest, ProduceResponse>>,
    size: AtomicU64,
}

impl Default for ProduceRequestCollection {
    fn default() -> Self {
        Self::new()
    }
}

impl ProduceRequestCollection {
    pub fn new() -> Self {
        Self {
            inner: DashMap::new(),
            response_senders: DashMap::new(),
            size: AtomicU64::new(0),
        }
    }

    /// This pulls out the response senders, replacing it with an empty HashMap.
    /// Perhaps this should rather be an option, considering that we might not want
    /// to allocate a new HashMap unnecessarily.
    pub fn extract_response_senders(
        &mut self,
    ) -> DashMap<u32, Request<ProduceRequest, ProduceResponse>> {
        let mut hmap = DashMap::new();

        std::mem::swap(&mut hmap, &mut self.response_senders);

        hmap
    }

    pub fn clear(&mut self) {
        self.inner.clear();
        self.size = AtomicU64::new(0);
    }

    pub fn collect(&self, req: Request<ProduceRequest, ProduceResponse>) -> RisklessResult<()> {
        tracing::info!("Collecting: {:#?}", req);

        let wrapped_request = req;
        let req = wrapped_request.inner();

        let topic_id_partition = TopicIdPartition(req.topic.clone(), req.partition);

        let entry = self.inner.entry(topic_id_partition);

        match entry {
            Entry::Occupied(mut occupied_entry) => {
                self.size
                    .fetch_add(TryInto::<u64>::try_into(req.data.len())?, Ordering::Relaxed);
                occupied_entry.get_mut().push(req.clone());
            }
            Entry::Vacant(vacant_entry) => {
                self.size
                    .fetch_add(TryInto::<u64>::try_into(req.data.len())?, Ordering::Relaxed);
                vacant_entry.insert(vec![req.clone()]);
            }
        }

        self.response_senders
            .insert(req.request_id, wrapped_request);

        Ok(())
    }

    pub fn size(&self) -> u64 {
        self.size.load(Ordering::Relaxed)
    }

    pub fn iter_partitions(&mut self) -> IterMut<'_, TopicIdPartition, Vec<ProduceRequest>> {
        self.inner.iter_mut()
    }
}
