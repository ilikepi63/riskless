// use std::collections::{
//     HashMap,
//     hash_map::{Entry, Values},
// };

use std::sync::atomic::{AtomicU64, Ordering};

use dashmap::{DashMap, Entry, iter::IterMut};

use crate::{
    batch_coordinator::TopicIdPartition, error::RisklessResult,
};

#[derive(Debug, Clone)]
pub struct ProduceRequest {
    pub request_id: u32,
    pub topic: String,
    pub partition: u64,
    pub data: Vec<u8>,
}

#[derive(Debug)]
pub struct ProduceRequestCollection {
    pub inner: DashMap<TopicIdPartition, Vec<ProduceRequest>>,
    pub size: AtomicU64,
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
            size: AtomicU64::new(0),
        }
    }

    pub fn clear(&mut self) {
        self.inner.clear();
        self.size = AtomicU64::new(0);
    }

    pub fn collect(&self, req: ProduceRequest) -> RisklessResult<()> {
        tracing::info!("Collecting: {:#?}", req);

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

        Ok(())
    }

    pub fn size(&self) -> u64 {
        self.size.load(Ordering::Relaxed)
    }

    pub fn iter_partitions(&mut self) -> IterMut<'_, TopicIdPartition, Vec<ProduceRequest>> {
        self.inner.iter_mut()
    }
}
