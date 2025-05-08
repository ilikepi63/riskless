use std::sync::atomic::{AtomicU64, Ordering};

use dashmap::{DashMap, Entry, iter::IterMut};

use crate::{batch_coordinator::TopicIdPartition, error::RisklessResult};

/// A Request representing the intention to produce a record on a topic/partition.
#[derive(Debug, Clone)]
pub struct ProduceRequest {
    /// Unique ID for this request.
    pub request_id: u32,
    /// The topic to which this data will be produced.
    pub topic: String,
    /// The partition on which this data will be produced.
    pub partition: u64,
    /// The data that will be added to the partition.
    pub data: Vec<u8>,
}

/// A collection of ProduceRequests.
///
///  This is primarily used to be converted into a SharedLogSegment.
#[derive(Debug)]
pub struct ProduceRequestCollection {
    /// A concurrent data structure for handling produce requests for each topic/partition combination.
    pub inner: DashMap<TopicIdPartition, Vec<ProduceRequest>>,
    /// The size in bytes for this collection.
    pub size: AtomicU64,
}

impl Default for ProduceRequestCollection {
    fn default() -> Self {
        Self::new()
    }
}

impl ProduceRequestCollection {
    /// Create a new intance of this struct.
    pub fn new() -> Self {
        Self {
            inner: DashMap::new(),
            size: AtomicU64::new(0),
        }
    }

    /// Clear this struct.
    pub fn clear(&mut self) {
        self.inner.clear();
        self.size = AtomicU64::new(0);
    }

    /// Creates a new collection, swaps it with this instance and returns the given collection.
    pub fn take(&mut self) -> Self {
        let mut other = ProduceRequestCollection::new();

        std::mem::swap(&mut *self, &mut other);

        other
    }

    /// Collect a produce request into this struct.
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

    /// Get the size in bytes for this collection.
    pub fn size(&self) -> u64 {
        self.size.load(Ordering::Relaxed)
    }

    /// Iterate over the partitions of this structure.
    pub fn iter_partitions(&mut self) -> IterMut<'_, TopicIdPartition, Vec<ProduceRequest>> {
        self.inner.iter_mut()
    }
}
