use std::collections::{HashMap, hash_map::Entry};

use crate::{coordinator::TopicIdPartition, error::RisklessResult};

#[derive(Clone)]
pub struct ProduceRequest {
    pub topic: String,
    pub partition: u64,
    pub data: Vec<u8>,
}

#[derive(Clone)] // TODO: This clone is definitely not required.
pub struct ProduceRequestCollection {
    inner: HashMap<TopicIdPartition, Vec<ProduceRequest>>,
    size: u64,
}

impl ProduceRequestCollection {
    pub fn new() -> Self {
        Self {
            inner: HashMap::new(),
            size: 0
        }
    }

    pub fn clear(&mut self) {
        self.inner.clear();
        self.size = 0;
    }

    pub fn collect(&mut self, req: ProduceRequest) -> RisklessResult<()> {
        let topic_id_partition = TopicIdPartition(req.topic.clone(), req.partition.clone());

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
}
