# Riskless

An implementation of KIP-1150 - Diskless Topics as a reuseable library for general implementation of distributed logs on object storage. 

### Motivation

Considering separation between compute and storage for distributed logs is becoming more important than ever, primarily because of the costs related to traditional implementations, this library was written with the intention to be built upon. 

When most organisations set out to build a distributed log, it is built with the intention to be fairly "monolithic" in nature. For example, most distributed log implementations come with a specific network protocol, consensus protocol, storage implementation and are generally quite opinionated with most of their implementations. 

Riskless is hopefully the first in a number of libraries that try to make distributed logs composable, similar to what the Apache Arrow/Datafusion projects are doing for traditional databases.

## Usage

### Installation

To use `riskless`, first add this to your `Cargo.toml`:

```toml
[dependencies]
riskless = "x.x"
```

### How to use

```rust
let mut batch_coordinator = ...; // Custom implementation of BatchCoordinator.
let mut object_store = ...; // Create an object store.

let config = BrokerConfiguration {
    object_store,
    batch_coordinator,
    segment_size_in_bytes: 50_000,
    flush_interval_in_ms: 500,
};

// Create the broker.
let mut broker = Broker::new(config);

// Produce to an aribitrary topic. 
let produce_result = broker
    .produce(ProduceRequest {
        topic: "example-topic".to_string(),
        partition: 1,
        data: "hello".as_bytes().to_vec(),
    })
    .await?;

let receiver = broker
    .consume(ConsumeRequest {
        topic: "example-topic".to_string(),
        partition: 1,
        offset: 0,
        max_partition_fetch_bytes: 50_000,
    });

let response = receiver.recv().await;    
```

#### Implementation of BatchCoordinator

The BatchCoordinator is a trait based on the writings from KIP-1164. This is primarily for developers to implement their own BatchCoordinator based off of their own needs. 

There is a SimpleBatchCoordinator implementation that ships with Riskless that is: 

- Single Node
- Based (to a very small extent) on Kafka's index file implementation.
- Very simplistic in nature
- an example for developers to build their own Batch Coordinator implementation off of. 

```rust 
let mut batch_coordinator = SimpleBatchhCoordinator::new("current_dir");
let config = BrokerConfiguration {
    object_store,
    batch_coordinator
};
```

## License

This project is licensed under both an Apache 2.0 license and an MIT license.

### Contribution

Unless you explicitly state otherwise, any contribution intentionally submitted
for inclusion in `riskless` by you, shall be licensed under Apache 2.0 and MIT, without any additional
terms or conditions.