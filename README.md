# Jocko

![CI](https://github.com/bodaay/jocko/actions/workflows/ci.yml/badge.svg)

Kafka/distributed commit log service in Go.

## Goals

- Implement Kafka in Go
- Protocol compatible with Kafka so Kafka clients and services work with Jocko
- Make operating simpler
- Distribute a single binary
- Use Serf for discovery, Raft for consensus (no ZooKeeper dependency)
- Smarter configuration settings
    - Able to use percentages of disk space for retention policies rather than only bytes and time kept
    - Handling size configs when you change the number of partitions or add topics

## Status

- [x] Producing
- [x] Fetching
- [x] Partition consensus and distribution
- [x] Discovery
- [ ] Protocol
    - [x] Produce
    - [x] Fetch
    - [x] Metadata
    - [x] Create Topics
    - [x] Delete Topics
    - [ ] Consumer group
- [ ] API versioning (more versions to implement)
- [ ] Replication (first draft done)

## Reading

- [How Jocko's built-in service discovery and consensus works](https://medium.com/the-hoard/building-a-kafka-that-doesnt-depend-on-zookeeper-2c4701b6e961)
- [How Jocko's (and Kafka's) storage internals work](https://medium.com/the-hoard/how-kafkas-storage-internals-work-3a29b02e026)

## Project Layout

```
├── cmd/jocko      command to run a Jocko broker and manage topics
├── commitlog      low-level commit log implementation
├── jocko          broker, server, and core subsystems
│   ├── config     configuration
│   ├── fsm        finite state machine for Raft
│   ├── metadata   broker metadata
│   └── structs    data structures
├── log            logging utilities
├── mock           mocks for testing
├── protocol       Kafka protocol implementation
└── testutil       test utilities
```

## Building

### Prerequisites

- Go 1.23 or later

### Local Build

```bash
git clone https://github.com/bodaay/jocko.git
cd jocko
make build
```

### Docker

```bash
docker build -t jocko:latest .
```

### Running

```bash
# Start a single broker
./cmd/jocko/jocko broker

# Start with custom configuration
./cmd/jocko/jocko broker --data-dir /tmp/jocko --broker-addr 127.0.0.1:9092
```

## Contributing

See [CONTRIBUTING](CONTRIBUTING.md) for details on submitting patches and the contribution workflow.

## License

Jocko is under the MIT license, see the [LICENSE](LICENSE) file for details.
