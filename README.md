# Quafka 🦆

![CI](https://github.com/bodaay/quafka/actions/workflows/ci.yml/badge.svg)

Kafka/distributed commit log service in Go.

*Quafka = Kafka + Go (sounds like a duck!) 🦆*

## Goals

- Implement Kafka in Go
- Protocol compatible with Kafka so Kafka clients and services work with Quafka
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

- [How the built-in service discovery and consensus works](https://medium.com/the-hoard/building-a-kafka-that-doesnt-depend-on-zookeeper-2c4701b6e961)
- [How Kafka's storage internals work](https://medium.com/the-hoard/how-kafkas-storage-internals-work-3a29b02e026)

## Project Layout

```
├── cmd/quafka     command to run a Quafka broker and manage topics
├── commitlog      low-level commit log implementation
├── quafka         broker, server, and core subsystems
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
git clone https://github.com/bodaay/quafka.git
cd quafka
make build
```

### Docker

```bash
docker build -t quafka:latest .
```

### Running

```bash
# Start a single broker
./cmd/quafka/quafka broker

# Start with custom configuration
./cmd/quafka/quafka broker --data-dir /tmp/quafka --broker-addr 127.0.0.1:9092
```

## Contributing

See [CONTRIBUTING](CONTRIBUTING.md) for details on submitting patches and the contribution workflow.

## License

Quafka is under the MIT license, see the [LICENSE](LICENSE) file for details.

---

*Originally forked from [travisjeffery/jocko](https://github.com/travisjeffery/jocko)*
