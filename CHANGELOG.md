# Changelog

All notable changes to Quafka (forked from Jocko) are documented here.

## [Unreleased] - 2025

### Project Rename
- **Renamed from Jocko to Quafka** - Fresh identity for the modernized fork
- Updated all import paths from `github.com/travisjeffery/jocko` → `github.com/bodaay/quafka`
- Renamed main package directory from `jocko/` → `quafka/`
- Renamed binary from `jocko` → `quafka`

### Go Version & Tooling
- **Go 1.12 → Go 1.23** - Upgraded to latest stable Go version
- **Removed dep** - Migrated from `dep` to Go modules
- **GitHub Actions CI** - Added automated testing and linting pipeline
- **golangci-lint** - Integrated static analysis with sensible defaults for legacy code

### Dependency Modernization
| Old | New | Notes |
|-----|-----|-------|
| `github.com/Shopify/sarama` | `github.com/IBM/sarama` | Shopify transferred ownership |
| `github.com/satori/go.uuid` | `github.com/google/uuid` | More maintained alternative |
| `github.com/hashicorp/raft-boltdb` | `github.com/hashicorp/raft-boltdb/v2` | Major version upgrade |
| `github.com/boltdb/bolt` | `go.etcd.io/bbolt` | BoltDB is now maintained by etcd |
| `github.com/hashicorp/consul/testutil/retry` | Custom implementation | Removed heavy consul dependency |
| `upspin.io/log` | Custom `log/` package | Removed obscure dependency |
| `io/ioutil` | `os` / `io` | ioutil deprecated in Go 1.16 |

All hashicorp libraries updated to latest versions:
- `hashicorp/raft` v1.7+
- `hashicorp/serf` v0.10+
- `hashicorp/memberlist` v0.5+
- `hashicorp/go-memdb` v1.3+

### Bug Fixes
- **Protocol decoder bounds checking** - Added missing bounds checks to `Int8()` and `Int16()` 
  decoder methods, preventing panics on malformed/truncated messages
- **Raft StartAsLeader removed** - Removed deprecated `StartAsLeader` config (not supported 
  in modern raft)
- **Leader election timing** - Added `WaitForLeader` helper to ensure Raft leader is elected 
  before tests proceed
- **Logging format strings** - Fixed mismatched format specifiers in log statements
- **Append bug** - Fixed `res.Groups = append(res.Groups)` → `append(res.Groups, group)`
- **MetadataResponse ISR** - Added missing error check for `d.Int32Array()`

### Context Propagation
- **Broker lifecycle context** - Added `context.Context` to Broker for graceful shutdown
- **Replicator context support** - `Replicate()` now accepts context for cancellation
- **Conn context methods** - Added `FetchContext`, `CreateTopicsContext`, `LeaderAndISRContext`
- **Context-aware operations** - Internal operations respect context deadlines and cancellation

This enables:
- Graceful shutdown of background replication goroutines
- Request timeout propagation through the stack
- Clean cancellation of in-flight operations

### Code Quality Improvements
- **Replaced all panic() calls** - Converted to proper error returns in:
  - `broker.go` - All request handlers
  - `server.go` - Request decoding
  - `leader.go` - Raft operations
  - `fsm/commands.go` - State machine commands
  - `protocol/request_header.go` - Encoding
- **Removed goto statements** - Refactored for cleaner control flow in:
  - `broker.go` - Main run loop
  - `replicator.go` - Backoff handling
  - `replica_lookup.go` - Partition lookup
- **Removed debug prints** - Cleaned up leftover `fmt.Println` and `spew.Dump` calls
- **Fixed struct literals** - Added field names to composite literals
- **Fixed receiver types** - Changed `Replica.String()` to pointer receiver

### Dockerfile & Build
- **Base image**: `golang:1.12-alpine` → `golang:1.23-alpine`
- **Removed dep installation** - Uses Go modules directly
- **Simplified build process** - Single `go build` command

### Documentation
- Updated README with modern build instructions
- Added acknowledgment section for original author Travis Jeffery
- Updated CONTRIBUTING.md with Go modules workflow
- Removed outdated personal links and book promotions

### Known Limitations
- `TestConsumerGroup` - Needs migration to IBM/sarama ConsumerGroup API (was using 
  deprecated `bsm/sarama-cluster`)
- Some integration tests require longer timeouts in CI environments

---

## Migration from Jocko

If you're coming from the original Jocko project:

1. **Update imports**:
   ```go
   // Old
   import "github.com/travisjeffery/jocko/jocko"
   
   // New  
   import "github.com/bodaay/quafka/quafka"
   ```

2. **Update go.mod**:
   ```
   require github.com/bodaay/quafka v0.1.0
   ```

3. **Binary name changed**: `jocko` → `quafka`

4. **Docker image**: Build locally with `make build-docker`

---

## Original Project

Quafka is a modernized fork of [Jocko](https://github.com/travisjeffery/jocko) by 
[Travis Jeffery](https://github.com/travisjeffery). The original project implemented 
a Kafka-compatible broker in Go using Raft for consensus and Serf for discovery.

See the [Acknowledgments](README.md#acknowledgments) section for more details.

