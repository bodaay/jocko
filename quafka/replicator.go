package quafka

import (
	"context"
	"time"

	"github.com/cenkalti/backoff"

	"github.com/bodaay/quafka/log"
	"github.com/bodaay/quafka/protocol"
)

// Client is used to request other brokers.
type client interface {
	FetchContext(ctx context.Context, fetchRequest *protocol.FetchRequest) (*protocol.FetchResponse, error)
	CreateTopicsContext(ctx context.Context, createRequest *protocol.CreateTopicRequests) (*protocol.CreateTopicsResponse, error)
	LeaderAndISRContext(ctx context.Context, request *protocol.LeaderAndISRRequest) (*protocol.LeaderAndISRResponse, error)
	// others
}

// Replicator fetches from the partition's leader producing to itself the follower, thereby replicating the partition.
type Replicator struct {
	config              ReplicatorConfig
	replica             *Replica
	highwaterMarkOffset int64
	offset              int64
	msgs                chan []byte
	done                chan struct{}
	leader              client
	backoff             *backoff.ExponentialBackOff
	cancel              context.CancelFunc
}

type ReplicatorConfig struct {
	MinBytes int32
	// todo: make this a time.Duration
	MaxWaitTime time.Duration
}

// NewReplicator returns a new replicator instance.
func NewReplicator(config ReplicatorConfig, replica *Replica, leader client) *Replicator {
	if config.MinBytes == 0 {
		config.MinBytes = 1
	}
	bo := backoff.NewExponentialBackOff()
	r := &Replicator{
		config:  config,
		replica: replica,
		leader:  leader,
		done:    make(chan struct{}, 2),
		msgs:    make(chan []byte, 2),
		backoff: bo,
	}
	return r
}

// Replicate starts fetching messages from the leader and appending them to the local commit log.
// The provided context controls the lifetime of the replication goroutines.
func (r *Replicator) Replicate(ctx context.Context) {
	ctx, r.cancel = context.WithCancel(ctx)
	go r.fetchMessages(ctx)
	go r.appendMessages(ctx)
}

func (r *Replicator) fetchMessages(ctx context.Context) {
	var fetchRequest *protocol.FetchRequest
	var fetchResponse *protocol.FetchResponse
	var err error
	for {
		select {
		case <-ctx.Done():
			return
		case <-r.done:
			return
		default:
			fetchRequest = &protocol.FetchRequest{
				ReplicaID:   r.replica.BrokerID,
				MaxWaitTime: r.config.MaxWaitTime,
				MinBytes:    r.config.MinBytes,
				Topics: []*protocol.FetchTopic{{
					Topic: r.replica.Partition.Topic,
					Partitions: []*protocol.FetchPartition{{
						Partition:   r.replica.Partition.ID,
						FetchOffset: r.offset,
					}},
				}},
			}

			// Use context with timeout for fetch operations
			fetchCtx, cancel := context.WithTimeout(ctx, r.config.MaxWaitTime+5*time.Second)
			fetchResponse, err = r.leader.FetchContext(fetchCtx, fetchRequest)
			cancel()

			if err != nil {
				if ctx.Err() != nil {
					return // Context cancelled, exit gracefully
				}
				log.Error.Printf("replicator: fetch messages error: %s", err)
				time.Sleep(r.backoff.NextBackOff())
				continue
			}

			needBackoff := false
			for _, resp := range fetchResponse.Responses {
				for _, p := range resp.PartitionResponses {
					if p.ErrorCode != protocol.ErrNone.Code() {
						log.Error.Printf("replicator: partition response error: %d", p.ErrorCode)
						needBackoff = true
						break
					}
					if p.RecordSet == nil {
						needBackoff = true
						break
					}
					offset := int64(protocol.Encoding.Uint64(p.RecordSet[:8]))
					if offset > r.offset {
						select {
						case r.msgs <- p.RecordSet:
							r.highwaterMarkOffset = p.HighWatermark
							r.offset = offset
						case <-ctx.Done():
							return
						}
					}
				}
				if needBackoff {
					break
				}
			}

			if needBackoff {
				time.Sleep(r.backoff.NextBackOff())
			} else {
				r.backoff.Reset()
			}
		}
	}
}

func (r *Replicator) appendMessages(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-r.done:
			return
		case msg := <-r.msgs:
			_, err := r.replica.Log.Append(msg)
			if err != nil {
				log.Error.Printf("replicator: failed to append to log: %v", err)
				// Continue processing messages even on error - the message will be re-fetched
			}
		}
	}
}

// Close the replicator object when we are no longer following
func (r *Replicator) Close() error {
	if r.cancel != nil {
		r.cancel()
	}
	close(r.done)
	return nil
}
