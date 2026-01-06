package quafka

import (
	"bytes"
	"container/ring"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/davecgh/go-spew/spew"
	"github.com/google/uuid"
	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb/v2"
	"github.com/hashicorp/serf/serf"
	opentracing "github.com/opentracing/opentracing-go"
	"github.com/pkg/errors"

	"github.com/bodaay/quafka/commitlog"
	"github.com/bodaay/quafka/log"
	"github.com/bodaay/quafka/protocol"
	"github.com/bodaay/quafka/quafka/config"
	"github.com/bodaay/quafka/quafka/fsm"
	"github.com/bodaay/quafka/quafka/metadata"
	"github.com/bodaay/quafka/quafka/structs"
	"github.com/bodaay/quafka/quafka/util"
)

var (
	brokerVerboseLogs bool

	ErrTopicExists            = errors.New("topic exists already")
	ErrInvalidArgument        = errors.New("no logger set")
	OffsetsTopicName          = "__consumer_offsets"
	OffsetsTopicNumPartitions = 50
)

const (
	serfLANSnapshot   = "serf/local.snapshot"
	raftState         = "raft/"
	raftLogCacheSize  = 512
	snapshotsRetained = 2
)

func init() {
	spew.Config.Indent = ""

	e := os.Getenv("QUAFKADEBUG")
	if strings.Contains(e, "broker=1") {
		brokerVerboseLogs = true
	}
}

// Broker represents a broker in a Quafka cluster, like a broker in a Kafka cluster.
type Broker struct {
	sync.RWMutex
	config *config.Config

	// ctx is the broker's lifecycle context, canceled on shutdown
	ctx    context.Context
	cancel context.CancelFunc

	// readyForConsistentReads is used to track when the leader server is
	// ready to serve consistent reads, after it has applied its initial
	// barrier. This is updated atomically.
	readyForConsistentReads int32
	// brokerLookup tracks servers in the local datacenter.
	brokerLookup  *brokerLookup
	replicaLookup *replicaLookup
	// The raft instance is used among Quafka brokers within the DC to protect operations that require strong consistency.
	raft          *raft.Raft
	raftStore     *raftboltdb.BoltStore
	raftTransport *raft.NetworkTransport
	raftInmem     *raft.InmemStore
	// raftNotifyCh ensures we get reliable leader transition notifications from the raft layer.
	raftNotifyCh <-chan bool
	// reconcileCh is used to pass events from the serf handler to the raft leader to update its state.
	reconcileCh      chan serf.Member
	serf             *serf.Serf
	fsm              *fsm.FSM
	eventChLAN       chan serf.Event
	logStateInterval time.Duration

	tracer opentracing.Tracer

	shutdownCh   chan struct{}
	shutdown     bool
	shutdownLock sync.Mutex
}

// New is used to instantiate a new broker.
func NewBroker(config *config.Config, tracer opentracing.Tracer) (*Broker, error) {
	ctx, cancel := context.WithCancel(context.Background())
	b := &Broker{
		config:           config,
		ctx:              ctx,
		cancel:           cancel,
		shutdownCh:       make(chan struct{}),
		eventChLAN:       make(chan serf.Event, 256),
		brokerLookup:     NewBrokerLookup(),
		replicaLookup:    NewReplicaLookup(),
		reconcileCh:      make(chan serf.Member, 32),
		tracer:           tracer,
		logStateInterval: time.Millisecond * 250,
	}

	if err := b.setupRaft(); err != nil {
		b.Shutdown()
		return nil, fmt.Errorf("start raft: %v", err)
	}

	var err error
	b.serf, err = b.setupSerf(config.SerfLANConfig, b.eventChLAN, serfLANSnapshot)
	if err != nil {
		return nil, err
	}

	go b.lanEventHandler()

	go b.monitorLeadership()

	go b.logState()

	return b, nil
}

// Broker API.

// Run starts a loop to handle requests send back responses.
func (b *Broker) Run(ctx context.Context, requests <-chan *Context, responses chan<- *Context) {
	defer func() {
		log.Debug.Printf("broker/%d: run done", b.config.ID)
	}()

	for {
		select {
		case reqCtx := <-requests:
			log.Debug.Printf("broker/%d: request: %v", b.config.ID, reqCtx)

			if reqCtx == nil {
				return
			}

			queueSpan, ok := reqCtx.Value(requestQueueSpanKey).(opentracing.Span)
			if ok {
				queueSpan.Finish()
			}

			var res protocol.ResponseBody

			switch req := reqCtx.req.(type) {
			case *protocol.ProduceRequest:
				res = b.handleProduce(reqCtx, req)
			case *protocol.FetchRequest:
				res = b.handleFetch(reqCtx, req)
			case *protocol.OffsetsRequest:
				res = b.handleOffsets(reqCtx, req)
			case *protocol.MetadataRequest:
				res = b.handleMetadata(reqCtx, req)
			case *protocol.LeaderAndISRRequest:
				res = b.handleLeaderAndISR(reqCtx, req)
			case *protocol.StopReplicaRequest:
				res = b.handleStopReplica(reqCtx, req)
			case *protocol.UpdateMetadataRequest:
				res = b.handleUpdateMetadata(reqCtx, req)
			case *protocol.ControlledShutdownRequest:
				res = b.handleControlledShutdown(reqCtx, req)
			case *protocol.OffsetCommitRequest:
				res = b.handleOffsetCommit(reqCtx, req)
			case *protocol.OffsetFetchRequest:
				res = b.handleOffsetFetch(reqCtx, req)
			case *protocol.FindCoordinatorRequest:
				res = b.handleFindCoordinator(reqCtx, req)
			case *protocol.JoinGroupRequest:
				res = b.handleJoinGroup(reqCtx, req)
			case *protocol.HeartbeatRequest:
				res = b.handleHeartbeat(reqCtx, req)
			case *protocol.LeaveGroupRequest:
				res = b.handleLeaveGroup(reqCtx, req)
			case *protocol.SyncGroupRequest:
				res = b.handleSyncGroup(reqCtx, req)
			case *protocol.DescribeGroupsRequest:
				res = b.handleDescribeGroups(reqCtx, req)
			case *protocol.ListGroupsRequest:
				res = b.handleListGroups(reqCtx, req)
			case *protocol.SaslHandshakeRequest:
				res = b.handleSaslHandshake(reqCtx, req)
			case *protocol.APIVersionsRequest:
				res = b.handleAPIVersions(reqCtx, req)
			case *protocol.CreateTopicRequests:
				res = b.handleCreateTopic(reqCtx, req)
			case *protocol.DeleteTopicsRequest:
				res = b.handleDeleteTopics(reqCtx, req)
			}

			parentSpan := opentracing.SpanFromContext(reqCtx)
			queueSpan = b.tracer.StartSpan("broker: queue response", opentracing.ChildOf(parentSpan.Context()))
			responseCtx := context.WithValue(reqCtx, responseQueueSpanKey, queueSpan)

			responses <- &Context{
				parent: responseCtx,
				conn:   reqCtx.conn,
				header: reqCtx.header,
				res: &protocol.Response{
					CorrelationID: reqCtx.header.CorrelationID,
					Body:          res,
				},
			}
		case <-ctx.Done():
			return
		}
	}
}

// Join is used to have the broker join the gossip ring.
// The given address should be another broker listening on the Serf address.
func (b *Broker) JoinLAN(addrs ...string) protocol.Error {
	if _, err := b.serf.Join(addrs, true); err != nil {
		return protocol.ErrUnknown.WithErr(err)
	}
	return protocol.ErrNone
}

// req handling.

func span(ctx context.Context, tracer opentracing.Tracer, op string) opentracing.Span {
	if ctx == nil {
		// only done for unit tests
		return tracer.StartSpan("broker: " + op)
	}
	parentSpan := opentracing.SpanFromContext(ctx)
	if parentSpan == nil {
		// only done for unit tests
		return tracer.StartSpan("broker: " + op)
	}
	return tracer.StartSpan("broker: "+op, opentracing.ChildOf(parentSpan.Context()))
}

var apiVersions = &protocol.APIVersionsResponse{APIVersions: protocol.APIVersions}

func (b *Broker) handleAPIVersions(ctx *Context, req *protocol.APIVersionsRequest) *protocol.APIVersionsResponse {
	sp := span(ctx, b.tracer, "api versions")
	defer sp.Finish()
	return apiVersions
}

func (b *Broker) handleCreateTopic(ctx *Context, reqs *protocol.CreateTopicRequests) *protocol.CreateTopicsResponse {
	sp := span(ctx, b.tracer, "create topic")
	defer sp.Finish()
	res := new(protocol.CreateTopicsResponse)
	res.APIVersion = reqs.Version()
	res.TopicErrorCodes = make([]*protocol.TopicErrorCode, len(reqs.Requests))
	isController := b.isController()
	sp.LogKV("is controller", isController)
	for i, req := range reqs.Requests {
		if !isController {
			res.TopicErrorCodes[i] = &protocol.TopicErrorCode{
				Topic:     req.Topic,
				ErrorCode: protocol.ErrNotController.Code(),
			}
			continue
		}
		if req.ReplicationFactor > int16(len(b.LANMembers())) {
			res.TopicErrorCodes[i] = &protocol.TopicErrorCode{
				Topic:     req.Topic,
				ErrorCode: protocol.ErrInvalidReplicationFactor.Code(),
			}
			continue
		}
		err := b.withTimeout(reqs.Timeout, func() protocol.Error {
			return b.createTopic(ctx, req)
		})
		res.TopicErrorCodes[i] = &protocol.TopicErrorCode{
			Topic:     req.Topic,
			ErrorCode: err.Code(),
		}

	}
	return res
}

func (b *Broker) handleDeleteTopics(ctx *Context, reqs *protocol.DeleteTopicsRequest) *protocol.DeleteTopicsResponse {
	sp := span(ctx, b.tracer, "delete topics")
	defer sp.Finish()
	res := new(protocol.DeleteTopicsResponse)
	res.APIVersion = reqs.Version()
	res.TopicErrorCodes = make([]*protocol.TopicErrorCode, len(reqs.Topics))
	isController := b.isController()
	for i, topic := range reqs.Topics {
		if !isController {
			res.TopicErrorCodes[i] = &protocol.TopicErrorCode{
				Topic:     topic,
				ErrorCode: protocol.ErrNotController.Code(),
			}
			continue
		}
		err := b.withTimeout(reqs.Timeout, func() protocol.Error {
			// TODO: this will delete from fsm -- need to delete associated partitions, etc.
			_, err := b.raftApply(structs.DeregisterTopicRequestType, structs.DeregisterTopicRequest{
				Topic: structs.Topic{
					Topic: topic,
				},
			})
			if err != nil {
				return protocol.ErrUnknown.WithErr(err)
			}
			return protocol.ErrNone
		})
		res.TopicErrorCodes[i] = &protocol.TopicErrorCode{
			Topic:     topic,
			ErrorCode: err.Code(),
		}
	}
	return res
}

func (b *Broker) handleLeaderAndISR(ctx *Context, req *protocol.LeaderAndISRRequest) *protocol.LeaderAndISRResponse {
	sp := span(ctx, b.tracer, "leader and isr")
	defer sp.Finish()
	res := &protocol.LeaderAndISRResponse{
		Partitions: make([]*protocol.LeaderAndISRPartition, len(req.PartitionStates)),
	}
	res.APIVersion = req.Version()
	setErr := func(i int, p *protocol.PartitionState, err protocol.Error) {
		res.Partitions[i] = &protocol.LeaderAndISRPartition{
			ErrorCode: err.Code(),
			Partition: p.Partition,
			Topic:     p.Topic,
		}
	}
	for i, p := range req.PartitionStates {
		// TODO: need to replace the replica regardless
		replica := &Replica{
			BrokerID: b.config.ID,
			Partition: structs.Partition{
				ID:              p.Partition,
				Partition:       p.Partition,
				Topic:           p.Topic,
				ISR:             p.ISR,
				AR:              p.Replicas,
				ControllerEpoch: p.ZKVersion,
				LeaderEpoch:     p.LeaderEpoch,
				Leader:          p.Leader,
			},
			IsLocal: true,
		}
		b.replicaLookup.AddReplica(replica)

		if p.Leader == b.config.ID && (replica.Partition.Leader == b.config.ID) {
			// is command asking this broker to be the new leader for p and this broker is not already the leader for

			if err := b.startReplica(replica); err != protocol.ErrNone {
				setErr(i, p, err)
				continue
			}

			if err := b.becomeLeader(replica, p); err != protocol.ErrNone {
				setErr(i, p, err)
				continue
			}
		} else if contains(p.Replicas, b.config.ID) && (p.Leader != b.config.ID) {
			// is command asking this broker to follow leader who it isn't a leader of already
			if err := b.startReplica(replica); err != protocol.ErrNone {
				setErr(i, p, err)
				continue
			}

			if err := b.becomeFollower(replica, p); err != protocol.ErrNone {
				setErr(i, p, err)
				continue
			}
		}
		res.Partitions[i] = &protocol.LeaderAndISRPartition{Partition: p.Partition, Topic: p.Topic, ErrorCode: protocol.ErrNone.Code()}
	}
	return res
}

func (b *Broker) handleOffsets(ctx *Context, req *protocol.OffsetsRequest) *protocol.OffsetsResponse {
	sp := span(ctx, b.tracer, "offsets")
	defer sp.Finish()
	res := new(protocol.OffsetsResponse)
	res.APIVersion = req.Version()
	res.Responses = make([]*protocol.OffsetResponse, len(req.Topics))
	for i, t := range req.Topics {
		res.Responses[i] = new(protocol.OffsetResponse)
		res.Responses[i].Topic = t.Topic
		res.Responses[i].PartitionResponses = make([]*protocol.PartitionResponse, 0, len(t.Partitions))
		for _, p := range t.Partitions {
			pres := new(protocol.PartitionResponse)
			pres.Partition = p.Partition
			replica, err := b.replicaLookup.Replica(t.Topic, p.Partition)
			if err != nil {
				// TODO: have replica lookup return an error with a code
				pres.ErrorCode = protocol.ErrUnknown.Code()
				continue
			}
			var offset int64
			if p.Timestamp == -2 {
				offset = replica.Log.OldestOffset()
			} else {
				// TODO: this is nil because i'm not sending the leader and isr requests telling the new leader to start the replica and instantiate the log...
				offset = replica.Log.NewestOffset()
			}
			pres.Offsets = []int64{offset}
			res.Responses[i].PartitionResponses = append(res.Responses[i].PartitionResponses, pres)
		}
	}
	return res
}

func (b *Broker) handleProduce(ctx *Context, req *protocol.ProduceRequest) *protocol.ProduceResponse {
	sp := span(ctx, b.tracer, "produce")
	defer sp.Finish()
	res := new(protocol.ProduceResponse)
	res.APIVersion = req.Version()
	res.Responses = make([]*protocol.ProduceTopicResponse, len(req.TopicData))
	log.Debug.Printf("broker/%d: produce: %#v", b.config.ID, req)
	for i, td := range req.TopicData {
		log.Debug.Printf("broker/%d: produce to partition: %d: %v", b.config.ID, i, td)
		tres := make([]*protocol.ProducePartitionResponse, len(td.Data))
		for j, p := range td.Data {
			pres := &protocol.ProducePartitionResponse{}
			pres.Partition = p.Partition
			err := b.withTimeout(req.Timeout, func() protocol.Error {
				state := b.fsm.State()
				_, t, err := state.GetTopic(td.Topic)
				if err != nil {
					log.Error.Printf("broker/%d: produce to partition error: get topic: %s", b.config.ID, err)
					return protocol.ErrUnknown.WithErr(err)
				}
				if t == nil {
					log.Error.Printf("broker/%d: produce to partition error: unknown topic", b.config.ID)
					return protocol.ErrUnknownTopicOrPartition
				}
				replica, err := b.replicaLookup.Replica(td.Topic, p.Partition)
				if err != nil || replica == nil || replica.Log == nil {
					log.Error.Printf("broker/%d: produce to partition error: %s", b.config.ID, err)
					pres.Partition = p.Partition
					return protocol.ErrReplicaNotAvailable
				}
				offset, appendErr := replica.Log.Append(p.RecordSet)
				if appendErr != nil {
					log.Error.Printf("broker/%d: log append error: %s", b.config.ID, err)
					return protocol.ErrUnknown
				}
				pres.BaseOffset = offset
				pres.LogAppendTime = time.Now()
				return protocol.ErrNone
			})
			pres.ErrorCode = err.Code()
			tres[j] = pres
		}
		res.Responses[i] = &protocol.ProduceTopicResponse{
			Topic:              td.Topic,
			PartitionResponses: tres,
		}
	}
	return res
}

func (b *Broker) handleMetadata(ctx *Context, req *protocol.MetadataRequest) *protocol.MetadataResponse {
	sp := span(ctx, b.tracer, "metadata")
	defer sp.Finish()
	state := b.fsm.State()
	brokers := make([]*protocol.Broker, 0, len(b.LANMembers()))

	_, nodes, err := state.GetNodes()
	if err != nil {
		log.Error.Printf("broker/%d: failed to get nodes: %v", b.config.ID, err)
		return &protocol.MetadataResponse{}
	}

	// TODO: add an index to the table on the check status
	var passing []*structs.Node
	for _, n := range nodes {
		if n.Check.Status == structs.HealthPassing {
			passing = append(passing, n)
		}
	}

	for _, mem := range b.LANMembers() {
		// TODO: should filter elsewhere
		if mem.Status != serf.StatusAlive {
			continue
		}

		m, ok := metadata.IsBroker(mem)
		if !ok {
			continue
		}
		brokers = append(brokers, &protocol.Broker{
			NodeID: m.ID.Int32(),
			Host:   m.Host(),
			Port:   m.Port(),
		})
	}
	var topicMetadata []*protocol.TopicMetadata
	topicMetadataFn := func(topic *structs.Topic, err protocol.Error) *protocol.TopicMetadata {
		if err != protocol.ErrNone {
			return &protocol.TopicMetadata{
				TopicErrorCode: err.Code(),
				Topic:          topic.Topic,
			}
		}
		partitionMetadata := make([]*protocol.PartitionMetadata, 0, len(topic.Partitions))
		for id := range topic.Partitions {
			_, p, err := state.GetPartition(topic.Topic, id)
			if err != nil {
				partitionMetadata = append(partitionMetadata, &protocol.PartitionMetadata{
					PartitionID:        id,
					PartitionErrorCode: protocol.ErrUnknown.Code(),
				})
				continue
			}
			if p == nil {
				partitionMetadata = append(partitionMetadata, &protocol.PartitionMetadata{
					PartitionID:        id,
					PartitionErrorCode: protocol.ErrUnknownTopicOrPartition.Code(),
				})
				continue
			}
			partitionMetadata = append(partitionMetadata, &protocol.PartitionMetadata{
				PartitionID:        p.ID,
				PartitionErrorCode: protocol.ErrNone.Code(),
				Leader:             p.Leader,
				Replicas:           p.AR,
				ISR:                p.ISR,
			})
		}
		return &protocol.TopicMetadata{
			TopicErrorCode:    protocol.ErrNone.Code(),
			Topic:             topic.Topic,
			PartitionMetadata: partitionMetadata,
		}
	}
	if len(req.Topics) == 0 {
		// Respond with metadata for all topics
		// how to handle err here?
		_, topics, _ := state.GetTopics()
		topicMetadata = make([]*protocol.TopicMetadata, 0, len(topics))
		for _, topic := range topics {
			topicMetadata = append(topicMetadata, topicMetadataFn(topic, protocol.ErrNone))
		}
	} else {
		topicMetadata = make([]*protocol.TopicMetadata, 0, len(req.Topics))
		for _, topicName := range req.Topics {
			_, topic, err := state.GetTopic(topicName)
			if topic == nil {
				topicMetadata = append(topicMetadata, topicMetadataFn(&structs.Topic{Topic: topicName}, protocol.ErrUnknownTopicOrPartition))
			} else if err != nil {
				topicMetadata = append(topicMetadata, topicMetadataFn(&structs.Topic{Topic: topicName}, protocol.ErrUnknown.WithErr(err)))
			} else {
				topicMetadata = append(topicMetadata, topicMetadataFn(topic, protocol.ErrNone))
			}
		}
	}
	res := &protocol.MetadataResponse{
		Brokers:       brokers,
		TopicMetadata: topicMetadata,
	}
	res.APIVersion = req.Version()
	return res
}

func (b *Broker) handleFindCoordinator(ctx *Context, req *protocol.FindCoordinatorRequest) *protocol.FindCoordinatorResponse {
	sp := span(ctx, b.tracer, "find coordinator")
	defer sp.Finish()

	res := &protocol.FindCoordinatorResponse{}
	res.APIVersion = req.Version()

	state := b.fsm.State()

	topic, err := b.offsetsTopic(ctx)
	if err != nil {
		res.ErrorCode = protocol.ErrUnknown.Code()
		log.Error.Printf("broker/%d: coordinator error getting offsets topic: %v", b.config.ID, err)
		return res
	}

	i := int32(util.Hash(req.CoordinatorKey) % uint64(len(topic.Partitions)))
	_, p, err := state.GetPartition(OffsetsTopicName, i)
	if err != nil {
		res.ErrorCode = protocol.ErrUnknown.Code()
		log.Error.Printf("broker/%d: coordinator error getting partition: %v", b.config.ID, err)
		return res
	}
	if p == nil {
		res.ErrorCode = protocol.ErrUnknownTopicOrPartition.Code()
		log.Error.Printf("broker/%d: coordinator error: partition not found", b.config.ID)
		return res
	}

	broker := b.brokerLookup.BrokerByID(raft.ServerID(fmt.Sprintf("%d", p.Leader)))
	res.Coordinator.NodeID = broker.ID.Int32()
	res.Coordinator.Host = broker.Host()
	res.Coordinator.Port = broker.Port()

	return res
}

func (b *Broker) handleJoinGroup(ctx *Context, r *protocol.JoinGroupRequest) *protocol.JoinGroupResponse {
	sp := span(ctx, b.tracer, "join group")
	defer sp.Finish()

	res := &protocol.JoinGroupResponse{}
	res.APIVersion = r.Version()

	state := b.fsm.State()

	_, group, err := state.GetGroup(r.GroupID)
	if err != nil {
		log.Error.Printf("broker/%d: get group error: %s", b.config.ID, err)
		res.ErrorCode = protocol.ErrUnknown.Code()
		return res
	}

	// Handle group creation vs existing group
	if group == nil {
		// Group doesn't exist
		if r.MemberID != "" {
			// Member ID specified but group doesn't exist - reject
			res.ErrorCode = protocol.ErrInvalidGroupId.Code()
			return res
		}
		// Create new group
		group = &structs.Group{
			Group:        r.GroupID,
			Coordinator:  b.config.ID,
			Members:      make(map[string]structs.Member),
			State:        structs.GroupStateEmpty,
			GenerationID: 0,
		}
	} else {
		// Group exists - validate member
		if r.MemberID != "" {
			// Existing member rejoining
			if _, exists := group.Members[r.MemberID]; !exists {
				// Member ID doesn't exist in group
				res.ErrorCode = protocol.ErrUnknownMemberId.Code()
				return res
			}
		}
	}

	// Generate member ID if not provided
	if r.MemberID == "" {
		r.MemberID = ctx.Header().ClientID + "-" + uuid.New().String()
		group.Members[r.MemberID] = structs.Member{
			ID:       r.MemberID,
			Metadata: r.GroupProtocols[0].ProtocolMetadata, // Use first protocol's metadata
		}
	} else {
		// Update existing member metadata
		if member, exists := group.Members[r.MemberID]; exists {
			if len(r.GroupProtocols) > 0 {
				member.Metadata = r.GroupProtocols[0].ProtocolMetadata
				group.Members[r.MemberID] = member
			}
		}
	}

	// Set leader if not set
	if group.LeaderID == "" {
		group.LeaderID = r.MemberID
	}

	// Transition to PreparingRebalance if not already
	if group.State == structs.GroupStateEmpty || group.State == structs.GroupStateStable {
		group.State = structs.GroupStatePreparingRebalance
		group.GenerationID++
	}

	// For simplicity, transition to CompletingRebalance immediately
	// In a real implementation, this would wait for all expected members to join
	// or timeout after SessionTimeout
	if group.State == structs.GroupStatePreparingRebalance {
		group.State = structs.GroupStateCompletingRebalance
	}

	// Save group
	_, err = b.raftApply(structs.RegisterGroupRequestType, structs.RegisterGroupRequest{
		Group: *group,
	})
	if err != nil {
		log.Error.Printf("broker/%d: register group error: %s", b.config.ID, err)
		res.ErrorCode = protocol.ErrUnknown.Code()
		return res
	}

	res.GenerationID = group.GenerationID
	res.LeaderID = group.LeaderID
	res.MemberID = r.MemberID

	// Set the selected protocol (use the first one from the request)
	if len(r.GroupProtocols) > 0 {
		res.GroupProtocol = r.GroupProtocols[0].ProtocolName
	}

	// Fill in members on response for the leader
	if res.LeaderID == res.MemberID {
		for _, m := range group.Members {
			res.Members = append(res.Members, protocol.Member{
				MemberID:       m.ID,
				MemberMetadata: m.Metadata,
			})
		}
	}

	return res
}

func (b *Broker) handleLeaveGroup(ctx *Context, r *protocol.LeaveGroupRequest) *protocol.LeaveGroupResponse {
	sp := span(ctx, b.tracer, "leave group")
	defer sp.Finish()

	res := &protocol.LeaveGroupResponse{}
	res.APIVersion = r.Version()

	// // TODO: distribute this.
	state := b.fsm.State()

	_, group, err := state.GetGroup(r.GroupID)
	if err != nil {
		res.ErrorCode = protocol.ErrUnknown.Code()
		return res
	}
	if group == nil {
		res.ErrorCode = protocol.ErrInvalidGroupId.Code()
		return res
	}
	if _, ok := group.Members[r.MemberID]; !ok {
		res.ErrorCode = protocol.ErrUnknownMemberId.Code()
		return res
	}

	delete(group.Members, r.MemberID)

	_, err = b.raftApply(structs.RegisterGroupRequestType, structs.RegisterGroupRequest{
		Group: *group,
	})
	if err != nil {
		res.ErrorCode = protocol.ErrUnknown.Code()
		return res
	}

	return res
}

func (b *Broker) handleSyncGroup(ctx *Context, r *protocol.SyncGroupRequest) *protocol.SyncGroupResponse {
	sp := span(ctx, b.tracer, "sync group")
	defer sp.Finish()

	state := b.fsm.State()
	res := &protocol.SyncGroupResponse{}
	res.APIVersion = r.Version()

	_, group, err := state.GetGroup(r.GroupID)
	if err != nil {
		res.ErrorCode = protocol.ErrUnknown.Code()
		return res
	}
	if group == nil {
		res.ErrorCode = protocol.ErrInvalidGroupId.Code()
		return res
	}
	if _, ok := group.Members[r.MemberID]; !ok {
		res.ErrorCode = protocol.ErrUnknownMemberId.Code()
		return res
	}
	if r.GenerationID != group.GenerationID {
		res.ErrorCode = protocol.ErrIllegalGeneration.Code()
		return res
	}
	switch group.State {
	case structs.GroupStateEmpty, structs.GroupStateDead:
		res.ErrorCode = protocol.ErrUnknownMemberId.Code()
		return res
	case structs.GroupStatePreparingRebalance:
		res.ErrorCode = protocol.ErrRebalanceInProgress.Code()
		return res
	case structs.GroupStateCompletingRebalance:
		if group.LeaderID == r.MemberID {
			// Leader provides assignments - save them and transition to stable
			for _, ga := range r.GroupAssignments {
				if m, ok := group.Members[ga.MemberID]; ok {
					m.Assignment = ga.MemberAssignment
					group.Members[ga.MemberID] = m
				} else {
					log.Error.Printf("broker/%d: sync group: unknown member in assignments: %s", b.config.ID, ga.MemberID)
				}
			}
			// Transition to stable
			group.State = structs.GroupStateStable
			_, err = b.raftApply(structs.RegisterGroupRequestType, structs.RegisterGroupRequest{
				Group: *group,
			})
			if err != nil {
				res.ErrorCode = protocol.ErrUnknown.Code()
				return res
			}
			// Leader also gets their assignment
			if m, ok := group.Members[r.MemberID]; ok {
				res.MemberAssignment = m.Assignment
			}
		} else {
			// Non-leader member - return their assignment if available
			if m, ok := group.Members[r.MemberID]; ok && m.Assignment != nil {
				res.MemberAssignment = m.Assignment
			} else {
				// Assignment not ready yet, return rebalance in progress
				res.ErrorCode = protocol.ErrRebalanceInProgress.Code()
				return res
			}
		}
	case structs.GroupStateStable:
		// In stable state, return current assignment
		if m, ok := group.Members[r.MemberID]; ok {
			res.MemberAssignment = m.Assignment
		} else {
			log.Error.Printf("broker/%d: sync group: unknown member: %s", b.config.ID, r.MemberID)
			res.ErrorCode = protocol.ErrUnknownMemberId.Code()
		}
	}

	return res
}

func (b *Broker) handleHeartbeat(ctx *Context, r *protocol.HeartbeatRequest) *protocol.HeartbeatResponse {
	sp := span(ctx, b.tracer, "heartbeat")
	defer sp.Finish()

	res := &protocol.HeartbeatResponse{}
	res.APIVersion = r.Version()

	state := b.fsm.State()
	_, group, err := state.GetGroup(r.GroupID)
	if err != nil {
		res.ErrorCode = protocol.ErrUnknown.Code()
		return res
	}
	if group == nil {
		res.ErrorCode = protocol.ErrInvalidGroupId.Code()
		return res
	}

	// Validate member exists
	if _, ok := group.Members[r.MemberID]; !ok {
		res.ErrorCode = protocol.ErrUnknownMemberId.Code()
		return res
	}

	// Validate generation ID
	if r.GroupGenerationID != group.GenerationID {
		res.ErrorCode = protocol.ErrIllegalGeneration.Code()
		return res
	}

	// Check if rebalance is in progress
	if group.State == structs.GroupStatePreparingRebalance || group.State == structs.GroupStateCompletingRebalance {
		res.ErrorCode = protocol.ErrRebalanceInProgress.Code()
		return res
	}

	// Heartbeat is valid
	res.ErrorCode = protocol.ErrNone.Code()
	return res
}

func (b *Broker) handleFetch(ctx *Context, r *protocol.FetchRequest) *protocol.FetchResponse {
	sp := span(ctx, b.tracer, "fetch")
	defer sp.Finish()
	fres := &protocol.FetchResponse{
		Responses: make(protocol.FetchTopicResponses, len(r.Topics)),
	}
	fres.APIVersion = r.Version()
	for i, topic := range r.Topics {
		fr := &protocol.FetchTopicResponse{
			Topic:              topic.Topic,
			PartitionResponses: make([]*protocol.FetchPartitionResponse, len(topic.Partitions)),
		}
		for j, p := range topic.Partitions {
			fpres := &protocol.FetchPartitionResponse{}
			fpres.Partition = p.Partition
			err := b.withTimeout(r.MaxWaitTime, func() protocol.Error {
				replica, err := b.replicaLookup.Replica(topic.Topic, p.Partition)
				if err != nil {
					return protocol.ErrReplicaNotAvailable
				}
				if replica.Partition.Leader != b.config.ID {
					return protocol.ErrNotLeaderForPartition
				}
				if replica.Log == nil {
					return protocol.ErrReplicaNotAvailable
				}
				rdr, rdrErr := replica.Log.NewReader(p.FetchOffset, p.MaxBytes)
				if rdrErr != nil {
					log.Error.Printf("broker/%d: replica log read error: %s", b.config.ID, rdrErr)
					return protocol.ErrUnknown.WithErr(rdrErr)
				}
				buf := new(bytes.Buffer)
				var n int32
				for n < r.MinBytes {
					// TODO: copy these bytes to outer bytes
					nn, err := io.Copy(buf, rdr)
					if err != nil && err != io.EOF {
						log.Error.Printf("broker/%d: reader copy error: %v", b.config.ID, err)
						return protocol.ErrUnknown.WithErr(rdrErr)
					}
					n += int32(nn)
					if err == io.EOF {
						// TODO: should use a different error here?
						break
					}
				}
				fpres.HighWatermark = replica.Log.NewestOffset() - 1
				fpres.RecordSet = buf.Bytes()
				return protocol.ErrNone
			})
			fpres.ErrorCode = err.Code()
			fr.PartitionResponses[j] = fpres
		}
		fres.Responses[i] = fr
	}
	return fres
}

func (b *Broker) handleSaslHandshake(ctx *Context, req *protocol.SaslHandshakeRequest) *protocol.SaslHandshakeResponse {
	sp := span(ctx, b.tracer, "sasl handshake")
	defer sp.Finish()
	log.Error.Printf("broker/%d: SASL handshake not implemented", b.config.ID)
	return &protocol.SaslHandshakeResponse{}
}

func (b *Broker) handleListGroups(ctx *Context, req *protocol.ListGroupsRequest) *protocol.ListGroupsResponse {
	sp := span(ctx, b.tracer, "list groups")
	defer sp.Finish()
	res := new(protocol.ListGroupsResponse)
	res.APIVersion = req.Version()
	state := b.fsm.State()

	_, groups, err := state.GetGroups()
	if err != nil {
		res.ErrorCode = protocol.ErrUnknown.Code()
		return res
	}
	for _, group := range groups {
		// Default to "consumer" protocol type for now
		// In a full implementation, this would be stored in the group struct
		protocolType := "consumer"
		res.Groups = append(res.Groups, protocol.ListGroup{
			GroupID:      group.Group,
			ProtocolType: protocolType,
		})
	}
	return res
}

func (b *Broker) handleDescribeGroups(ctx *Context, req *protocol.DescribeGroupsRequest) *protocol.DescribeGroupsResponse {
	sp := span(ctx, b.tracer, "describe groups")
	defer sp.Finish()
	res := new(protocol.DescribeGroupsResponse)
	res.APIVersion = req.Version()
	state := b.fsm.State()

	for _, id := range req.GroupIDs {
		group := protocol.Group{
			GroupMembers: make(map[string]*protocol.GroupMember),
		}
		_, g, err := state.GetGroup(id)
		if err != nil {
			group.ErrorCode = protocol.ErrUnknown.Code()
			group.GroupID = id
			res.Groups = append(res.Groups, group)
			continue
		}
		if g == nil {
			group.ErrorCode = protocol.ErrInvalidGroupId.Code()
			group.GroupID = id
			res.Groups = append(res.Groups, group)
			continue
		}

		group.GroupID = id
		group.ErrorCode = protocol.ErrNone.Code()

		// Map group state to string
		switch g.State {
		case structs.GroupStateEmpty:
			group.State = "Empty"
		case structs.GroupStatePreparingRebalance:
			group.State = "PreparingRebalance"
		case structs.GroupStateCompletingRebalance:
			group.State = "CompletingRebalance"
		case structs.GroupStateStable:
			group.State = "Stable"
		case structs.GroupStateDead:
			group.State = "Dead"
		default:
			group.State = "Unknown"
		}

		// Default to "consumer" protocol type
		// In a full implementation, this would be stored in the group struct
		group.ProtocolType = "consumer"
		group.Protocol = "consumer"

		// Add members
		for memberID, member := range g.Members {
			// Extract client host from member ID if possible (format: clientID-uuid)
			clientHost := ""
			if idx := strings.LastIndex(memberID, "-"); idx > 0 {
				// Could extract more info, but for now leave empty
				clientHost = ""
			}

			group.GroupMembers[memberID] = &protocol.GroupMember{
				ClientID:              memberID,
				ClientHost:            clientHost,
				GroupMemberMetadata:   member.Metadata,
				GroupMemberAssignment: member.Assignment,
			}
		}

		res.Groups = append(res.Groups, group)
	}

	return res
}

func (b *Broker) handleStopReplica(ctx *Context, req *protocol.StopReplicaRequest) *protocol.StopReplicaResponse {
	sp := span(ctx, b.tracer, "stop replica")
	defer sp.Finish()
	log.Error.Printf("broker/%d: stop replica not implemented", b.config.ID)
	return &protocol.StopReplicaResponse{}
}

func (b *Broker) handleUpdateMetadata(ctx *Context, req *protocol.UpdateMetadataRequest) *protocol.UpdateMetadataResponse {
	sp := span(ctx, b.tracer, "update metadata")
	defer sp.Finish()
	log.Error.Printf("broker/%d: update metadata not implemented", b.config.ID)
	return &protocol.UpdateMetadataResponse{}
}

func (b *Broker) handleControlledShutdown(ctx *Context, req *protocol.ControlledShutdownRequest) *protocol.ControlledShutdownResponse {
	sp := span(ctx, b.tracer, "controlled shutdown")
	defer sp.Finish()
	log.Error.Printf("broker/%d: controlled shutdown not implemented", b.config.ID)
	res := &protocol.ControlledShutdownResponse{}
	res.APIVersion = req.Version()
	return res
}

func (b *Broker) handleOffsetCommit(ctx *Context, req *protocol.OffsetCommitRequest) *protocol.OffsetCommitResponse {
	sp := span(ctx, b.tracer, "offset commit")
	defer sp.Finish()

	res := &protocol.OffsetCommitResponse{}
	res.APIVersion = req.Version()
	res.Responses = make([]protocol.OffsetCommitTopicResponse, len(req.Topics))

	// Validate coordinator
	state := b.fsm.State()
	topic, err := b.offsetsTopic(ctx)
	if err != nil {
		log.Error.Printf("broker/%d: offset commit error getting offsets topic: %v", b.config.ID, err)
		for i := range res.Responses {
			res.Responses[i] = protocol.OffsetCommitTopicResponse{
				Topic:              req.Topics[i].Topic,
				PartitionResponses: make([]protocol.OffsetCommitPartitionResponse, len(req.Topics[i].Partitions)),
			}
			for j := range res.Responses[i].PartitionResponses {
				res.Responses[i].PartitionResponses[j] = protocol.OffsetCommitPartitionResponse{
					Partition: req.Topics[i].Partitions[j].Partition,
					ErrorCode: protocol.ErrUnknown.Code(),
				}
			}
		}
		return res
	}

	// Calculate partition for this group
	partitionID := int32(util.Hash(req.GroupID) % uint64(len(topic.Partitions)))

	// Get the partition and replica
	_, p, err := state.GetPartition(OffsetsTopicName, partitionID)
	if err != nil || p == nil {
		log.Error.Printf("broker/%d: offset commit error getting partition: %v", b.config.ID, err)
		for i := range res.Responses {
			res.Responses[i] = protocol.OffsetCommitTopicResponse{
				Topic:              req.Topics[i].Topic,
				PartitionResponses: make([]protocol.OffsetCommitPartitionResponse, len(req.Topics[i].Partitions)),
			}
			for j := range res.Responses[i].PartitionResponses {
				res.Responses[i].PartitionResponses[j] = protocol.OffsetCommitPartitionResponse{
					Partition: req.Topics[i].Partitions[j].Partition,
					ErrorCode: protocol.ErrUnknown.Code(),
				}
			}
		}
		return res
	}

	// Check if this broker is the leader for the offsets partition
	if p.Leader != b.config.ID {
		log.Debug.Printf("broker/%d: offset commit - not leader, leader is %d", b.config.ID, p.Leader)
		for i := range res.Responses {
			res.Responses[i] = protocol.OffsetCommitTopicResponse{
				Topic:              req.Topics[i].Topic,
				PartitionResponses: make([]protocol.OffsetCommitPartitionResponse, len(req.Topics[i].Partitions)),
			}
			for j := range res.Responses[i].PartitionResponses {
				res.Responses[i].PartitionResponses[j] = protocol.OffsetCommitPartitionResponse{
					Partition: req.Topics[i].Partitions[j].Partition,
					ErrorCode: protocol.ErrNotLeaderForPartition.Code(),
				}
			}
		}
		return res
	}

	// Validate generation ID if provided (version >= 1)
	if req.Version() >= 1 {
		_, group, err := state.GetGroup(req.GroupID)
		if err != nil {
			log.Error.Printf("broker/%d: offset commit error getting group: %v", b.config.ID, err)
		} else if group != nil && req.GenerationID >= 0 && req.GenerationID != group.GenerationID {
			for i := range res.Responses {
				res.Responses[i] = protocol.OffsetCommitTopicResponse{
					Topic:              req.Topics[i].Topic,
					PartitionResponses: make([]protocol.OffsetCommitPartitionResponse, len(req.Topics[i].Partitions)),
				}
				for j := range res.Responses[i].PartitionResponses {
					res.Responses[i].PartitionResponses[j] = protocol.OffsetCommitPartitionResponse{
						Partition: req.Topics[i].Partitions[j].Partition,
						ErrorCode: protocol.ErrIllegalGeneration.Code(),
					}
				}
			}
			return res
		}
	}

	// Get the replica for the offsets partition
	replica, err := b.replicaLookup.Replica(OffsetsTopicName, partitionID)
	if err != nil || replica == nil || replica.Log == nil {
		log.Error.Printf("broker/%d: offset commit error getting replica: %v", b.config.ID, err)
		for i := range res.Responses {
			res.Responses[i] = protocol.OffsetCommitTopicResponse{
				Topic:              req.Topics[i].Topic,
				PartitionResponses: make([]protocol.OffsetCommitPartitionResponse, len(req.Topics[i].Partitions)),
			}
			for j := range res.Responses[i].PartitionResponses {
				res.Responses[i].PartitionResponses[j] = protocol.OffsetCommitPartitionResponse{
					Partition: req.Topics[i].Partitions[j].Partition,
					ErrorCode: protocol.ErrReplicaNotAvailable.Code(),
				}
			}
		}
		return res
	}

	// Process each topic
	for i, topicReq := range req.Topics {
		topicRes := protocol.OffsetCommitTopicResponse{
			Topic:              topicReq.Topic,
			PartitionResponses: make([]protocol.OffsetCommitPartitionResponse, len(topicReq.Partitions)),
		}

		for j, partReq := range topicReq.Partitions {
			partRes := protocol.OffsetCommitPartitionResponse{
				Partition: partReq.Partition,
			}

			// Use current time if timestamp not provided
			timestamp := partReq.Timestamp
			if timestamp == 0 {
				timestamp = time.Now().UnixNano() / int64(time.Millisecond)
			}

			// Encode the offset commit message
			msgData, err := EncodeOffsetCommitMessage(
				req.GroupID,
				topicReq.Topic,
				partReq.Partition,
				partReq.Offset,
				partReq.Metadata,
				timestamp,
			)
			if err != nil {
				log.Error.Printf("broker/%d: offset commit encode error: %v", b.config.ID, err)
				partRes.ErrorCode = protocol.ErrUnknown.Code()
				topicRes.PartitionResponses[j] = partRes
				continue
			}

			// Append to the offsets topic
			_, appendErr := replica.Log.Append(msgData)
			if appendErr != nil {
				log.Error.Printf("broker/%d: offset commit append error: %v", b.config.ID, appendErr)
				partRes.ErrorCode = protocol.ErrUnknown.Code()
			} else {
				partRes.ErrorCode = protocol.ErrNone.Code()
			}

			topicRes.PartitionResponses[j] = partRes
		}

		res.Responses[i] = topicRes
	}

	return res
}

func (b *Broker) handleOffsetFetch(ctx *Context, req *protocol.OffsetFetchRequest) *protocol.OffsetFetchResponse {
	sp := span(ctx, b.tracer, "offset fetch")
	defer sp.Finish()

	res := new(protocol.OffsetFetchResponse)
	res.APIVersion = req.Version()

	// Helper to build error response with all requested partitions
	buildErrorResponse := func(errorCode int16) *protocol.OffsetFetchResponse {
		res.Responses = make([]protocol.OffsetFetchTopicResponse, len(req.Topics))
		for i, topicReq := range req.Topics {
			topicRes := protocol.OffsetFetchTopicResponse{
				Topic:      topicReq.Topic,
				Partitions: make([]protocol.OffsetFetchPartition, len(topicReq.Partitions)),
			}
			for j, partitionID := range topicReq.Partitions {
				topicRes.Partitions[j] = protocol.OffsetFetchPartition{
					Partition: partitionID,
					Offset:    -1,
					Metadata:  nil,
					ErrorCode: errorCode,
				}
			}
			res.Responses[i] = topicRes
		}
		// Set top-level error code for v2+
		if res.APIVersion >= 2 {
			res.ErrorCode = errorCode
		}
		return res
	}

	// Validate coordinator
	state := b.fsm.State()
	topic, err := b.offsetsTopic(ctx)
	if err != nil {
		log.Error.Printf("broker/%d: offset fetch error getting offsets topic: %v", b.config.ID, err)
		return buildErrorResponse(protocol.ErrCoordinatorLoadInProgress.Code())
	}

	// Calculate partition for this group
	partitionID := int32(util.Hash(req.GroupID) % uint64(len(topic.Partitions)))

	// Get the partition
	_, p, err := state.GetPartition(OffsetsTopicName, partitionID)
	if err != nil || p == nil {
		log.Error.Printf("broker/%d: offset fetch error getting partition: %v", b.config.ID, err)
		return buildErrorResponse(protocol.ErrCoordinatorLoadInProgress.Code())
	}

	// Check if this broker is the leader
	if p.Leader != b.config.ID {
		log.Debug.Printf("broker/%d: offset fetch - not leader, leader is %d", b.config.ID, p.Leader)
		return buildErrorResponse(protocol.ErrNotCoordinator.Code())
	}

	// Get the replica
	replica, err := b.replicaLookup.Replica(OffsetsTopicName, partitionID)
	if err != nil || replica == nil || replica.Log == nil {
		log.Error.Printf("broker/%d: offset fetch error getting replica: %v", b.config.ID, err)
		return buildErrorResponse(protocol.ErrCoordinatorLoadInProgress.Code())
	}

	// Read all offset commit messages from the offsets topic partition
	// Start from the oldest offset
	oldestOffset := replica.Log.OldestOffset()
	reader, err := replica.Log.NewReader(oldestOffset, 10*1024*1024) // Read up to 10MB
	if err != nil {
		log.Error.Printf("broker/%d: offset fetch error creating reader: %v", b.config.ID, err)
		return buildErrorResponse(protocol.ErrCoordinatorLoadInProgress.Code())
	}

	// Read and parse offset commit messages
	offsetsMap := make(map[string]map[int32]*OffsetCommitValue) // topic -> partition -> value

	// Read message sets sequentially
	for {
		// Read message set header (offset 8 bytes + size 4 bytes)
		header := make([]byte, 12)
		n, err := io.ReadFull(reader, header)
		if err == io.EOF {
			break
		}
		if err != nil || n < 12 {
			break
		}

		// Get the size of the message set payload (commitlog format)
		// The commitlog stores: offset (8) + size (4) + payload
		// But we stored a protocol MessageSet, so the payload IS a protocol MessageSet
		payloadSize := int32(binary.BigEndian.Uint32(header[8:12]))
		if payloadSize <= 0 {
			break
		}

		// Read the payload (this is a protocol MessageSet)
		payload := make([]byte, payloadSize)
		n, err = io.ReadFull(reader, payload)
		if err == io.EOF && n == 0 {
			break
		}
		if err != nil && err != io.EOF {
			break
		}
		if n < int(payloadSize) {
			// Partial read, skip
			break
		}

		// Decode the protocol MessageSet
		decoder := protocol.NewDecoder(payload)
		msgSet := &protocol.MessageSet{}
		if err := msgSet.Decode(decoder); err != nil {
			// Skip invalid message sets
			continue
		}

		// Process each message in the message set
		for _, msg := range msgSet.Messages {
			// Decode the offset commit message
			key, value, decodeErr := DecodeOffsetCommitMessageFromProtocol(msg)
			if decodeErr == nil && key != nil && value != nil {
				// Only process messages for this group
				if key.GroupID == req.GroupID {
					if offsetsMap[key.Topic] == nil {
						offsetsMap[key.Topic] = make(map[int32]*OffsetCommitValue)
					}
					// Keep the latest offset (highest timestamp)
					if existing, exists := offsetsMap[key.Topic][key.Partition]; !exists || value.Timestamp > existing.Timestamp {
						offsetsMap[key.Topic][key.Partition] = value
					}
				}
			}
		}
	}

	// Build response
	if len(req.Topics) == 0 {
		// Return all topics for this group
		res.Responses = make([]protocol.OffsetFetchTopicResponse, 0, len(offsetsMap))
		for topicName, partitions := range offsetsMap {
			topicRes := protocol.OffsetFetchTopicResponse{
				Topic:      topicName,
				Partitions: make([]protocol.OffsetFetchPartition, 0, len(partitions)),
			}
			for partitionID, value := range partitions {
				topicRes.Partitions = append(topicRes.Partitions, protocol.OffsetFetchPartition{
					Partition: partitionID,
					Offset:    value.Offset,
					Metadata:  value.Metadata,
					ErrorCode: protocol.ErrNone.Code(),
				})
			}
			res.Responses = append(res.Responses, topicRes)
		}
	} else {
		// Return only requested topics
		res.Responses = make([]protocol.OffsetFetchTopicResponse, len(req.Topics))
		for i, topicReq := range req.Topics {
			topicRes := protocol.OffsetFetchTopicResponse{
				Topic:      topicReq.Topic,
				Partitions: make([]protocol.OffsetFetchPartition, 0),
			}

			if partitions, exists := offsetsMap[topicReq.Topic]; exists {
				if len(topicReq.Partitions) == 0 {
					// Return all partitions for this topic
					for partitionID, value := range partitions {
						topicRes.Partitions = append(topicRes.Partitions, protocol.OffsetFetchPartition{
							Partition: partitionID,
							Offset:    value.Offset,
							Metadata:  value.Metadata,
							ErrorCode: protocol.ErrNone.Code(),
						})
					}
				} else {
					// Return only requested partitions
					for _, partitionID := range topicReq.Partitions {
						if value, exists := partitions[partitionID]; exists {
							topicRes.Partitions = append(topicRes.Partitions, protocol.OffsetFetchPartition{
								Partition: partitionID,
								Offset:    value.Offset,
								Metadata:  value.Metadata,
								ErrorCode: protocol.ErrNone.Code(),
							})
						} else {
							// Partition not found
							topicRes.Partitions = append(topicRes.Partitions, protocol.OffsetFetchPartition{
								Partition: partitionID,
								Offset:    -1,
								Metadata:  nil,
								ErrorCode: protocol.ErrNone.Code(), // No offset committed yet
							})
						}
					}
				}
			} else {
				// Topic not found, return empty partitions with no offset
				if len(topicReq.Partitions) == 0 {
					// No partitions specified, return empty
				} else {
					for _, partitionID := range topicReq.Partitions {
						topicRes.Partitions = append(topicRes.Partitions, protocol.OffsetFetchPartition{
							Partition: partitionID,
							Offset:    -1,
							Metadata:  nil,
							ErrorCode: protocol.ErrNone.Code(),
						})
					}
				}
			}

			res.Responses[i] = topicRes
		}
	}

	return res
}

// isController returns true if this is the cluster controller.
func (b *Broker) isController() bool {
	return b.isLeader()
}

func (b *Broker) isLeader() bool {
	return b.raft.State() == raft.Leader
}

// createPartition is used to add a partition across the cluster.
func (b *Broker) createPartition(partition structs.Partition) error {
	_, err := b.raftApply(structs.RegisterPartitionRequestType, structs.RegisterPartitionRequest{
		Partition: partition,
	})
	return err
}

// startReplica is used to start a replica on this, including creating its commit log.
func (b *Broker) startReplica(replica *Replica) protocol.Error {
	b.Lock()
	defer b.Unlock()

	state := b.fsm.State()
	_, topic, _ := state.GetTopic(replica.Partition.Topic)

	// TODO: think i need to just ensure/add the topic if it's not here yet

	if topic == nil {
		log.Info.Printf("broker/%d: start replica called on unknown topic: %s", b.config.ID, replica.Partition.Topic)
		return protocol.ErrUnknownTopicOrPartition
	}

	if replica.Log == nil {
		log, err := commitlog.New(commitlog.Options{
			Path:            filepath.Join(b.config.DataDir, "data", fmt.Sprintf("%d", replica.Partition.ID)),
			MaxSegmentBytes: 1024,
			MaxLogBytes:     -1,
			CleanupPolicy:   commitlog.CleanupPolicy(topic.Config.GetValue("cleanup.policy").(string)),
		})
		if err != nil {
			return protocol.ErrUnknown.WithErr(err)
		}
		replica.Log = log
		// TODO: register leader-change listener on r.replica.Partition.id
	}

	return protocol.ErrNone
}

// createTopic is used to create the topic across the cluster.
func (b *Broker) createTopic(ctx *Context, topic *protocol.CreateTopicRequest) protocol.Error {
	state := b.fsm.State()
	_, t, _ := state.GetTopic(topic.Topic)
	if t != nil {
		return protocol.ErrTopicAlreadyExists
	}
	ps, err := b.buildPartitions(topic.Topic, topic.NumPartitions, topic.ReplicationFactor)
	if err != protocol.ErrNone {
		return err
	}
	tt := structs.Topic{
		Topic:      topic.Topic,
		Partitions: make(map[int32][]int32),
	}
	for _, partition := range ps {
		tt.Partitions[partition.ID] = partition.AR
	}
	// TODO: create/set topic config here
	if _, err := b.raftApply(structs.RegisterTopicRequestType, structs.RegisterTopicRequest{Topic: tt}); err != nil {
		return protocol.ErrUnknown.WithErr(err)
	}
	for _, partition := range ps {
		if err := b.createPartition(partition); err != nil {
			return protocol.ErrUnknown.WithErr(err)
		}
	}
	// could move this up maybe and do the iteration once
	req := &protocol.LeaderAndISRRequest{
		ControllerID: b.config.ID,
		// TODO ControllerEpoch
		PartitionStates: make([]*protocol.PartitionState, 0, len(ps)),
	}
	for _, partition := range ps {
		req.PartitionStates = append(req.PartitionStates, &protocol.PartitionState{
			Topic:     partition.Topic,
			Partition: partition.ID,
			// TODO: ControllerEpoch, LeaderEpoch, ZKVersion
			Leader:   partition.Leader,
			ISR:      partition.ISR,
			Replicas: partition.AR,
		})
	}
	// TODO: can optimize this
	for _, broker := range b.brokerLookup.Brokers() {
		if broker.ID.Int32() == b.config.ID {
			errCode := b.handleLeaderAndISR(ctx, req).ErrorCode
			if protocol.ErrNone.Code() != errCode {
				log.Error.Printf("broker/%d: handling leader and isr error: %d", b.config.ID, errCode)
			}
		} else {
			conn, err := Dial("tcp", broker.BrokerAddr)
			if err != nil {
				return protocol.ErrUnknown.WithErr(err)
			}
			res, err := conn.LeaderAndISR(req)
			if err != nil {
				// handle err and responses
				return protocol.ErrUnknown.WithErr(err)
			}
			log.Debug.Printf("broker/%d: leader and isr response: %+v", b.config.ID, res)
		}
	}
	return protocol.ErrNone
}

func (b *Broker) buildPartitions(topic string, partitionsCount int32, replicationFactor int16) ([]structs.Partition, protocol.Error) {
	brokers := b.brokerLookup.Brokers()
	count := len(brokers)

	if int(replicationFactor) > count {
		return nil, protocol.ErrInvalidReplicationFactor
	}

	// container/ring is dope af
	r := ring.New(count)
	for i := 0; i < r.Len(); i++ {
		r.Value = brokers[i]
		r = r.Next()
	}

	var partitions []structs.Partition

	for i := int32(0); i < partitionsCount; i++ {
		// TODO: maybe just go next here too
		r = r.Move(rand.Intn(count))
		leader := r.Value.(*metadata.Broker)
		replicas := []int32{leader.ID.Int32()}
		for i := int16(0); i < replicationFactor-1; i++ {
			r = r.Next()
			replicas = append(replicas, r.Value.(*metadata.Broker).ID.Int32())
		}
		partition := structs.Partition{
			Topic:     topic,
			ID:        i,
			Partition: i,
			Leader:    leader.ID.Int32(),
			AR:        replicas,
			ISR:       replicas,
		}
		partitions = append(partitions, partition)
	}

	return partitions, protocol.ErrNone
}

// Leave is used to prepare for a graceful shutdown.
func (b *Broker) Leave() error {
	log.Info.Printf("broker/%d: starting leave", b.config.ID)

	numPeers, err := b.numPeers()
	if err != nil {
		log.Error.Printf("broker/%d: check raft peers error: %s", b.config.ID, err)
		return err
	}

	isLeader := b.isLeader()
	if isLeader && numPeers > 1 {
		future := b.raft.RemoveServer(raft.ServerID(fmt.Sprintf("%d", b.config.ID)), 0, 0)
		if err := future.Error(); err != nil {
			log.Error.Printf("broker/%d: remove ourself as raft peer error: %s", b.config.ID, err)
		}
	}

	if b.serf != nil {
		if err := b.serf.Leave(); err != nil {
			log.Error.Printf("broker/%d: leave LAN serf cluster error: %s", b.config.ID, err)
		}
	}

	time.Sleep(b.config.LeaveDrainTime)

	if !isLeader {
		left := false
		limit := time.Now().Add(5 * time.Second)
		for !left && time.Now().Before(limit) {
			// Sleep a while before we check.
			time.Sleep(50 * time.Millisecond)

			// Get the latest configuration.
			future := b.raft.GetConfiguration()
			if err := future.Error(); err != nil {
				log.Error.Printf("broker/%d: get raft configuration error: %s", b.config.ID, err)
				break
			}

			// See if we are no longer included.
			left = true
			for _, server := range future.Configuration().Servers {
				if server.Address == raft.ServerAddress(b.config.RaftAddr) {
					left = false
					break
				}
			}
		}
	}

	return nil
}

// Shutdown is used to shutdown the broker, its serf, its raft, and so on.
func (b *Broker) Shutdown() error {
	log.Info.Printf("broker/%d: shutting down broker", b.config.ID)
	b.shutdownLock.Lock()
	defer b.shutdownLock.Unlock()

	if b.shutdown {
		return nil
	}
	b.shutdown = true
	close(b.shutdownCh)

	// Cancel the broker context to stop all background operations
	if b.cancel != nil {
		b.cancel()
	}

	if b.serf != nil {
		b.serf.Shutdown()
	}

	if b.raft != nil {
		b.raftTransport.Close()
		future := b.raft.Shutdown()
		if err := future.Error(); err != nil {
			log.Error.Printf("broker/%d: shutdown error: %s", b.config.ID, err)
		}
		if b.raftStore != nil {
			b.raftStore.Close()
		}
	}

	return nil
}

// Replication.

func (b *Broker) becomeFollower(replica *Replica, cmd *protocol.PartitionState) protocol.Error {
	// stop replicator to current leader
	b.Lock()
	defer b.Unlock()
	if replica.Replicator != nil {
		if err := replica.Replicator.Close(); err != nil {
			return protocol.ErrUnknown.WithErr(err)
		}
	}
	hw := replica.Log.NewestOffset()
	if err := replica.Log.Truncate(hw); err != nil {
		return protocol.ErrUnknown.WithErr(err)
	}
	broker := b.brokerLookup.BrokerByID(raft.ServerID(fmt.Sprintf("%d", cmd.Leader)))
	if broker == nil {
		return protocol.ErrBrokerNotAvailable
	}
	conn, err := NewDialer(fmt.Sprintf("quafka-replicator-%d", b.config.ID)).Dial("tcp", broker.BrokerAddr)
	if err != nil {
		return protocol.ErrUnknown.WithErr(err)
	}
	r := NewReplicator(ReplicatorConfig{}, replica, conn)
	replica.Replicator = r
	if !b.config.DevMode {
		r.Replicate(b.ctx)
	}
	return protocol.ErrNone
}

func (b *Broker) becomeLeader(replica *Replica, cmd *protocol.PartitionState) protocol.Error {
	b.Lock()
	defer b.Unlock()
	if replica.Replicator != nil {
		if err := replica.Replicator.Close(); err != nil {
			return protocol.ErrUnknown.WithErr(err)
		}
		replica.Replicator = nil
	}
	replica.Partition.Leader = cmd.Leader
	replica.Partition.AR = cmd.Replicas
	replica.Partition.ISR = cmd.ISR
	replica.Partition.LeaderEpoch = cmd.ZKVersion
	return protocol.ErrNone
}

func contains(rs []int32, r int32) bool {
	for _, ri := range rs {
		if ri == r {
			return true
		}
	}
	return false
}

// ensurePath is used to make sure a path exists
func ensurePath(path string, dir bool) error {
	if !dir {
		path = filepath.Dir(path)
	}
	return os.MkdirAll(path, 0755)
}

// Atomically sets a readiness state flag when leadership is obtained, to indicate that server is past its barrier write
func (b *Broker) setConsistentReadReady() {
	atomic.StoreInt32(&b.readyForConsistentReads, 1)
}

// Atomically reset readiness state flag on leadership revoke
func (b *Broker) resetConsistentReadReady() {
	atomic.StoreInt32(&b.readyForConsistentReads, 0)
}

// Returns true if this server is ready to serve consistent reads
func (b *Broker) isReadyForConsistentReads() bool {
	return atomic.LoadInt32(&b.readyForConsistentReads) == 1
}

func (b *Broker) numPeers() (int, error) {
	future := b.raft.GetConfiguration()
	if err := future.Error(); err != nil {
		return 0, err
	}
	raftConfig := future.Configuration()
	var numPeers int
	for _, server := range raftConfig.Servers {
		if server.Suffrage == raft.Voter {
			numPeers++
		}
	}
	return numPeers, nil
}

func (b *Broker) LANMembers() []serf.Member {
	return b.serf.Members()
}

// Replica
type Replica struct {
	BrokerID   int32
	Partition  structs.Partition
	IsLocal    bool
	Log        CommitLog
	Hw         int64
	Leo        int64
	Replicator *Replicator
	sync.Mutex
}

func (r *Replica) String() string {
	return fmt.Sprintf("replica: %d {broker: %d, leader: %d, hw: %d, leo: %d}", r.Partition.ID, r.BrokerID, r.Partition.Leader, r.Hw, r.Leo)
}

func (b *Broker) offsetsTopic(ctx *Context) (topic *structs.Topic, err error) {
	state := b.fsm.State()

	// check if the topic exists already
	_, topic, err = state.GetTopic(OffsetsTopicName)
	if err != nil {
		return
	}
	if topic != nil {
		return
	}

	// doesn't exist so let's create it
	numPartitions := b.config.OffsetsTopicNumPartitions
	if numPartitions <= 0 {
		numPartitions = int32(OffsetsTopicNumPartitions) // Use default from constant
	}
	partitions, err := b.buildPartitions(OffsetsTopicName, numPartitions, b.config.OffsetsTopicReplicationFactor)
	if err != protocol.ErrNone {
		return nil, err
	}
	topic = &structs.Topic{
		Topic:      OffsetsTopicName,
		Internal:   true,
		Partitions: make(map[int32][]int32),
	}
	for _, p := range partitions {
		topic.Partitions[p.Partition] = p.AR
	}
	_, err = b.raftApply(structs.RegisterTopicRequestType, structs.RegisterTopicRequest{
		Topic: *topic,
	})
	for _, partition := range partitions {
		if err := b.createPartition(partition); err != nil {
			return nil, err
		}
	}

	// Start replicas for the offsets topic partitions via LeaderAndISR
	// This ensures replicas are ready when FindCoordinator returns
	partitionStates := make([]*protocol.PartitionState, 0, len(partitions))
	for _, p := range partitions {
		partitionStates = append(partitionStates, &protocol.PartitionState{
			Topic:     OffsetsTopicName,
			Partition: p.Partition,
			Leader:    p.Leader,
			ISR:       p.ISR,
			Replicas:  p.AR,
		})
	}
	if len(partitionStates) > 0 {
		leaderAndISRReq := &protocol.LeaderAndISRRequest{
			ControllerID:    b.config.ID,
			PartitionStates: partitionStates,
		}
		res := b.handleLeaderAndISR(ctx, leaderAndISRReq)
		for _, pRes := range res.Partitions {
			if pRes.ErrorCode != protocol.ErrNone.Code() {
				log.Error.Printf("broker/%d: LeaderAndISR failed for offsets topic partition %d: %d", b.config.ID, pRes.Partition, pRes.ErrorCode)
			}
		}
	}

	return
}

// debugSnapshot takes a snapshot of this broker's state. Used to debug errors.
func (b *Broker) debugSnapshot() {

}

func (b *Broker) withTimeout(timeout time.Duration, fn func() protocol.Error) protocol.Error {
	if timeout <= 0 {
		go fn()
		return protocol.ErrNone
	}

	c := make(chan protocol.Error, 1)
	defer close(c)

	timer := time.NewTimer(timeout)
	defer timer.Stop()

	go func() {
		c <- fn()
	}()

	select {
	case err := <-c:
		return err
	case <-timer.C:
		return protocol.ErrRequestTimedOut
	}
}

func (b *Broker) logState() {
	t := time.NewTicker(b.logStateInterval)
	for {
		select {
		case <-b.shutdownCh:
			return
		case <-t.C:
			var buf bytes.Buffer
			buf.WriteString("\tmembers:\n")
			members := b.LANMembers()
			for i, member := range members {
				buf.WriteString(fmt.Sprintf("\t\t- %d:\n\t\t\tname: %s\n\t\t\taddr: %s\n\t\t\tstatus: %s\n", i, member.Name, member.Addr, member.Status))
			}
			buf.WriteString("\tnodes:\n")
			state := b.fsm.State()
			_, nodes, err := state.GetNodes()
			if err != nil {
				log.Error.Printf("broker/%d: logState: failed to get nodes: %v", b.config.ID, err)
				continue
			}
			for i, node := range nodes {
				buf.WriteString(fmt.Sprintf("\t\t- %d:\n\t\t\tid: %d\n\t\t\tstatus: %s\n", i, node.Node, node.Check.Status))
			}
			_, topics, err := state.GetTopics()
			if err != nil {
				log.Error.Printf("broker/%d: logState: failed to get topics: %v", b.config.ID, err)
				continue
			}
			buf.WriteString("\ttopics:\n")
			for i, topic := range topics {
				buf.WriteString(fmt.Sprintf("\t\t- %d:\n\t\t\tid: %s\n\t\t\tpartitions: %v\n", i, topic.Topic, topic.Partitions))
			}
			log.Info.Printf("broker/%d: state:\n%s", b.config.ID, buf.String())
		}
	}
}
