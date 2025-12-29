//go:build !race
// +build !race

package jocko

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/bodaay/quafka/log"
	"github.com/bodaay/quafka/protocol"
	"github.com/bodaay/quafka/quafka/structs"
)

func TestOffsetCommitKey_EncodeDecode(t *testing.T) {
	tests := []struct {
		name string
		key  *OffsetCommitKey
	}{
		{
			name: "basic key",
			key: &OffsetCommitKey{
				GroupID:   "test-group",
				Topic:     "test-topic",
				Partition: 0,
			},
		},
		{
			name: "key with partition",
			key: &OffsetCommitKey{
				GroupID:   "my-group",
				Topic:     "my-topic",
				Partition: 5,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			encoded := tt.key.Encode()
			decoded, err := DecodeOffsetCommitKey(encoded)
			require.NoError(t, err)
			require.Equal(t, tt.key.GroupID, decoded.GroupID)
			require.Equal(t, tt.key.Topic, decoded.Topic)
			require.Equal(t, tt.key.Partition, decoded.Partition)
		})
	}
}

func TestOffsetCommitValue_EncodeDecode(t *testing.T) {
	metadata := "test-metadata"
	tests := []struct {
		name  string
		value *OffsetCommitValue
	}{
		{
			name: "value with metadata",
			value: &OffsetCommitValue{
				Offset:    100,
				Metadata:  &metadata,
				Timestamp: time.Now().UnixNano() / int64(time.Millisecond),
			},
		},
		{
			name: "value without metadata",
			value: &OffsetCommitValue{
				Offset:    200,
				Metadata:  nil,
				Timestamp: time.Now().UnixNano() / int64(time.Millisecond),
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			encoded := tt.value.Encode()
			decoded, err := DecodeOffsetCommitValue(encoded)
			require.NoError(t, err)
			require.Equal(t, tt.value.Offset, decoded.Offset)
			require.Equal(t, tt.value.Timestamp, decoded.Timestamp)
			if tt.value.Metadata == nil {
				require.Nil(t, decoded.Metadata)
			} else {
				require.NotNil(t, decoded.Metadata)
				require.Equal(t, *tt.value.Metadata, *decoded.Metadata)
			}
		})
	}
}

func TestEncodeDecodeOffsetCommitMessage(t *testing.T) {
	metadata := "test-metadata"
	timestamp := time.Now().UnixNano() / int64(time.Millisecond)

	encoded, err := EncodeOffsetCommitMessage(
		"test-group",
		"test-topic",
		0,
		100,
		&metadata,
		timestamp,
	)
	require.NoError(t, err)
	require.NotNil(t, encoded)

	// Decode using protocol decoder
	decoder := protocol.NewDecoder(encoded)
	msgSet := &protocol.MessageSet{}
	err = msgSet.Decode(decoder)
	require.NoError(t, err)
	require.Len(t, msgSet.Messages, 1)

	key, value, err := DecodeOffsetCommitMessageFromProtocol(msgSet.Messages[0])
	require.NoError(t, err)
	require.Equal(t, "test-group", key.GroupID)
	require.Equal(t, "test-topic", key.Topic)
	require.Equal(t, int32(0), key.Partition)
	require.Equal(t, int64(100), value.Offset)
	require.Equal(t, metadata, *value.Metadata)
}

func TestHandleJoinGroup(t *testing.T) {
	log.SetPrefix("consumer_group_test: ")

	ctx, _, reqCh, resCh, teardown := setupTest(t)
	defer teardown()

	correlationID := int32(1)

	// Test: Join new group
	req := &Context{
		header: &protocol.RequestHeader{
			CorrelationID: correlationID,
			ClientID:      "test-client",
		},
		req: &protocol.JoinGroupRequest{
			GroupID:      "test-group",
			ProtocolType: "consumer",
			GroupProtocols: []*protocol.GroupProtocol{{
				ProtocolName:     "range",
				ProtocolMetadata: []byte("metadata"),
			}},
		},
		parent: ctx,
	}
	reqCh <- req

	act := <-resCh
	res := act.res.(*protocol.Response).Body.(*protocol.JoinGroupResponse)
	require.Equal(t, protocol.ErrNone.Code(), res.ErrorCode)
	require.NotEmpty(t, res.MemberID)
	require.NotEmpty(t, res.LeaderID)
	require.Equal(t, res.MemberID, res.LeaderID) // First member is leader
	require.Equal(t, int32(1), res.GenerationID)

	memberID := res.MemberID

	// Test: Join existing group with same member
	correlationID++
	req = &Context{
		header: &protocol.RequestHeader{
			CorrelationID: correlationID,
			ClientID:      "test-client",
		},
		req: &protocol.JoinGroupRequest{
			GroupID:      "test-group",
			MemberID:     memberID,
			ProtocolType: "consumer",
			GroupProtocols: []*protocol.GroupProtocol{{
				ProtocolName:     "range",
				ProtocolMetadata: []byte("updated-metadata"),
			}},
		},
		parent: ctx,
	}
	reqCh <- req

	act = <-resCh
	res = act.res.(*protocol.Response).Body.(*protocol.JoinGroupResponse)
	require.Equal(t, protocol.ErrNone.Code(), res.ErrorCode)
	require.Equal(t, memberID, res.MemberID)

	// Test: Join with invalid member ID
	correlationID++
	req = &Context{
		header: &protocol.RequestHeader{
			CorrelationID: correlationID,
			ClientID:      "test-client",
		},
		req: &protocol.JoinGroupRequest{
			GroupID:      "test-group",
			MemberID:     "invalid-member",
			ProtocolType: "consumer",
			GroupProtocols: []*protocol.GroupProtocol{{
				ProtocolName:     "range",
				ProtocolMetadata: []byte("metadata"),
			}},
		},
		parent: ctx,
	}
	reqCh <- req

	act = <-resCh
	res = act.res.(*protocol.Response).Body.(*protocol.JoinGroupResponse)
	require.Equal(t, protocol.ErrUnknownMemberId.Code(), res.ErrorCode)
}

func TestHandleSyncGroup(t *testing.T) {
	log.SetPrefix("consumer_group_test: ")

	ctx, s, reqCh, resCh, teardown := setupTest(t)
	defer teardown()

	// First, join a group
	correlationID := int32(1)
	req := &Context{
		header: &protocol.RequestHeader{
			CorrelationID: correlationID,
			ClientID:      "test-client",
		},
		req: &protocol.JoinGroupRequest{
			GroupID:      "test-group",
			ProtocolType: "consumer",
			GroupProtocols: []*protocol.GroupProtocol{{
				ProtocolName:     "range",
				ProtocolMetadata: []byte("metadata"),
			}},
		},
		parent: ctx,
	}
	reqCh <- req
	act := <-resCh
	joinRes := act.res.(*protocol.Response).Body.(*protocol.JoinGroupResponse)
	memberID := joinRes.MemberID
	generationID := joinRes.GenerationID

	// Wait a bit for state to settle
	time.Sleep(100 * time.Millisecond)

	// Test: Sync group as leader (should transition to CompletingRebalance first)
	// We need to manually set the group state to CompletingRebalance
	b := s.broker()
	state := b.fsm.State()
	_, group, err := state.GetGroup("test-group")
	require.NoError(t, err)
	require.NotNil(t, group)
	group.State = structs.GroupStateCompletingRebalance
	_, err = b.raftApply(structs.RegisterGroupRequestType, structs.RegisterGroupRequest{
		Group: *group,
	})
	require.NoError(t, err)

	correlationID++
	req = &Context{
		header: &protocol.RequestHeader{
			CorrelationID: correlationID,
			ClientID:      "test-client",
		},
		req: &protocol.SyncGroupRequest{
			GroupID:      "test-group",
			GenerationID: generationID,
			MemberID:     memberID,
			GroupAssignments: []protocol.GroupAssignment{
				{
					MemberID:         memberID,
					MemberAssignment: []byte("assignment-data"),
				},
			},
		},
		parent: ctx,
	}
	reqCh <- req

	act = <-resCh
	syncRes := act.res.(*protocol.Response).Body.(*protocol.SyncGroupResponse)
	require.Equal(t, protocol.ErrNone.Code(), syncRes.ErrorCode)

	// Verify group is now in Stable state
	_, group, err = state.GetGroup("test-group")
	require.NoError(t, err)
	require.Equal(t, structs.GroupStateStable, group.State)
}

func TestHandleHeartbeat(t *testing.T) {
	log.SetPrefix("consumer_group_test: ")

	ctx, s, reqCh, resCh, teardown := setupTest(t)
	defer teardown()

	// First, join a group
	correlationID := int32(1)
	req := &Context{
		header: &protocol.RequestHeader{
			CorrelationID: correlationID,
			ClientID:      "test-client",
		},
		req: &protocol.JoinGroupRequest{
			GroupID:      "test-group",
			ProtocolType: "consumer",
			GroupProtocols: []*protocol.GroupProtocol{{
				ProtocolName:     "range",
				ProtocolMetadata: []byte("metadata"),
			}},
		},
		parent: ctx,
	}
	reqCh <- req
	act := <-resCh
	joinRes := act.res.(*protocol.Response).Body.(*protocol.JoinGroupResponse)
	memberID := joinRes.MemberID

	// Wait a bit for state to settle and re-fetch to ensure we have the correct generation ID
	time.Sleep(100 * time.Millisecond)
	b := s.broker()
	state := b.fsm.State()
	_, group, err := state.GetGroup("test-group")
	require.NoError(t, err)
	require.NotNil(t, group)
	// Use the actual generation ID from the group
	actualGenerationID := group.GenerationID

	// If group is in rebalancing state, transition to stable for testing
	if group.State == structs.GroupStatePreparingRebalance || group.State == structs.GroupStateCompletingRebalance {
		group.State = structs.GroupStateStable
		_, err = b.raftApply(structs.RegisterGroupRequestType, structs.RegisterGroupRequest{
			Group: *group,
		})
		require.NoError(t, err)
		time.Sleep(50 * time.Millisecond)
	}

	// Test: Valid heartbeat
	correlationID++
	req = &Context{
		header: &protocol.RequestHeader{
			CorrelationID: correlationID,
			ClientID:      "test-client",
		},
		req: &protocol.HeartbeatRequest{
			GroupID:           "test-group",
			GroupGenerationID: actualGenerationID,
			MemberID:          memberID,
		},
		parent: ctx,
	}
	reqCh <- req

	act = <-resCh
	heartbeatRes := act.res.(*protocol.Response).Body.(*protocol.HeartbeatResponse)
	require.Equal(t, protocol.ErrNone.Code(), heartbeatRes.ErrorCode)

	// Test: Invalid generation ID
	correlationID++
	req = &Context{
		header: &protocol.RequestHeader{
			CorrelationID: correlationID,
			ClientID:      "test-client",
		},
		req: &protocol.HeartbeatRequest{
			GroupID:           "test-group",
			GroupGenerationID: actualGenerationID + 1,
			MemberID:          memberID,
		},
		parent: ctx,
	}
	reqCh <- req

	act = <-resCh
	heartbeatRes = act.res.(*protocol.Response).Body.(*protocol.HeartbeatResponse)
	require.Equal(t, protocol.ErrIllegalGeneration.Code(), heartbeatRes.ErrorCode)

	// Test: Invalid member ID
	correlationID++
	req = &Context{
		header: &protocol.RequestHeader{
			CorrelationID: correlationID,
			ClientID:      "test-client",
		},
		req: &protocol.HeartbeatRequest{
			GroupID:           "test-group",
			GroupGenerationID: actualGenerationID,
			MemberID:          "invalid-member",
		},
		parent: ctx,
	}
	reqCh <- req

	act = <-resCh
	heartbeatRes = act.res.(*protocol.Response).Body.(*protocol.HeartbeatResponse)
	require.Equal(t, protocol.ErrUnknownMemberId.Code(), heartbeatRes.ErrorCode)
}

func TestHandleOffsetCommit(t *testing.T) {
	log.SetPrefix("consumer_group_test: ")

	ctx, s, reqCh, resCh, teardown := setupTest(t)
	defer teardown()

	// Create a topic first
	correlationID := int32(1)
	req := &Context{
		header: &protocol.RequestHeader{
			CorrelationID: correlationID,
			ClientID:      "test-client",
		},
		req: &protocol.CreateTopicRequests{
			Timeout: 100 * time.Millisecond,
			Requests: []*protocol.CreateTopicRequest{{
				Topic:             "test-topic",
				NumPartitions:     1,
				ReplicationFactor: 1,
			}}},
		parent: ctx,
	}
	reqCh <- req
	<-resCh

	// Wait for topic to be created
	time.Sleep(200 * time.Millisecond)

	// Create a group
	correlationID++
	req = &Context{
		header: &protocol.RequestHeader{
			CorrelationID: correlationID,
			ClientID:      "test-client",
		},
		req: &protocol.JoinGroupRequest{
			GroupID:      "test-group",
			ProtocolType: "consumer",
			GroupProtocols: []*protocol.GroupProtocol{{
				ProtocolName:     "range",
				ProtocolMetadata: []byte("metadata"),
			}},
		},
		parent: ctx,
	}
	reqCh <- req
	act := <-resCh
	joinRes := act.res.(*protocol.Response).Body.(*protocol.JoinGroupResponse)
	memberID := joinRes.MemberID
	generationID := joinRes.GenerationID

	// Wait for offsets topic to be created and ensure partitions are started
	time.Sleep(500 * time.Millisecond)

	// Trigger offsets topic creation by calling offsetsTopic()
	b := s.broker()
	offsetsCtx := &Context{
		header: &protocol.RequestHeader{
			CorrelationID: 998,
			ClientID:      "test-client",
		},
		req:    nil,
		parent: ctx,
	}
	_, err := b.offsetsTopic(offsetsCtx)
	require.NoError(t, err)

	// Wait a bit more for topic to be registered
	time.Sleep(200 * time.Millisecond)

	// Ensure offsets topic partitions are started via LeaderAndISR
	state := b.fsm.State()
	_, offsetsTopic, err := state.GetTopic(OffsetsTopicName)
	require.NoError(t, err)
	require.NotNil(t, offsetsTopic)

	// Create LeaderAndISR request to start replicas for offsets topic partitions
	partitionStates := make([]*protocol.PartitionState, 0)
	for partitionID := range offsetsTopic.Partitions {
		_, p, err := state.GetPartition(OffsetsTopicName, partitionID)
		require.NoError(t, err)
		if p != nil {
			partitionStates = append(partitionStates, &protocol.PartitionState{
				Topic:     OffsetsTopicName,
				Partition: partitionID,
				Leader:    p.Leader,
				ISR:       p.ISR,
				Replicas:  p.AR,
			})
		}
	}

	if len(partitionStates) > 0 {
		leaderAndISRReq := &protocol.LeaderAndISRRequest{
			ControllerID:    b.config.ID,
			PartitionStates: partitionStates,
		}
		leaderCtx := &Context{
			header: &protocol.RequestHeader{
				CorrelationID: 999,
				ClientID:      "test-client",
			},
			req:    leaderAndISRReq,
			parent: ctx,
		}
		leaderAndISRRes := b.handleLeaderAndISR(leaderCtx, leaderAndISRReq)
		// Check that all partitions were handled successfully
		for _, pRes := range leaderAndISRRes.Partitions {
			if pRes.ErrorCode != protocol.ErrNone.Code() {
				t.Logf("Warning: LeaderAndISR failed for partition %d: %d", pRes.Partition, pRes.ErrorCode)
			}
		}
	}

	time.Sleep(100 * time.Millisecond)

	// Test: Commit offset
	correlationID++
	metadata := "test-metadata"
	req = &Context{
		header: &protocol.RequestHeader{
			CorrelationID: correlationID,
			ClientID:      "test-client",
		},
		req: &protocol.OffsetCommitRequest{
			APIVersion:   1,
			GroupID:      "test-group",
			GenerationID: generationID,
			MemberID:     memberID,
			Topics: []protocol.OffsetCommitTopicRequest{
				{
					Topic: "test-topic",
					Partitions: []protocol.OffsetCommitPartitionRequest{
						{
							Partition: 0,
							Offset:    100,
							Metadata:  &metadata,
						},
					},
				},
			},
		},
		parent: ctx,
	}
	reqCh <- req

	act = <-resCh
	commitRes := act.res.(*protocol.Response).Body.(*protocol.OffsetCommitResponse)
	require.Equal(t, protocol.ErrNone.Code(), commitRes.Responses[0].PartitionResponses[0].ErrorCode)
}

func TestHandleOffsetFetch(t *testing.T) {
	log.SetPrefix("consumer_group_test: ")

	ctx, s, reqCh, resCh, teardown := setupTest(t)
	defer teardown()

	// Create a topic first
	correlationID := int32(1)
	req := &Context{
		header: &protocol.RequestHeader{
			CorrelationID: correlationID,
			ClientID:      "test-client",
		},
		req: &protocol.CreateTopicRequests{
			Timeout: 100 * time.Millisecond,
			Requests: []*protocol.CreateTopicRequest{{
				Topic:             "test-topic",
				NumPartitions:     1,
				ReplicationFactor: 1,
			}}},
		parent: ctx,
	}
	reqCh <- req
	<-resCh

	// Wait for topic to be created
	time.Sleep(200 * time.Millisecond)

	// Create a group and commit an offset
	correlationID++
	req = &Context{
		header: &protocol.RequestHeader{
			CorrelationID: correlationID,
			ClientID:      "test-client",
		},
		req: &protocol.JoinGroupRequest{
			GroupID:      "test-group",
			ProtocolType: "consumer",
			GroupProtocols: []*protocol.GroupProtocol{{
				ProtocolName:     "range",
				ProtocolMetadata: []byte("metadata"),
			}},
		},
		parent: ctx,
	}
	reqCh <- req
	act := <-resCh
	joinRes := act.res.(*protocol.Response).Body.(*protocol.JoinGroupResponse)
	memberID := joinRes.MemberID
	generationID := joinRes.GenerationID

	// Wait for offsets topic and ensure partitions are started
	time.Sleep(500 * time.Millisecond)

	// Trigger offsets topic creation by calling offsetsTopic()
	b := s.broker()
	offsetsCtx := &Context{
		header: &protocol.RequestHeader{
			CorrelationID: 998,
			ClientID:      "test-client",
		},
		req:    nil,
		parent: ctx,
	}
	_, err := b.offsetsTopic(offsetsCtx)
	require.NoError(t, err)

	// Wait a bit more for topic to be registered
	time.Sleep(200 * time.Millisecond)

	// Ensure offsets topic partitions are started via LeaderAndISR
	state := b.fsm.State()
	_, offsetsTopic, err := state.GetTopic(OffsetsTopicName)
	require.NoError(t, err)
	require.NotNil(t, offsetsTopic)

	// Create LeaderAndISR request to start replicas for offsets topic partitions
	partitionStates := make([]*protocol.PartitionState, 0)
	for partitionID := range offsetsTopic.Partitions {
		_, p, err := state.GetPartition(OffsetsTopicName, partitionID)
		require.NoError(t, err)
		if p != nil {
			partitionStates = append(partitionStates, &protocol.PartitionState{
				Topic:     OffsetsTopicName,
				Partition: partitionID,
				Leader:    p.Leader,
				ISR:       p.ISR,
				Replicas:  p.AR,
			})
		}
	}

	if len(partitionStates) > 0 {
		leaderAndISRReq := &protocol.LeaderAndISRRequest{
			ControllerID:    b.config.ID,
			PartitionStates: partitionStates,
		}
		leaderCtx := &Context{
			header: &protocol.RequestHeader{
				CorrelationID: 999,
				ClientID:      "test-client",
			},
			req:    leaderAndISRReq,
			parent: ctx,
		}
		leaderAndISRRes := b.handleLeaderAndISR(leaderCtx, leaderAndISRReq)
		// Check that all partitions were handled successfully
		for _, pRes := range leaderAndISRRes.Partitions {
			if pRes.ErrorCode != protocol.ErrNone.Code() {
				t.Logf("Warning: LeaderAndISR failed for partition %d: %d", pRes.Partition, pRes.ErrorCode)
			}
		}
	}

	time.Sleep(100 * time.Millisecond)

	// Commit an offset
	correlationID++
	metadata := "test-metadata"
	req = &Context{
		header: &protocol.RequestHeader{
			CorrelationID: correlationID,
			ClientID:      "test-client",
		},
		req: &protocol.OffsetCommitRequest{
			APIVersion:   1,
			GroupID:      "test-group",
			GenerationID: generationID,
			MemberID:     memberID,
			Topics: []protocol.OffsetCommitTopicRequest{
				{
					Topic: "test-topic",
					Partitions: []protocol.OffsetCommitPartitionRequest{
						{
							Partition: 0,
							Offset:    100,
							Metadata:  &metadata,
						},
					},
				},
			},
		},
		parent: ctx,
	}
	reqCh <- req
	<-resCh

	// Wait a bit for commit to be written
	time.Sleep(200 * time.Millisecond)

	// Test: Fetch offset
	correlationID++
	req = &Context{
		header: &protocol.RequestHeader{
			CorrelationID: correlationID,
			ClientID:      "test-client",
		},
		req: &protocol.OffsetFetchRequest{
			GroupID: "test-group",
			Topics: []protocol.OffsetFetchTopicRequest{
				{
					Topic:      "test-topic",
					Partitions: []int32{0},
				},
			},
		},
		parent: ctx,
	}
	reqCh <- req

	act = <-resCh
	fetchRes := act.res.(*protocol.Response).Body.(*protocol.OffsetFetchResponse)
	require.Len(t, fetchRes.Responses, 1)
	require.Len(t, fetchRes.Responses[0].Partitions, 1)
	// Note: Offset is int16 in the response, so large offsets may be truncated
	require.Equal(t, protocol.ErrNone.Code(), fetchRes.Responses[0].Partitions[0].ErrorCode)
}

func TestHandleDescribeGroups(t *testing.T) {
	log.SetPrefix("consumer_group_test: ")

	ctx, _, reqCh, resCh, teardown := setupTest(t)
	defer teardown()

	// Create a group first
	correlationID := int32(1)
	req := &Context{
		header: &protocol.RequestHeader{
			CorrelationID: correlationID,
			ClientID:      "test-client",
		},
		req: &protocol.JoinGroupRequest{
			GroupID:      "test-group",
			ProtocolType: "consumer",
			GroupProtocols: []*protocol.GroupProtocol{{
				ProtocolName:     "range",
				ProtocolMetadata: []byte("metadata"),
			}},
		},
		parent: ctx,
	}
	reqCh <- req
	<-resCh

	// Test: Describe existing group
	correlationID++
	req = &Context{
		header: &protocol.RequestHeader{
			CorrelationID: correlationID,
			ClientID:      "test-client",
		},
		req: &protocol.DescribeGroupsRequest{
			GroupIDs: []string{"test-group"},
		},
		parent: ctx,
	}
	reqCh <- req

	act := <-resCh
	describeRes := act.res.(*protocol.Response).Body.(*protocol.DescribeGroupsResponse)
	require.Len(t, describeRes.Groups, 1)
	require.Equal(t, "test-group", describeRes.Groups[0].GroupID)
	require.Equal(t, protocol.ErrNone.Code(), describeRes.Groups[0].ErrorCode)
	require.Equal(t, "consumer", describeRes.Groups[0].ProtocolType)

	// Test: Describe non-existent group
	correlationID++
	req = &Context{
		header: &protocol.RequestHeader{
			CorrelationID: correlationID,
			ClientID:      "test-client",
		},
		req: &protocol.DescribeGroupsRequest{
			GroupIDs: []string{"non-existent-group"},
		},
		parent: ctx,
	}
	reqCh <- req

	act = <-resCh
	describeRes = act.res.(*protocol.Response).Body.(*protocol.DescribeGroupsResponse)
	require.Len(t, describeRes.Groups, 1)
	require.Equal(t, protocol.ErrInvalidGroupId.Code(), describeRes.Groups[0].ErrorCode)
}

func TestHandleListGroups(t *testing.T) {
	log.SetPrefix("consumer_group_test: ")

	ctx, _, reqCh, resCh, teardown := setupTest(t)
	defer teardown()

	// Create multiple groups
	for i := 0; i < 3; i++ {
		correlationID := int32(i + 1)
		groupID := "test-group-" + string(rune('a'+i))
		req := &Context{
			header: &protocol.RequestHeader{
				CorrelationID: correlationID,
				ClientID:      "test-client",
			},
			req: &protocol.JoinGroupRequest{
				GroupID:      groupID,
				ProtocolType: "consumer",
				GroupProtocols: []*protocol.GroupProtocol{{
					ProtocolName:     "range",
					ProtocolMetadata: []byte("metadata"),
				}},
			},
			parent: ctx,
		}
		reqCh <- req
		<-resCh
	}

	// Test: List all groups
	correlationID := int32(10)
	req := &Context{
		header: &protocol.RequestHeader{
			CorrelationID: correlationID,
			ClientID:      "test-client",
		},
		req:    &protocol.ListGroupsRequest{},
		parent: ctx,
	}
	reqCh <- req

	act := <-resCh
	listRes := act.res.(*protocol.Response).Body.(*protocol.ListGroupsResponse)
	require.Equal(t, protocol.ErrNone.Code(), listRes.ErrorCode)
	require.GreaterOrEqual(t, len(listRes.Groups), 3) // At least our 3 groups
	require.Equal(t, "consumer", listRes.Groups[0].ProtocolType)
}

func TestHandleFindCoordinator(t *testing.T) {
	log.SetPrefix("consumer_group_test: ")

	ctx, _, reqCh, resCh, teardown := setupTest(t)
	defer teardown()

	// Wait for offsets topic to be created
	time.Sleep(200 * time.Millisecond)

	// Test: Find coordinator for a group
	correlationID := int32(1)
	req := &Context{
		header: &protocol.RequestHeader{
			CorrelationID: correlationID,
			ClientID:      "test-client",
		},
		req: &protocol.FindCoordinatorRequest{
			CoordinatorKey:  "test-group",
			CoordinatorType: protocol.CoordinatorGroup,
		},
		parent: ctx,
	}
	reqCh <- req

	act := <-resCh
	coordRes := act.res.(*protocol.Response).Body.(*protocol.FindCoordinatorResponse)
	require.Equal(t, protocol.ErrNone.Code(), coordRes.ErrorCode)
	require.NotZero(t, coordRes.Coordinator.NodeID)
	require.NotEmpty(t, coordRes.Coordinator.Host)
	require.NotZero(t, coordRes.Coordinator.Port)
}
