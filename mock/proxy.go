package mock

import (
	"context"
	"strconv"

	"github.com/bodaay/quafka/protocol"
)

// Client for testing
type Client struct {
	msgCount int
	msgs     [][]byte
}

// NewClient is a client that fetches given number of msgs
func NewClient(msgCount int) *Client {
	return &Client{
		msgCount: msgCount,
	}
}

func (p *Client) Messages() [][]byte {
	return p.msgs
}

// FetchContext implements the context-aware Fetch for the replicator client interface.
func (p *Client) FetchContext(ctx context.Context, fetchRequest *protocol.FetchRequest) (*protocol.FetchResponse, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	if len(p.msgs) >= p.msgCount {
		return &protocol.FetchResponse{}, nil
	}
	msgs := [][]byte{
		[]byte("msg " + strconv.Itoa(len(p.msgs))),
	}
	response := &protocol.FetchResponse{
		Responses: protocol.FetchTopicResponses{{
			Topic: fetchRequest.Topics[0].Topic,
			PartitionResponses: []*protocol.FetchPartitionResponse{{
				RecordSet: msgs[0],
			},
			},
		},
		},
	}
	p.msgs = append(p.msgs, msgs...)
	return response, nil
}

// CreateTopicsContext implements the context-aware CreateTopics for the replicator client interface.
func (p *Client) CreateTopicsContext(ctx context.Context, createRequest *protocol.CreateTopicRequests) (*protocol.CreateTopicsResponse, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}
	return nil, nil
}

// LeaderAndISRContext implements the context-aware LeaderAndISR for the replicator client interface.
func (p *Client) LeaderAndISRContext(ctx context.Context, request *protocol.LeaderAndISRRequest) (*protocol.LeaderAndISRResponse, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}
	return nil, nil
}
