package quafka_test

import (
	"bytes"
	"context"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/bodaay/quafka/mock"
	"github.com/bodaay/quafka/quafka"
	"github.com/bodaay/quafka/quafka/structs"
	"github.com/bodaay/quafka/testutil"
)

func TestBroker_Replicate(t *testing.T) {
	c := newCommitLog()
	l := mock.NewClient(4)

	replica := &quafka.Replica{
		Partition: structs.Partition{
			Topic:  "test",
			ID:     0,
			Leader: 0,
			AR:     []int32{0},
		},
		BrokerID: 0,
		Log:      c,
	}

	replicator := quafka.NewReplicator(quafka.ReplicatorConfig{
		MinBytes:    5,
		MaxWaitTime: 250 * time.Millisecond,
	}, replica, l)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	replicator.Replicate(ctx)

	testutil.WaitForResult(func() (bool, error) {
		commitLog := c.Log()
		if len(commitLog) < 4 {
			return false, nil
		}
		for i, m := range l.Messages() {
			require.True(t, bytes.Equal(m, commitLog[i]))
		}
		return true, nil
	}, func(err error) {
		t.Fatalf("err: %v", err)
	})

	require.NoError(t, replicator.Close())
}

type commitLog struct {
	*mock.CommitLog
	sync.RWMutex
	b [][]byte
}

func (c *commitLog) Log() [][]byte {
	log := [][]byte{}
	c.RLock()
	log = append(log, c.b...)
	c.RUnlock()
	return log
}

func newCommitLog() *commitLog {
	c := &commitLog{}
	c.CommitLog = &mock.CommitLog{
		AppendFunc: func(b []byte) (int64, error) {
			c.Lock()
			c.b = append(c.b, b)
			c.Unlock()
			return 0, nil
		},
		DeleteFunc: func() error {
			return nil
		},
		NewReaderFunc: func(offset int64, maxBytes int32) (io.Reader, error) {
			return nil, nil
		},
		TruncateFunc: func(int64) error {
			return nil
		},

		NewestOffsetFunc: func() int64 {
			return 0
		},

		OldestOffsetFunc: func() int64 {
			return 0
		},
	}
	return c
}
