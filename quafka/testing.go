package quafka

import (
	"fmt"
	"os"
	"sync/atomic"
	"time"

	"github.com/hashicorp/raft"
	dynaport "github.com/travisjeffery/go-dynaport"

	"github.com/bodaay/quafka/quafka/config"

	"github.com/uber/jaeger-lib/metrics"

	"github.com/uber/jaeger-client-go"
	jaegercfg "github.com/uber/jaeger-client-go/config"
)

var (
	nodeNumber int32
)

// testingT is an interface that matches *testing.T
type testingT interface {
	Name() string
	Fatalf(format string, args ...interface{})
	Fatal(args ...interface{})
	Errorf(format string, args ...interface{})
	Error(args ...interface{})
	Helper()
}

func NewTestServer(t testingT, cbBroker func(cfg *config.Config), cbServer func(cfg *config.Config)) (*Server, string) {
	ports := dynaport.Get(4)
	nodeID := atomic.AddInt32(&nodeNumber, 1)

	cfg := jaegercfg.Configuration{
		Sampler: &jaegercfg.SamplerConfig{
			Type:  jaeger.SamplerTypeConst,
			Param: 1,
		},
		Reporter: &jaegercfg.ReporterConfig{
			LogSpans: true,
		},
	}

	// jLogger := jaegerlog.StdLogger
	jMetricsFactory := metrics.NullFactory

	tracer, closer, err := cfg.New(
		"quafka",
		// jaegercfg.Logger(jLogger),
		jaegercfg.Metrics(jMetricsFactory),
	)
	if err != nil {
		panic(err)
	}

	tmpDir, err := os.MkdirTemp("", fmt.Sprintf("quafka-test-server-%d", nodeID))
	if err != nil {
		panic(err)
	}

	config := config.DefaultConfig()
	config.ID = nodeID
	config.NodeName = fmt.Sprintf("%s-node-%d", t.Name(), nodeID)
	config.DataDir = tmpDir
	config.Addr = fmt.Sprintf("%s:%d", "127.0.0.1", ports[0])
	config.RaftAddr = fmt.Sprintf("%s:%d", "127.0.0.1", ports[1])
	config.SerfLANConfig.MemberlistConfig.BindAddr = "127.0.0.1"
	config.SerfLANConfig.MemberlistConfig.BindPort = ports[2]
	config.LeaveDrainTime = 1 * time.Millisecond
	config.ReconcileInterval = 300 * time.Millisecond

	// Tighten the Serf timing
	config.SerfLANConfig.MemberlistConfig.BindAddr = "127.0.0.1"
	config.SerfLANConfig.MemberlistConfig.SuspicionMult = 2
	config.SerfLANConfig.MemberlistConfig.RetransmitMult = 2
	config.SerfLANConfig.MemberlistConfig.ProbeTimeout = 50 * time.Millisecond
	config.SerfLANConfig.MemberlistConfig.ProbeInterval = 100 * time.Millisecond
	config.SerfLANConfig.MemberlistConfig.GossipInterval = 100 * time.Millisecond

	// Tighten the Raft timing
	config.RaftConfig.LeaderLeaseTimeout = 100 * time.Millisecond
	config.RaftConfig.HeartbeatTimeout = 200 * time.Millisecond
	config.RaftConfig.ElectionTimeout = 200 * time.Millisecond

	if cbBroker != nil {
		cbBroker(config)
	}

	b, err := NewBroker(config, tracer)
	if err != nil {
		t.Fatalf("err != nil: %s", err)
	}

	if cbServer != nil {
		cbServer(config)
	}

	return NewServer(config, b, nil, tracer, closer.Close), tmpDir
}

func TestJoin(t testingT, s1 *Server, other ...*Server) {
	addr := fmt.Sprintf("127.0.0.1:%d",
		s1.config.SerfLANConfig.MemberlistConfig.BindPort)
	for _, s2 := range other {
		if num, err := s2.handler.(*Broker).serf.Join([]string{addr}, true); err != nil {
			t.Fatalf("err: %v", err)
		} else if num != 1 {
			t.Fatalf("bad: %d", num)
		}
	}
}

// WaitForLeader waits for one of the servers to be leader, failing the test if no one is the leader. Returns the leader (if there is one) and non-leaders.
func WaitForLeader(t testingT, servers ...*Server) (*Server, []*Server) {
	tmp := struct {
		leader    *Server
		followers map[*Server]bool
	}{nil, make(map[*Server]bool)}

	RetryFunc(t, func() error {
		for _, s := range servers {
			if raft.Leader == s.handler.(*Broker).raft.State() {
				tmp.leader = s
			} else {
				tmp.followers[s] = true
			}
		}
		if tmp.leader == nil {
			return fmt.Errorf("no leader")
		}
		return nil
	})

	followers := make([]*Server, 0, len(tmp.followers))
	for f := range tmp.followers {
		followers = append(followers, f)
	}
	return tmp.leader, followers
}

// WaitForBrokerLeader waits for a broker to become leader
func WaitForBrokerLeader(t testingT, b *Broker) {
	t.Helper()
	RetryFunc(t, func() error {
		if b.raft.State() != raft.Leader {
			return fmt.Errorf("broker not leader, state: %s", b.raft.State())
		}
		return nil
	})
}

// RetryFunc retries a function until it succeeds or times out
func RetryFunc(t testingT, fn func() error) {
	t.Helper()
	deadline := time.Now().Add(7 * time.Second)
	for time.Now().Before(deadline) {
		err := fn()
		if err == nil {
			return
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatal("timeout waiting for condition")
}

// Retry is a helper for test retries
type Retry struct {
	t        testingT
	deadline time.Time
}

// NewRetry creates a new retry helper
func NewRetry(t testingT) *Retry {
	return &Retry{
		t:        t,
		deadline: time.Now().Add(7 * time.Second),
	}
}

// Run runs the retry function
func (r *Retry) Run(fn func(rr *RetryReporter)) {
	r.t.Helper()
	for time.Now().Before(r.deadline) {
		rr := &RetryReporter{}
		fn(rr)
		if !rr.failed {
			return
		}
		time.Sleep(25 * time.Millisecond)
	}
	r.t.Fatal("timeout waiting for condition")
}

// RetryReporter is a reporter for retry attempts
type RetryReporter struct {
	failed bool
	err    error
}

// Fatal marks the retry as failed
func (r *RetryReporter) Fatal(args ...interface{}) {
	r.failed = true
	r.err = fmt.Errorf("%v", args)
}

// Fatalf marks the retry as failed with a formatted message
func (r *RetryReporter) Fatalf(format string, args ...interface{}) {
	r.failed = true
	r.err = fmt.Errorf(format, args...)
}

// Check checks if an error occurred and marks failed if so
func (r *RetryReporter) Check(err error) {
	if err != nil {
		r.failed = true
		r.err = err
	}
}
