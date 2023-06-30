package weed_server

import (
	"errors"
	"time"
	"weed/topology"
	"weed/util"

	"google.golang.org/grpc"
)

var ErrNotImplement = errors.New("raft server has not implement")
var ErrRaftNotReady = errors.New("raft server not ready yet")
var ErrLeaderNotSelected = errors.New("raft leader not selected yet")

type RaftServer interface {
	Leader() (string, error)
	IsLeader() bool
	LeaderChangeTrigger(func(newLeader string))
	Apply(command Command) *Future
	Peers() (members []string)
}

type changeInfo struct {
	prevLeader string
	newLeader  string
}

// Command represents an action to be taken on the replicated state machine.
type Command interface {
	CommandName() string
}

type Future struct {
	doneCh chan error
	err    error
}

func (g *Future) Complete() *Future {
	if g.doneCh != nil {
		<-g.doneCh
	}
	return g
}

func (g *Future) Error() error {
	return g.err
}

func (g *Future) Done() *Future {
	if g.doneCh != nil {
		close(g.doneCh)
	}
	return g
}

func newFuture() *Future {
	return &Future{doneCh: make(chan error)}
}

type RaftServerOption struct {
	GrpcDialOption grpc.DialOption
	Peers          map[string]util.ServerAddress
	ServerAddr     util.ServerAddress
	DataDir        string
	Topo           *topology.Topology
	// RaftResumeState is used for goRaft
	RaftResumeState   bool
	HeartbeatInterval time.Duration
	ElectionTimeout   time.Duration
	RaftBootstrap     bool
}
