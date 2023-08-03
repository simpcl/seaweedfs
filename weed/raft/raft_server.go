package raft

import (
	"time"
	"weed/util"
)

type RaftServer interface {
	CheckLeader() (string, error)
	Leader() string
	IsLeader() bool
	LeaderChangeTrigger(func(newLeader string))
	Apply(command Command) *util.Future
	Peers() (members []string)
}

// Command represents an action to be taken on the replicated state machine.
type Command interface {
	CommandName() string
}

type RaftServerOption struct {
	Peers             map[string]util.ServerAddress
	ServerAddr        util.ServerAddress
	DataDir           string
	ResumeState       bool // for GoRaftServer
	HeartbeatInterval time.Duration
	ElectionTimeout   time.Duration
}
