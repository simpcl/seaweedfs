package raft

import (
	"sort"
	"strings"
	"time"
	"weed/glog"
	"weed/util"
)

type RaftServer interface {
	CheckLeader() (string, error)
	Leader() string
	IsLeader() bool
	LeaderChangeTrigger(func(newLeader string))
	Exec(command Command) *util.Future
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
	IsHashicorpRaft   bool
}

func GetPeerIndex(peer util.ServerAddress, peersMap map[string]util.ServerAddress) int {
	if peersMap == nil || len(peersMap) == 0 {
		return -1
	}
	var peers []util.ServerAddress
	for _, sa := range peersMap {
		peers = append(peers, sa)
	}
	sort.Slice(peers, func(i int, j int) bool {
		return strings.Compare(string(peers[i]), string(peers[j])) < 0
	})
	glog.V(1).Infof("sorted peers: %v", peers)
	for idx, sa := range peers {
		if string(peer) == string(sa) {
			return idx
		}
	}
	return -1
}
