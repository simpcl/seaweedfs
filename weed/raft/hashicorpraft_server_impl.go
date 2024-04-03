package raft

import (
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"net"
	"os"
	"path"
	"path/filepath"
	"time"
	"weed/glog"
	"weed/util"

	"github.com/hashicorp/raft"
	hashicorpRaft "github.com/hashicorp/raft"
	boltdb "github.com/hashicorp/raft-boltdb/v2"
)

const (
	raftDBFile         = "hraft.db"
	updatePeersTimeout = 15 * time.Minute
)

type HashicorpRaftServer struct {
	serverAddr util.ServerAddress
	peers      map[string]util.ServerAddress // initial peers to join with
	dataDir    string
	raftServer *hashicorpRaft.Raft
}

func NewHashicorpRaftServer(stateMachine raft.FSM, option *RaftServerOption) RaftServer {
	s := &HashicorpRaftServer{
		peers:      option.Peers,
		serverAddr: option.ServerAddr,
		dataDir:    option.DataDir,
	}

	c := hashicorpRaft.DefaultConfig()
	c.LocalID = hashicorpRaft.ServerID(s.serverAddr) // TODO maybee the IP:port address will change
	c.HeartbeatTimeout = time.Duration(float64(option.HeartbeatInterval) * (rand.Float64()*0.25 + 1))
	c.ElectionTimeout = option.ElectionTimeout
	if c.LeaderLeaseTimeout > c.HeartbeatTimeout {
		c.LeaderLeaseTimeout = c.HeartbeatTimeout
	}
	if glog.V(4) {
		c.LogLevel = "Debug"
	} else if glog.V(2) {
		c.LogLevel = "Info"
	} else if glog.V(1) {
		c.LogLevel = "Warn"
	} else if glog.V(0) {
		c.LogLevel = "Error"
	}

	if err := hashicorpRaft.ValidateConfig(c); err != nil {
		glog.V(0).Infof("raft.ValidateConfig: %v", err)
		return nil
	}

	// Setup Raft communication.
	addr, err := net.ResolveTCPAddr("tcp", s.serverAddr.ToGrpcAddress())
	if err != nil {
		glog.V(0).Infoln(err)
		return nil
	}
	transport, err := raft.NewTCPTransport(s.serverAddr.ToGrpcAddress(), addr, 3, 10*time.Second, os.Stderr)
	if err != nil {
		glog.V(0).Infoln(err)
		return nil
	}

	glog.V(0).Infof("option.ResumeState: %+v", option.ResumeState)
	if !option.ResumeState {
		os.RemoveAll(path.Join(s.dataDir, raftDBFile))
		os.RemoveAll(path.Join(s.dataDir, "snapshots"))
	}
	baseDir := s.dataDir

	raftDB, err := boltdb.NewBoltStore(filepath.Join(baseDir, raftDBFile))
	if err != nil {
		glog.V(0).Infoln(fmt.Errorf(`boltdb.NewBoltStore(%q): %v`, filepath.Join(baseDir, raftDBFile), err))
		return nil
	}
	glog.V(4).Infof("boltdb.NewBoltStore(%q): init successfully", filepath.Join(baseDir, raftDBFile))
	logStore := raftDB
	stableStore := raftDB

	snapshotStore, err := hashicorpRaft.NewFileSnapshotStore(baseDir, 3, os.Stderr)
	if err != nil {
		glog.V(0).Infoln(fmt.Errorf(`raft.NewFileSnapshotStore(%q, ...): %v`, baseDir, err))
		return nil
	}

	s.raftServer, err = hashicorpRaft.NewRaft(c, stateMachine, logStore, stableStore, snapshotStore, transport)
	if err != nil {
		glog.V(0).Infoln(fmt.Errorf("raft.NewRaft: %v", err))
		return nil
	}

	configFuture := s.raftServer.GetConfiguration()
	if err = configFuture.Error(); err != nil {
		glog.Fatalf("error getting config: %s", err)
	}
	configuration := configFuture.Configuration()

	if !option.ResumeState || len(configuration.Servers) == 0 {
		if err = s.initWithPeers(c.LeaderLeaseTimeout); err != nil {
			glog.Fatalf("init raft with peers error: %s", err.Error())
		}
	} else {
		go s.updatePeers()
	}

	ticker := time.NewTicker(c.HeartbeatTimeout * 10)
	if glog.V(4) {
		go func() {
			for {
				select {
				case <-ticker.C:
					cfuture := s.raftServer.GetConfiguration()
					if err = cfuture.Error(); err != nil {
						glog.Fatalf("error getting config: %s", err)
					}
					configuration := cfuture.Configuration()
					glog.V(4).Infof("Showing peers known by %s:\n%+v", s.raftServer.String(), configuration.Servers)
				}
			}
		}()
	}

	return s
}

func (s *HashicorpRaftServer) initWithPeers(leaderLeaseTimeout time.Duration) error {
	var cfg hashicorpRaft.Configuration
	for _, peer := range s.peers {
		cfg.Servers = append(cfg.Servers, hashicorpRaft.Server{
			Suffrage: hashicorpRaft.Voter,
			ID:       hashicorpRaft.ServerID(peer),
			Address:  hashicorpRaft.ServerAddress(peer.ToGrpcAddress()),
		})
	}

	peerIdx := GetPeerIndex(s.serverAddr, s.peers)
	timeSleep := time.Duration(float64(leaderLeaseTimeout) * (rand.Float64()*0.25 + 1) * float64(peerIdx))
	glog.V(0).Infof("Bootstrapping idx: %d sleep: %v new cluster: %+v", peerIdx, timeSleep, cfg)
	time.Sleep(timeSleep)
	f := s.raftServer.BootstrapCluster(cfg)
	if err := f.Error(); err != nil {
		return fmt.Errorf("raft.BootstrapCluster failed: %s", err.Error())
	}
	return nil
}

func (s *HashicorpRaftServer) joinPeer(nodeID, addr string) error {
	glog.V(1).Infof("joining new peer: %s", nodeID)
	configFuture := s.raftServer.GetConfiguration()
	if err := configFuture.Error(); err != nil {
		return fmt.Errorf("getting raft configuration error: %s", err.Error())
	}

	for _, srv := range configFuture.Configuration().Servers {
		if srv.ID == hashicorpRaft.ServerID(nodeID) || srv.Address == hashicorpRaft.ServerAddress(addr) {
			if srv.Address == hashicorpRaft.ServerAddress(addr) && srv.ID == hashicorpRaft.ServerID(nodeID) {
				glog.V(0).Infof("node %s at %s already member of cluster, ignoring join request", nodeID, addr)
				return nil
			}

			future := s.raftServer.RemoveServer(srv.ID, 0, 0)
			if err := future.Error(); err != nil {
				return fmt.Errorf("removing existing node %s at %s error: %s", nodeID, addr, err)
			}
		}
	}

	f := s.raftServer.AddVoter(hashicorpRaft.ServerID(nodeID), hashicorpRaft.ServerAddress(addr), 0, 0)
	if f.Error() != nil {
		return fmt.Errorf("adding raft node %s at %s error: %s", nodeID, addr, f.Error())
	}

	glog.V(0).Infof("node %s at %s joined successfully", nodeID, addr)
	return nil
}

func (s *HashicorpRaftServer) leavePeer(nodeID string) error {
	glog.V(0).Infof("removing old peer: %s", nodeID)
	future := s.raftServer.RemoveServer(hashicorpRaft.ServerID(nodeID), 0, 0)
	if err := future.Error(); err != nil {
		return fmt.Errorf("removing raft node %s error: %s", nodeID, err)
	}
	return nil
}

func (s *HashicorpRaftServer) updatePeers() {
	for {
		select {
		case isLeader := <-s.raftServer.LeaderCh():
			if isLeader {
				leaderName := string(s.serverAddr)
				existPeerNameMap := make(map[string]bool)
				for _, server := range s.raftServer.GetConfiguration().Configuration().Servers {
					if string(server.ID) == leaderName {
						continue
					}
					existPeerNameMap[string(server.ID)] = true
				}
				for _, peer := range s.peers {
					peerName := string(peer)
					if peerName == leaderName {
						continue
					}
					if err := s.joinPeer(peerName, peer.ToGrpcAddress()); err != nil {
						glog.V(0).Infof("joining peer failed: %s", err.Error())
						return
					}
				}
				for peer := range existPeerNameMap {
					if _, found := s.peers[peer]; !found {
						if err := s.leavePeer(peer); err != nil {
							glog.V(0).Infof("leaving peer failed: %s", err.Error())
							return
						}
					}
				}
			}
			return
		case <-time.After(updatePeersTimeout):
			return
		}
	}
}

func (s *HashicorpRaftServer) CheckLeader() (string, error) {
	if s.raftServer == nil {
		return "", errors.New("raft server not ready yet")
	}

	_, id := s.raftServer.LeaderWithID()
	leader := string(id)

	if leader == "" {
		return "", errors.New("no leader")
	}

	return leader, nil
}

func (s *HashicorpRaftServer) Leader() string {
	if s.raftServer == nil {
		return ""
	}

	_, id := s.raftServer.LeaderWithID()
	return string(id)
}

func (s *HashicorpRaftServer) IsLeader() bool {
	if s.raftServer == nil {
		return false
	}
	return s.raftServer.State() == hashicorpRaft.Leader
}

func (s *HashicorpRaftServer) LeaderChangeTrigger(f func(newLeader string)) {
	prevLeader, _ := s.raftServer.LeaderWithID()
	ticker := time.NewTicker(time.Second * 2)
	go func() {
		for {
			select {
			case <-ticker.C:
				leader, _ := s.raftServer.LeaderWithID()
				glog.V(4).Infof("leader check ticker: current leader is %v", leader)
				if leader != prevLeader {
					if leader == "" {
						glog.V(0).Infof("we lost leader %+v", prevLeader)
					} else {
						f(string(leader))
					}
				}
				prevLeader = leader
			}
		}
	}()
}

func (s *HashicorpRaftServer) Exec(command Command) *util.Future {
	f := util.NewFuture()
	glog.V(4).Infof("HashicorpRaftServer Exec Command %+v", command)
	b, err := json.Marshal(command)
	if err != nil {
		f.SetError(fmt.Errorf("failed marshal command: %+v", err))
		return f.Done()
	}
	af := s.raftServer.Apply(b, time.Second)
	go func() {
		f.SetError(af.Error())
		f.Done()
	}()
	return f
}

func (s *HashicorpRaftServer) Peers() (members []string) {
	for _, server := range s.raftServer.GetConfiguration().Configuration().Servers {
		members = append(members, string(server.ID))
	}
	return members
}
