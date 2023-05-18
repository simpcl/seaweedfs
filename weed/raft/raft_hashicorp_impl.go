package raft

import (
	"encoding/json"
	"fmt"
	transport "github.com/Jille/raft-grpc-transport"
	"github.com/gorilla/mux"
	hashicorpRaft "github.com/hashicorp/raft"
	boltdb "github.com/hashicorp/raft-boltdb/v2"
	"io"
	"math/rand"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"time"
	"weed/glog"
	"weed/topology"
	"weed/util"
)

const (
	ldbFile            = "logs.dat"
	sdbFile            = "stable.dat"
	updatePeersTimeout = 15 * time.Minute
)

type HashicorpRaftServer struct {
	peers            map[string]util.ServerAddress // initial peers to join with
	raftServer       *hashicorpRaft.Raft
	dataDir          string
	serverAddr       util.ServerAddress
	router           *mux.Router
	topo             *topology.Topology
	TransportManager *transport.Manager
}

func NewHashicorpRaftServer(r *mux.Router, option *RaftServerOption) RaftServer {
	s := &HashicorpRaftServer{
		peers:      option.Peers,
		serverAddr: option.ServerAddr,
		dataDir:    option.DataDir,
		topo:       option.Topo,
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
		glog.V(0).Infoln(fmt.Errorf(`raft.ValidateConfig: %v`, err))
		return nil
	}

	if option.RaftBootstrap {
		os.RemoveAll(path.Join(s.dataDir, ldbFile))
		os.RemoveAll(path.Join(s.dataDir, sdbFile))
		os.RemoveAll(path.Join(s.dataDir, "snapshots"))
	}
	if err := os.MkdirAll(path.Join(s.dataDir, "snapshots"), os.ModePerm); err != nil {
		glog.V(0).Infoln(err)
		return nil
	}
	baseDir := s.dataDir

	ldb, err := boltdb.NewBoltStore(filepath.Join(baseDir, ldbFile))
	if err != nil {
		glog.V(0).Infoln(fmt.Errorf(`boltdb.NewBoltStore(%q): %v`, filepath.Join(baseDir, "logs.dat"), err))
		return nil
	}
	glog.V(4).Infoln(fmt.Errorf(`boltdb.NewBoltStore(%q): init successfully`, filepath.Join(baseDir, "logs.dat")))

	sdb, err := boltdb.NewBoltStore(filepath.Join(baseDir, sdbFile))
	if err != nil {
		glog.V(0).Infoln(fmt.Errorf(`boltdb.NewBoltStore(%q): %v`, filepath.Join(baseDir, "stable.dat"), err))
		return nil
	}
	glog.V(4).Infoln(fmt.Errorf(`boltdb.NewBoltStore(%q): init successfully`, filepath.Join(baseDir, "stable.dat")))

	fss, err := hashicorpRaft.NewFileSnapshotStore(baseDir, 3, os.Stderr)
	if err != nil {
		glog.V(0).Infoln(fmt.Errorf(`raft.NewFileSnapshotStore(%q, ...): %v`, baseDir, err))
		return nil
	}

	stateMachine := StateMachine{topo: option.Topo}
	s.raftServer, err = hashicorpRaft.NewRaft(c, &stateMachine, ldb, sdb, fss, option.HashicorpTransporter.Transport())
	if err != nil {
		glog.V(0).Infoln(fmt.Errorf("raft.NewRaft: %v", err))
		return nil
	}
	if option.RaftBootstrap || len(s.raftServer.GetConfiguration().Configuration().Servers) == 0 {
		cfg := s.AddPeersConfiguration()
		// Need to get lock, in case all servers do this at the same time.
		peerIdx := getPeerIdx(s.serverAddr, s.peers)
		timeSleep := time.Duration(float64(c.LeaderLeaseTimeout) * (rand.Float64()*0.25 + 1) * float64(peerIdx))
		glog.V(0).Infof("Bootstrapping idx: %d sleep: %v new cluster: %+v", peerIdx, timeSleep, cfg)
		time.Sleep(timeSleep)
		f := s.raftServer.BootstrapCluster(cfg)
		if err := f.Error(); err != nil {
			glog.V(0).Infoln(fmt.Errorf("raft.Raft.BootstrapCluster: %v", err))
			return nil
		}
	} else {
		go s.UpdatePeers()
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

func (s *HashicorpRaftServer) AddPeersConfiguration() (cfg hashicorpRaft.Configuration) {
	for _, peer := range s.peers {
		cfg.Servers = append(cfg.Servers, hashicorpRaft.Server{
			Suffrage: hashicorpRaft.Voter,
			ID:       hashicorpRaft.ServerID(peer),
			Address:  hashicorpRaft.ServerAddress(peer.ToGrpcAddress()),
		})
	}
	return cfg
}

func (s *HashicorpRaftServer) UpdatePeers() {
	for {
		select {
		case isLeader := <-s.raftServer.LeaderCh():
			if isLeader {
				peerLeader := string(s.serverAddr)
				existsPeerName := make(map[string]bool)
				for _, server := range s.raftServer.GetConfiguration().Configuration().Servers {
					if string(server.ID) == peerLeader {
						continue
					}
					existsPeerName[string(server.ID)] = true
				}
				for _, peer := range s.peers {
					peerName := string(peer)
					if peerName == peerLeader || existsPeerName[peerName] {
						continue
					}
					glog.V(0).Infof("adding new peer: %s", peerName)
					s.raftServer.AddVoter(
						hashicorpRaft.ServerID(peerName), hashicorpRaft.ServerAddress(peer.ToGrpcAddress()), 0, 0)
				}
				for peer := range existsPeerName {
					if _, found := s.peers[peer]; !found {
						glog.V(0).Infof("removing old peer: %s", peer)
						s.raftServer.RemoveServer(hashicorpRaft.ServerID(peer), 0, 0)
					}
				}
				if _, found := s.peers[peerLeader]; !found {
					glog.V(0).Infof("removing old leader peer: %s", peerLeader)
					s.raftServer.RemoveServer(hashicorpRaft.ServerID(peerLeader), 0, 0)
				}
			}
			return
		case <-time.After(updatePeersTimeout):
			return
		}
	}
}

func (s *HashicorpRaftServer) Leader() (string, error) {
	l := ""
	if s.raftServer != nil {
		ld, _ := s.raftServer.LeaderWithID()
		l = string(ld)
	} else {
		return "", ErrRaftNotReady
	}

	if l == "" {
		// todo: is String() == Name ?
		// We are a single node cluster, we are the leader
		return s.raftServer.String(), ErrLeaderNotSelected
	}

	return l, nil
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

func (s *HashicorpRaftServer) Apply(command Command) *Future {
	f := newFuture()
	b, err := json.Marshal(command)
	if err != nil {
		f.err = fmt.Errorf("failed marshal command: %+v", err)
		return f.Done()
	}
	future := s.raftServer.Apply(b, time.Second)
	go func() {
		f.err = future.Error()
		f.Done()
	}()
	return f
}

func (s *HashicorpRaftServer) Peers() (members []string) {
	for _, server := range s.raftServer.GetConfiguration().Configuration().Servers {
		members = append(members, string(server.Address))
	}
	return members
}

func getPeerIdx(self util.ServerAddress, mapPeers map[string]util.ServerAddress) int {
	peers := make([]util.ServerAddress, 0, len(mapPeers))
	for _, peer := range mapPeers {
		peers = append(peers, peer)
	}
	sort.Slice(peers, func(i, j int) bool {
		return strings.Compare(string(peers[i]), string(peers[j])) < 0
	})
	for i, peer := range peers {
		if string(peer) == string(self) {
			return i
		}
	}
	return -1
}

type StateMachine struct {
	topo *topology.Topology
}

var _ hashicorpRaft.FSM = &StateMachine{}

func (s *StateMachine) Save() ([]byte, error) {
	state := MaxVolumeIdCommand{
		MaxVolumeId: s.topo.GetMaxVolumeId(),
	}
	glog.V(1).Infof("Save raft state %+v", state)
	return json.Marshal(state)
}

func (s *StateMachine) Recovery(data []byte) error {
	state := MaxVolumeIdCommand{}
	err := json.Unmarshal(data, &state)
	if err != nil {
		return err
	}
	glog.V(1).Infof("Recovery raft state %+v", state)
	s.topo.UpAdjustMaxVolumeId(state.MaxVolumeId)
	return nil
}

func (s *StateMachine) Apply(l *hashicorpRaft.Log) interface{} {
	before := s.topo.GetMaxVolumeId()
	state := MaxVolumeIdCommand{}
	err := json.Unmarshal(l.Data, &state)
	if err != nil {
		return err
	}
	s.topo.UpAdjustMaxVolumeId(state.MaxVolumeId)

	glog.V(1).Infoln("max volume id", before, "==>", s.topo.GetMaxVolumeId())
	return nil
}

func (s *StateMachine) Snapshot() (hashicorpRaft.FSMSnapshot, error) {
	return &MaxVolumeIdCommand{
		MaxVolumeId: s.topo.GetMaxVolumeId(),
	}, nil
}

func (s *StateMachine) Restore(r io.ReadCloser) error {
	b, err := io.ReadAll(r)
	if err != nil {
		return err
	}
	if err := s.Recovery(b); err != nil {
		return err
	}
	return nil
}
