package weed_server

import (
	"encoding/json"
	"errors"
	"io/ioutil"
	"os"
	"path"
	"reflect"
	"sort"
	"strings"
	"time"

	"weed/glog"
	"weed/storage"
	"weed/topology"

	"github.com/chrislusf/raft"
	"github.com/gorilla/mux"
)

type RaftServer struct {
	peers      []string // initial peers to join with
	raftServer raft.Server
	dataDir    string
	httpAddr   string
	router     *mux.Router
	topo       *topology.Topology
}

func NewRaftServer(r *mux.Router, peers []string, httpAddr string, dataDir string, topo *topology.Topology, pulseSeconds int) *RaftServer {
	s := &RaftServer{
		peers:    peers,
		httpAddr: httpAddr,
		dataDir:  dataDir,
		router:   r,
		topo:     topo,
	}

	if glog.V(4) {
		raft.SetLogLevel(2)
	}

	raft.RegisterCommand(&MaxVolumeIdCommand{})

	var err error
	transporter := raft.NewHTTPTransporter("/cluster", 0)
	transporter.Transport.MaxIdleConnsPerHost = 1024
	glog.V(0).Infof("Starting RaftServer with %v", httpAddr)

	// Clear old cluster configurations if peers are changed
	if oldPeers, changed := isPeersChanged(s.dataDir, httpAddr, s.peers); changed {
		glog.V(0).Infof("Peers Change: %v => %v", oldPeers, s.peers)
		os.RemoveAll(path.Join(s.dataDir, "conf"))
		os.RemoveAll(path.Join(s.dataDir, "log"))
		os.RemoveAll(path.Join(s.dataDir, "snapshot"))
	}

	s.raftServer, err = raft.NewServer(s.httpAddr, s.dataDir, transporter, nil, topo, "")
	if err != nil {
		glog.V(0).Infoln(err)
		return nil
	}
	transporter.Install(s.raftServer, s)
	s.raftServer.SetHeartbeatInterval(500 * time.Millisecond)
	s.raftServer.SetElectionTimeout(time.Duration(pulseSeconds) * 500 * time.Millisecond)
	s.raftServer.Start()

	s.router.HandleFunc("/cluster/status", s.statusHandler).Methods("GET")

	for _, peer := range s.peers {
		s.raftServer.AddPeer(peer, "http://"+peer)
	}
	time.Sleep(2 * time.Second)
	if s.raftServer.IsLogEmpty() {
		// Initialize the server by joining itself.
		glog.V(0).Infoln("Initializing new cluster")

		_, err := s.raftServer.Do(&raft.DefaultJoinCommand{
			Name:             s.raftServer.Name(),
			ConnectionString: "http://" + s.httpAddr,
		})

		if err != nil {
			glog.V(0).Infoln(err)
			return nil
		}
	}

	glog.V(0).Infof("current cluster leader: %v", s.raftServer.Leader())

	s.raftServer.AddEventListener(raft.LeaderChangeEventType, func(e raft.Event) {
		glog.V(0).Infof("event: %+v", e)
		if s.raftServer.Leader() != "" {
			glog.V(0).Infoln("[", s.raftServer.Name(), "]", s.raftServer.Leader(), "becomes leader.")
		}
	})
	if s.IsLeader() {
		glog.V(0).Infoln("[", s.raftServer.Name(), "]", "I am the leader!")
	} else {
		if s.raftServer.Leader() != "" {
			glog.V(0).Infoln("[", s.raftServer.Name(), "]", s.raftServer.Leader(), "is the leader.")
		}
	}

	return s
}

func (s *RaftServer) NextVolumeId() storage.VolumeId {
	vid := s.topo.GetMaxVolumeId()
	next := vid.Next()
	go s.raftServer.Do(NewMaxVolumeIdCommand(next))
	return next
}

func (s *RaftServer) IsLeader() bool {
	if leader, e := s.Leader(); e == nil {
		return leader == s.raftServer.Name()
	}
	return false
}

func (s *RaftServer) Leader() (string, error) {
	l := ""
	if s.raftServer != nil {
		l = s.raftServer.Leader()
	} else {
		return "", errors.New("Raft Server not ready yet!")
	}

	if l == "" {
		// We are a single node cluster, we are the leader
		return s.raftServer.Name(), errors.New("Raft Server not initialized!")
	}

	return l, nil
}

func (s *RaftServer) Peers() (members []string) {
	peers := s.raftServer.Peers()

	for _, p := range peers {
		members = append(members, strings.TrimPrefix(p.ConnectionString, "http://"))
	}

	return
}

func isPeersChanged(dir string, self string, peers []string) (oldPeers []string, changed bool) {
	confPath := path.Join(dir, "conf")
	// open conf file
	b, err := ioutil.ReadFile(confPath)
	if err != nil {
		return oldPeers, true
	}
	conf := &raft.Config{}
	if err = json.Unmarshal(b, conf); err != nil {
		return oldPeers, true
	}

	for _, p := range conf.Peers {
		oldPeers = append(oldPeers, strings.TrimPrefix(p.ConnectionString, "http://"))
	}
	oldPeers = append(oldPeers, self)

	if len(peers) == 0 && len(oldPeers) <= 1 {
		return oldPeers, false
	}

	sort.Strings(peers)
	sort.Strings(oldPeers)

	return oldPeers, !reflect.DeepEqual(peers, oldPeers)
}
