package weed_server

import (
	"encoding/json"
	"errors"
	"io/ioutil"
	"math/rand"
	"os"
	"path"
	"reflect"
	"sort"
	"strings"
	"time"

	"weed/glog"
	"weed/topology"

	"github.com/gorilla/mux"
	"github.com/seaweedfs/raft"
)

type GoRaftServer struct {
	peers      []string // initial peers to join with
	raftServer raft.Server
	dataDir    string
	httpAddr   string
	router     *mux.Router
	topo       *topology.Topology
}

func NewRaftServer(r *mux.Router, peers []string, httpAddr string, dataDir string, topo *topology.Topology, pulseSeconds int) *GoRaftServer {
	s := &GoRaftServer{
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
	glog.V(0).Infof("Starting GoRaftServer with %v", httpAddr)

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
	heartbeatInterval := time.Duration(float64(400*time.Millisecond) * (rand.Float64()*0.25 + 1))
	s.raftServer.SetHeartbeatInterval(heartbeatInterval)
	s.raftServer.SetElectionTimeout(time.Duration(pulseSeconds) * 500 * time.Millisecond)
	s.raftServer.Start()

	s.router.HandleFunc("/cluster/status", s.statusHandler).Methods("GET")

	for _, peer := range s.peers {
		if err = s.raftServer.AddPeer(peer, "http://"+peer); err != nil {
			glog.V(0).Infoln(err)
		}
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

	if s.IsLeader() {
		glog.V(0).Infoln("[", s.raftServer.Name(), "]", "I am the leader!")
	} else {
		if s.raftServer.Leader() != "" {
			glog.V(0).Infoln("[", s.raftServer.Name(), "]", s.raftServer.Leader(), "is the leader.")
		}
	}

	return s
}

func (s *GoRaftServer) LeaderChangeTrigger(f func(newLeader string)) {
	s.raftServer.AddEventListener(raft.LeaderChangeEventType, func(e raft.Event) {
		glog.V(0).Infof("event: %+v", e)
		if s.raftServer.Leader() != "" {
			f(e.Value().(string))
		}
	})
}

func (s *GoRaftServer) IsLeader() bool {
	if leader, e := s.Leader(); e == nil {
		return leader == s.raftServer.Name()
	}
	return false
}

func (s *GoRaftServer) Leader() (string, error) {
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

func (s *GoRaftServer) Peers() (members []string) {
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

func (s *GoRaftServer) Apply(command Command) *Future {
	f := newFuture()
	go func() {
		_, err := s.raftServer.Do(command)
		f.err = err
		f.Done()
	}()
	return f
}
