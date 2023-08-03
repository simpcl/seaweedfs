package raft

import (
	"math/rand"
	"net/http"
	"os"
	"path"
	"sort"
	"strings"
	"time"

	"weed/glog"
	"weed/util"

	"github.com/gorilla/mux"
	goRaft "github.com/seaweedfs/raft"
)

type GoRaftServer struct {
	serverAddr util.ServerAddress
	peers      map[string]util.ServerAddress // initial peers to join with
	dataDir    string
	raftServer goRaft.Server
	router     *mux.Router
}

func NewGoRaftServer(r *mux.Router, sm goRaft.StateMachine, option *RaftServerOption, command Command) *GoRaftServer {
	s := &GoRaftServer{
		peers:      option.Peers,
		serverAddr: option.ServerAddr,
		dataDir:    option.DataDir,
		router:     r,
	}

	if glog.V(4) {
		goRaft.SetLogLevel(2)
	}

	goRaft.RegisterCommand(command)

	var err error
	transporter := goRaft.NewHTTPTransporter("/cluster", 0)
	transporter.Transport.MaxIdleConnsPerHost = 1024
	glog.V(0).Infof("Starting GoRaftServer with %v", option.ServerAddr)

	// always clear previous log to avoid server is promotable
	os.RemoveAll(path.Join(s.dataDir, "log"))
	if !option.ResumeState {
		// always clear previous metadata
		os.RemoveAll(path.Join(s.dataDir, "conf"))
		os.RemoveAll(path.Join(s.dataDir, "snapshot"))
	}
	if err = os.MkdirAll(path.Join(s.dataDir, "snapshot"), os.ModePerm); err != nil && !os.IsExist(err) {
		glog.V(0).Infoln(err)
		return nil
	}

	s.raftServer, err = goRaft.NewServer(string(s.serverAddr), s.dataDir, transporter, sm, nil, "")
	if err != nil {
		glog.V(0).Infoln(err)
		return nil
	}
	transporter.Install(s.raftServer, s)
	heartbeatInterval := time.Duration(float64(400*time.Millisecond) * (rand.Float64()*0.25 + 1))
	s.raftServer.SetHeartbeatInterval(heartbeatInterval)
	s.raftServer.SetElectionTimeout(option.ElectionTimeout)
	if err = s.raftServer.LoadSnapshot(); err != nil {
		glog.V(0).Infoln(err)
		return nil
	}
	if err = s.raftServer.Start(); err != nil {
		glog.V(0).Infoln(err)
		return nil
	}

	for name, peer := range s.peers {
		if err = s.raftServer.AddPeer(name, "http://"+peer.ToHttpAddress()); err != nil {
			glog.V(0).Infoln(err)
		}
	}

	// Remove deleted peers
	for existsPeerName := range s.raftServer.Peers() {
		if existingPeer, found := s.peers[existsPeerName]; !found {
			glog.V(0).Infof("removing old peer: %s", existingPeer)
			if err := s.raftServer.RemovePeer(existsPeerName); err != nil {
				glog.V(0).Infoln(err)
				return nil
			}
		}
	}

	return s
}

func (s *GoRaftServer) CheckLeader() (string, error) {
	glog.V(0).Infof("current cluster leader: [%s], self: [%s], peers: %v", s.raftServer.Leader(), s.raftServer.Name(), s.Peers())

	leader := s.raftServer.Leader()
	if leader != "" {
		return leader, nil
	}

	if s.raftServer.IsLogEmpty() && s.isFirstPeer() {
		// TODO: It is also not safe when the first peer can not communicate with other peers for a long time after the restart
		// Initialize the server by joining itself.
		glog.V(0).Infof("Initializing new cluster, DefaultJoinCommand %s", s.raftServer.Name())

		_, err := s.raftServer.Do(&goRaft.DefaultJoinCommand{
			Name:             s.raftServer.Name(),
			ConnectionString: "http://" + s.serverAddr.ToHttpAddress(),
		})
		if err != nil {
			glog.V(0).Infoln(err)
			return "", err
		}
	}

	return s.raftServer.Leader(), nil
}

func (s *GoRaftServer) isFirstPeer() bool {
	if len(s.peers) <= 0 {
		return false
	}
	var peers []util.ServerAddress
	for _, sa := range s.peers {
		peers = append(peers, sa)
	}
	sort.Slice(peers, func(i int, j int) bool {
		return strings.Compare(string(peers[i]), string(peers[j])) < 0
	})
	glog.V(1).Infof("sorted peers: %v", peers)
	return s.serverAddr == peers[0]
}

func (s *GoRaftServer) isSingleNodeCluster() bool {
	return len(s.peers) == 1
}

func (s *GoRaftServer) LeaderChangeTrigger(f func(newLeader string)) {
	s.raftServer.AddEventListener(goRaft.LeaderChangeEventType, func(e goRaft.Event) {
		glog.V(0).Infof("event: %+v", e)
		if s.raftServer.Leader() != "" {
			f(e.Value().(string))
		}
	})
}

func (s *GoRaftServer) IsLeader() bool {
	return s.Leader() == s.raftServer.Name()
}

func (s *GoRaftServer) Leader() string {
	leader := s.raftServer.Leader()
	if leader != "" {
		return leader
	}

	if s.isSingleNodeCluster() {
		// We are a single node cluster, we are the leader
		return s.raftServer.Name()
	}

	return ""
}

func (s *GoRaftServer) Peers() (members []string) {
	peers := s.raftServer.Peers()

	for _, p := range peers {
		members = append(members, strings.TrimPrefix(p.ConnectionString, "http://"))
	}

	return
}

func (s *GoRaftServer) Apply(command Command) *util.Future {
	f := util.NewFuture()
	go func() {
		_, err := s.raftServer.Do(command)
		f.SetError(err)
		f.Done()
	}()
	return f
}

func (s *GoRaftServer) HandleFunc(pattern string, handler func(http.ResponseWriter, *http.Request)) {
	s.router.HandleFunc(pattern, handler)
}
