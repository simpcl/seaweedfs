package command

import (
	"net/http"
	"os"
	"runtime"
	"strconv"
	"strings"
	"time"

	"weed/glog"
	"weed/pb/master_pb"
	"weed/raft"
	weed_server "weed/server"
	"weed/util"

	"github.com/gorilla/mux"
	"github.com/soheilhy/cmux"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

func init() {
	cmdMaster.Run = runMaster // break init cycle
}

var cmdMaster = &Command{
	UsageLine: "master -port=9333",
	Short:     "start a master server",
	Long: `start a master server to provide volume=>location mapping service
  and sequence number of file ids

  `,
}

var (
	mport                   = cmdMaster.Flag.Int("port", 9333, "http listen port")
	masterIp                = cmdMaster.Flag.String("ip", "localhost", "master <ip>|<server> address")
	masterBindIp            = cmdMaster.Flag.String("ip.bind", "0.0.0.0", "ip address to bind to")
	metaFolder              = cmdMaster.Flag.String("mdir", os.TempDir(), "data directory to store meta data")
	masterPeers             = cmdMaster.Flag.String("peers", "", "other master nodes in comma separated ip:port list, example: 127.0.0.1:9093,127.0.0.1:9094")
	volumeSizeLimitMB       = cmdMaster.Flag.Uint("volumeSizeLimitMB", 30*1000, "Master stops directing writes to oversized volumes.")
	volumePreallocate       = cmdMaster.Flag.Bool("volumePreallocate", false, "Preallocate disk space for volumes.")
	mpulse                  = cmdMaster.Flag.Int("pulseSeconds", 5, "number of seconds between heartbeats")
	defaultReplicaPlacement = cmdMaster.Flag.String("defaultReplication", "000", "Default replication type if not specified.")
	// mTimeout                = cmdMaster.Flag.Int("idleTimeout", 30, "connection idle seconds")
	mMaxCpu               = cmdMaster.Flag.Int("maxCpu", 0, "maximum number of CPUs. 0 means all available CPUs")
	garbageThreshold      = cmdMaster.Flag.String("garbageThreshold", "0.3", "threshold to vacuum and reclaim spaces")
	masterWhiteListOption = cmdMaster.Flag.String("whiteList", "", "comma separated Ip addresses having write permission. No limit if empty.")
	masterSecureKey       = cmdMaster.Flag.String("secure.secret", "", "secret to encrypt Json Web Token(JWT)")
	masterCpuProfile      = cmdMaster.Flag.String("cpuprofile", "", "cpu profile output file")
	masterMemProfile      = cmdMaster.Flag.String("memprofile", "", "memory profile output file")

	raftResumeState   = cmdMaster.Flag.Bool("resumeState", false, "resume previous state on start master server")
	heartbeatInterval = cmdMaster.Flag.Duration("heartbeatInterval", 300*time.Millisecond, "heartbeat interval of master servers, and will be randomly multiplied by [1, 1.25)")
	electionTimeout   = cmdMaster.Flag.Duration("electionTimeout", 10*time.Second, "election timeout of master servers")

	masterWhiteList []string
)

func runMaster(cmd *Command, args []string) bool {
	if *mMaxCpu < 1 {
		*mMaxCpu = runtime.NumCPU()
	}
	runtime.GOMAXPROCS(*mMaxCpu)
	util.SetupProfiling(*masterCpuProfile, *masterMemProfile)

	if err := util.TestFolderWritable(*metaFolder); err != nil {
		glog.Fatalf("Check Meta Folder (-mdir) Writable %s : %s", *metaFolder, err)
	}
	if *masterWhiteListOption != "" {
		masterWhiteList = strings.Split(*masterWhiteListOption, ",")
	}
	if *volumeSizeLimitMB > 30*1000 {
		glog.Fatalf("volumeSizeLimitMB should be smaller than 30000")
	}

	r := mux.NewRouter()
	ms := weed_server.NewMasterServer(r, *mport, *metaFolder,
		*volumeSizeLimitMB, *volumePreallocate,
		*mpulse, *defaultReplicaPlacement, *garbageThreshold,
		masterWhiteList, *masterSecureKey,
	)

	listeningAddress := *masterBindIp + ":" + strconv.Itoa(*mport)

	glog.V(0).Infoln("Start Seaweed Master", util.VERSION, "at", listeningAddress)

	listener, e := util.NewListener(listeningAddress, 0)
	if e != nil {
		glog.Fatalf("Master startup error: %v", e)
	}

	go func() {
		time.Sleep(100 * time.Millisecond)

		myMasterAddress, peers := checkPeers(*masterIp, *mport, *masterPeers)
		mPeers := make(map[string]util.ServerAddress)
		for _, peer := range peers {
			mPeers[string(peer)] = peer
		}

		raftServerOption := &raft.RaftServerOption{
			Peers:             mPeers,
			ServerAddr:        myMasterAddress,
			DataDir:           *metaFolder,
			ResumeState:       *raftResumeState,
			HeartbeatInterval: *heartbeatInterval,
			ElectionTimeout:   *electionTimeout,
		}
		ms.InitRaftServer(r, raftServerOption)
	}()

	// start grpc and http server
	m := cmux.New(listener)

	grpcL := m.MatchWithWriters(cmux.HTTP2MatchHeaderFieldSendSettings("content-type", "application/grpc"))
	httpL := m.Match(cmux.HTTP1Fast())

	// Create your protocol servers.
	grpcS := grpc.NewServer()
	master_pb.RegisterSeaweedServer(grpcS, ms)
	reflection.Register(grpcS)

	httpS := &http.Server{Handler: r}

	go grpcS.Serve(grpcL)
	go httpS.Serve(httpL)

	if err := m.Serve(); err != nil {
		glog.Fatalf("master server failed to serve: %v", err)
	}

	return true
}

func checkPeers(masterIp string, masterPort int, peers string) (masterAddress util.ServerAddress, cleanedPeers []util.ServerAddress) {
	glog.V(0).Infof("current: %s:%d peers:%s", masterIp, masterPort, peers)
	masterAddress = util.NewServerAddress(masterIp, masterPort, 0)
	cleanedPeers = util.FromStringToSAs(peers)

	hasSelf := false
	for _, peer := range cleanedPeers {
		if peer.ToHttpAddress() == masterAddress.ToHttpAddress() {
			hasSelf = true
			break
		}
	}

	if !hasSelf {
		cleanedPeers = append(cleanedPeers, masterAddress)
	}
	if len(cleanedPeers)%2 == 0 {
		glog.Fatalf("Only odd number of masters are supported: %+v", cleanedPeers)
	}
	return
}
