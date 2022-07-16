package command

import (
	"net/http"
	"os"
	"runtime"
	"runtime/pprof"
	"strconv"
	"strings"
	"sync"
	"time"

	"weed/glog"
	"weed/pb"
	weed_server "weed/server"
	"weed/storage"
	"weed/util"

	"github.com/gorilla/mux"
	"github.com/soheilhy/cmux"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

type ServerOptions struct {
	cpuprofile *string
}

var (
	serverOptions ServerOptions
)

func init() {
	cmdServer.Run = runServer // break init cycle
}

var cmdServer = &Command{
	UsageLine: "server -port=8080 -dir=/tmp -volume.max=5 -ip=server_name",
	Short:     "start a server, including volume server, and automatically elect a master server",
	Long: `start both a volume server to provide storage spaces
  and a master server to provide volume=>location mapping service and sequence number of file ids

  This is provided as a convenient way to start both volume server and master server.
  The servers are exactly the same as starting them separately.

  So other volume servers can use this embedded master server also.

  `,
}

var (
	serverIp                      = cmdServer.Flag.String("ip", "localhost", "ip or server name")
	serverBindIp                  = cmdServer.Flag.String("ip.bind", "0.0.0.0", "ip address to bind to")
	serverMaxCpu                  = cmdServer.Flag.Int("maxCpu", 0, "maximum number of CPUs. 0 means all available CPUs")
	serverTimeout                 = cmdServer.Flag.Int("idleTimeout", 30, "connection idle seconds")
	serverDataCenter              = cmdServer.Flag.String("dataCenter", "", "current volume server's data center name")
	serverRack                    = cmdServer.Flag.String("rack", "", "current volume server's rack name")
	serverWhiteListOption         = cmdServer.Flag.String("whiteList", "", "comma separated Ip addresses having write permission. No limit if empty.")
	serverPeers                   = cmdServer.Flag.String("master.peers", "", "other master nodes in comma separated ip:masterPort list")
	serverSecureKey               = cmdServer.Flag.String("secure.secret", "", "secret to encrypt Json Web Token(JWT)")
	serverGarbageThreshold        = cmdServer.Flag.String("garbageThreshold", "0.3", "threshold to vacuum and reclaim spaces")
	masterPort                    = cmdServer.Flag.Int("master.port", 9333, "master server http listen port")
	masterMetaFolder              = cmdServer.Flag.String("master.dir", "", "data directory to store meta data, default to same as -dir specified")
	masterVolumeSizeLimitMB       = cmdServer.Flag.Uint("master.volumeSizeLimitMB", 30*1000, "Master stops directing writes to oversized volumes.")
	masterVolumePreallocate       = cmdServer.Flag.Bool("master.volumePreallocate", false, "Preallocate disk space for volumes.")
	masterDefaultReplicaPlacement = cmdServer.Flag.String("master.defaultReplicaPlacement", "000", "Default replication type if not specified.")
	volumePort                    = cmdServer.Flag.Int("volume.port", 8080, "volume server http listen port")
	volumePublicPort              = cmdServer.Flag.Int("volume.port.public", 0, "volume server public port")
	volumeDataFolders             = cmdServer.Flag.String("dir", os.TempDir(), "directories to store data files. dir[,dir]...")
	volumeMaxDataVolumeCounts     = cmdServer.Flag.String("volume.max", "7", "maximum numbers of volumes, count[,count]...")
	volumePulse                   = cmdServer.Flag.Int("pulseSeconds", 5, "number of seconds between heartbeats")
	volumeIndexType               = cmdServer.Flag.String("volume.index", "memory", "Choose [memory|leveldb|boltdb|btree] mode for memory~performance balance.")
	volumeReadRedirect            = cmdServer.Flag.Bool("volume.read.redirect", true, "Redirect moved or non-local volumes.")
	volumeServerPublicUrl         = cmdServer.Flag.String("volume.publicUrl", "", "publicly accessible address")
	volumeEnableBytesCache        = cmdServer.Flag.Bool("volume.cache.enable", false, "direct cache instead of OS cache, cost more memory.")

	serverWhiteList []string
)

func init() {
	serverOptions.cpuprofile = cmdServer.Flag.String("cpuprofile", "", "cpu profile output file")
}

func runServer(cmd *Command, args []string) bool {
	if *serverOptions.cpuprofile != "" {
		f, err := os.Create(*serverOptions.cpuprofile)
		if err != nil {
			glog.Fatal(err)
		}
		pprof.StartCPUProfile(f)
		defer pprof.StopCPUProfile()
	}

	if *volumePublicPort == 0 {
		*volumePublicPort = *volumePort
	}

	if *serverMaxCpu < 1 {
		*serverMaxCpu = runtime.NumCPU()
	}
	runtime.GOMAXPROCS(*serverMaxCpu)

	folders := strings.Split(*volumeDataFolders, ",")
	maxCountStrings := strings.Split(*volumeMaxDataVolumeCounts, ",")
	var maxCounts []int
	for _, maxString := range maxCountStrings {
		if max, e := strconv.Atoi(maxString); e == nil {
			maxCounts = append(maxCounts, max)
		} else {
			glog.Fatalf("The max specified in -max not a valid number %s", maxString)
		}
	}
	if len(folders) != len(maxCounts) {
		glog.Fatalf("%d directories by -dir, but only %d max is set by -max", len(folders), len(maxCounts))
	}
	for _, folder := range folders {
		if err := util.TestFolderWritable(folder); err != nil {
			glog.Fatalf("Check Data Folder(-dir) Writable %s : %s", folder, err)
		}
	}

	if *masterMetaFolder == "" {
		*masterMetaFolder = folders[0]
	}
	if err := util.TestFolderWritable(*masterMetaFolder); err != nil {
		glog.Fatalf("Check Meta Folder (-mdir=\"%s\") Writable: %s", *masterMetaFolder, err)
	}

	if *serverWhiteListOption != "" {
		serverWhiteList = strings.Split(*serverWhiteListOption, ",")
	}

	var raftWaitForMaster sync.WaitGroup
	var volumeWait sync.WaitGroup

	raftWaitForMaster.Add(1)
	volumeWait.Add(1)

	go func() {
		r := mux.NewRouter()
		ms := weed_server.NewMasterServer(r, *masterPort, *masterMetaFolder,
			*masterVolumeSizeLimitMB, *masterVolumePreallocate,
			*volumePulse, *masterDefaultReplicaPlacement, *serverGarbageThreshold,
			serverWhiteList, *serverSecureKey,
		)

		glog.V(0).Infoln("Start Seaweed Master", util.VERSION, "at", *serverIp+":"+strconv.Itoa(*masterPort))
		masterListener, e := util.NewListener(*serverBindIp+":"+strconv.Itoa(*masterPort), 0)
		if e != nil {
			glog.Fatalf("Master startup error: %v", e)
		}

		go func() {
			raftWaitForMaster.Wait()
			time.Sleep(100 * time.Millisecond)
			myAddress := *serverIp + ":" + strconv.Itoa(*masterPort)
			var peers []string
			if *serverPeers != "" {
				peers = strings.Split(*serverPeers, ",")
			}
			raftServer := weed_server.NewRaftServer(r, peers, myAddress, *masterMetaFolder, ms.Topo, *volumePulse)
			ms.SetRaftServer(raftServer)
			volumeWait.Done()
		}()

		raftWaitForMaster.Done()

		// start grpc and http server
		m := cmux.New(masterListener)

		grpcL := m.Match(cmux.HTTP2HeaderField("content-type", "application/grpc"))
		httpL := m.Match(cmux.Any())

		// Create your protocol servers.
		grpcS := grpc.NewServer()
		pb.RegisterSeaweedServer(grpcS, ms)
		reflection.Register(grpcS)

		httpS := &http.Server{Handler: r}

		go grpcS.Serve(grpcL)
		go httpS.Serve(httpL)

		if err := m.Serve(); err != nil {
			glog.Fatalf("master server failed to serve: %v", err)
		}

	}()

	volumeWait.Wait()
	time.Sleep(100 * time.Millisecond)
	if *volumePublicPort == 0 {
		*volumePublicPort = *volumePort
	}
	if *volumeServerPublicUrl == "" {
		*volumeServerPublicUrl = *serverIp + ":" + strconv.Itoa(*volumePublicPort)
	}
	isSeperatedPublicPort := *volumePublicPort != *volumePort
	volumeMux := http.NewServeMux()
	publicVolumeMux := volumeMux
	if isSeperatedPublicPort {
		publicVolumeMux = http.NewServeMux()
	}
	volumeNeedleMapKind := storage.NeedleMapInMemory
	switch *volumeIndexType {
	case "leveldb":
		volumeNeedleMapKind = storage.NeedleMapLevelDb
	case "boltdb":
		volumeNeedleMapKind = storage.NeedleMapBoltDb
	case "btree":
		volumeNeedleMapKind = storage.NeedleMapBtree
	}
	volumeServer := weed_server.NewVolumeServer(volumeMux, publicVolumeMux,
		*serverIp, *volumePort, *volumeServerPublicUrl,
		folders, maxCounts,
		volumeNeedleMapKind,
		*serverIp+":"+strconv.Itoa(*masterPort), *volumePulse, *serverDataCenter, *serverRack,
		serverWhiteList, *volumeReadRedirect,
	)

	glog.V(0).Infoln("Start Seaweed volume server", util.VERSION, "at", *serverIp+":"+strconv.Itoa(*volumePort))
	volumeListener, eListen := util.NewListener(
		*serverBindIp+":"+strconv.Itoa(*volumePort),
		time.Duration(*serverTimeout)*time.Second,
	)
	if eListen != nil {
		glog.Fatalf("Volume server listener error: %v", eListen)
	}
	if isSeperatedPublicPort {
		publicListeningAddress := *serverIp + ":" + strconv.Itoa(*volumePublicPort)
		glog.V(0).Infoln("Start Seaweed volume server", util.VERSION, "public at", publicListeningAddress)
		publicListener, e := util.NewListener(publicListeningAddress, time.Duration(*serverTimeout)*time.Second)
		if e != nil {
			glog.Fatalf("Volume server listener error:%v", e)
		}
		go func() {
			if e := http.Serve(publicListener, publicVolumeMux); e != nil {
				glog.Fatalf("Volume server fail to serve public: %v", e)
			}
		}()
	}

	util.OnInterrupt(func() {
		volumeServer.Shutdown()
		pprof.StopCPUProfile()
	})

	if e := http.Serve(volumeListener, volumeMux); e != nil {
		glog.Fatalf("Volume server fail to serve:%v", e)
	}

	return true
}
