package weed_server

import (
	"fmt"
	"math/rand"
	"net/http"
	"net/http/httputil"
	"net/url"
	"sync"
	"time"

	"weed/glog"
	"weed/operation"
	"weed/security"
	"weed/sequence"
	"weed/storage"
	"weed/topology"
	"weed/util"

	"github.com/gorilla/mux"
)

type MasterServer struct {
	port                    int
	metaFolder              string
	volumeSizeLimitMB       uint
	preallocate             int64
	pulseSeconds            int
	defaultReplicaPlacement string
	garbageThreshold        string
	guard                   *security.Guard

	Topo   *topology.Topology
	vgLock sync.Mutex

	bounedLeaderChan chan int

	raftServer *RaftServer
}

func NewMasterServer(r *mux.Router, port int, metaFolder string,
	volumeSizeLimitMB uint,
	preallocate bool,
	pulseSeconds int,
	defaultReplicaPlacement string,
	garbageThreshold string,
	whiteList []string,
	secureKey string,
) *MasterServer {

	var preallocateSize int64
	if preallocate {
		preallocateSize = int64(volumeSizeLimitMB) * (1 << 20)
	}
	ms := &MasterServer{
		port:                    port,
		metaFolder:              metaFolder,
		volumeSizeLimitMB:       volumeSizeLimitMB,
		preallocate:             preallocateSize,
		pulseSeconds:            pulseSeconds,
		defaultReplicaPlacement: defaultReplicaPlacement,
		garbageThreshold:        garbageThreshold,
	}
	ms.bounedLeaderChan = make(chan int, 16)
	seq := sequence.NewMemorySequencer()
	ms.Topo = topology.NewTopology("topo", seq, uint64(volumeSizeLimitMB)*1024*1024)
	glog.V(0).Infoln("Volume Size Limit is", volumeSizeLimitMB, "MB")

	ms.guard = security.NewGuard(whiteList, secureKey)

	r.HandleFunc("/", ms.uiStatusHandler)
	r.HandleFunc("/ui/index.html", ms.uiStatusHandler)
	r.HandleFunc("/dir/assign", ms.proxyToLeader(ms.guard.WhiteList(ms.dirAssignHandler)))
	r.HandleFunc("/dir/lookup", ms.proxyToLeader(ms.guard.WhiteList(ms.dirLookupHandler)))
	r.HandleFunc("/dir/status", ms.proxyToLeader(ms.guard.WhiteList(ms.dirStatusHandler)))
	r.HandleFunc("/col/delete", ms.proxyToLeader(ms.guard.WhiteList(ms.collectionDeleteHandler)))
	r.HandleFunc("/vol/lookup", ms.proxyToLeader(ms.guard.WhiteList(ms.volumeLookupHandler)))
	r.HandleFunc("/vol/grow", ms.proxyToLeader(ms.guard.WhiteList(ms.volumeGrowHandler)))
	r.HandleFunc("/vol/status", ms.proxyToLeader(ms.guard.WhiteList(ms.volumeStatusHandler)))
	r.HandleFunc("/vol/vacuum", ms.proxyToLeader(ms.guard.WhiteList(ms.volumeVacuumHandler)))
	r.HandleFunc("/submit", ms.guard.WhiteList(ms.submitFromMasterServerHandler))
	r.HandleFunc("/delete", ms.guard.WhiteList(ms.deleteFromMasterServerHandler))
	r.HandleFunc("/stats/health", ms.guard.WhiteList(statsHealthHandler))
	r.HandleFunc("/stats/counter", ms.guard.WhiteList(statsCounterHandler))
	r.HandleFunc("/stats/memory", ms.guard.WhiteList(statsMemoryHandler))
	r.HandleFunc("/{fileId}", ms.proxyToLeader(ms.redirectHandler))

	ms.StartRefreshWritableVolumes()

	return ms
}

func (ms *MasterServer) InitRaftServer(r *mux.Router, peers []string, httpAddr string) {
	ms.raftServer = NewRaftServer(r, peers, httpAddr, ms.metaFolder, ms.Topo, ms.pulseSeconds)
	if ms.raftServer == nil {
		glog.Fatalf("Master startup error: can not create the raft server")
	}
}

func (ms *MasterServer) proxyToLeader(f func(w http.ResponseWriter, r *http.Request)) func(w http.ResponseWriter, r *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		if ms.raftServer.IsLeader() {
			f(w, r)
		} else if leader, e := ms.raftServer.Leader(); e == nil {
			ms.bounedLeaderChan <- 1
			defer func() { <-ms.bounedLeaderChan }()
			targetUrl, err := url.Parse("http://" + leader)
			if err != nil {
				writeJsonError(w, r, http.StatusInternalServerError,
					fmt.Errorf("Leader URL http://%s Parse Error: %v", leader, err))
				return
			}
			glog.V(4).Infoln("proxying to leader", leader)
			proxy := httputil.NewSingleHostReverseProxy(targetUrl)
			director := proxy.Director
			proxy.Director = func(req *http.Request) {
				actualHost, err := security.GetActualRemoteHost(req)
				if err == nil {
					req.Header.Set("HTTP_X_FORWARDED_FOR", actualHost)
				}
				director(req)
			}
			proxy.Transport = util.Transport
			proxy.ServeHTTP(w, r)
		} else {
			//drop it to the floor
			//writeJsonError(w, r, errors.New(ms.Topo.RaftServer.Name()+" does not know Leader yet:"+ms.Topo.RaftServer.Leader()))
		}
	}
}

func (ms *MasterServer) StartRefreshWritableVolumes() {
	go func() {
		for {
			if ms.raftServer.IsLeader() {
				ms.Topo.CheckFullVolumes()
			}
			time.Sleep(time.Duration(float32(ms.pulseSeconds*1e3)*(1+rand.Float32())) * time.Millisecond)
		}
	}()
	go func(garbageThreshold string) {
		c := time.Tick(15 * time.Minute)
		for _ = range c {
			if ms.raftServer.IsLeader() {
				ms.Topo.Vacuum(garbageThreshold, ms.preallocate)
			}
		}
	}(ms.garbageThreshold)
}

// one replication type may need rp.GetCopyCount() actual volumes
// given copyCount, how many logical volumes to create
func calcVolumeCount(copyCount int) (count int) {
	switch copyCount {
	case 1:
		count = 7
	case 2:
		count = 6
	case 3:
		count = 3
	default:
		count = 1
	}
	return
}

func (ms *MasterServer) AutomaticGrowVolumes(option *topology.VolumeOption) (count int, err error) {
	count, err = ms.GrowVolumesByCount(calcVolumeCount(option.ReplicaPlacement.GetCopyCount()), option)
	if count > 0 && count%option.ReplicaPlacement.GetCopyCount() == 0 {
		return count, nil
	}
	return count, err
}

func (ms *MasterServer) GrowVolumesByCount(targetCount int, option *topology.VolumeOption) (counter int, err error) {
	for i := 0; i < targetCount; i++ {
		if c, e := ms.findAndGrow(option); e == nil {
			counter += c
		} else {
			return counter, e
		}
	}
	return
}

func (ms *MasterServer) findAndGrow(option *topology.VolumeOption) (int, error) {
	servers, e := ms.Topo.FindEmptySlotsForOneVolume(option)
	if e != nil {
		return 0, e
	}
	vid := ms.raftServer.NextVolumeId()
	err := ms.grow(vid, option, servers...)
	return len(servers), err
}

func (ms *MasterServer) grow(vid storage.VolumeId, option *topology.VolumeOption, servers ...*topology.DataNode) error {
	for _, server := range servers {
		if err := operation.AllocateVolume(server.String(), vid.String(), option.Collection, option.ReplicaPlacement.String(), option.Ttl.String(), option.Preallocate); err == nil {
			vi := storage.VolumeInfo{
				Id:               vid,
				Size:             0,
				Collection:       option.Collection,
				ReplicaPlacement: option.ReplicaPlacement,
				Ttl:              option.Ttl,
				Version:          storage.CurrentVersion,
			}
			server.AddOrUpdateVolume(vi)
			ms.Topo.RegisterVolumeLayout(vi, server)
			glog.V(0).Infoln("Created Volume", vid, "on", server.NodeImpl.String())
		} else {
			glog.V(0).Infoln("Failed to assign volume", vid, "to", servers, "error", err)
			return fmt.Errorf("Failed to assign %d: %v", vid, err)
		}
	}
	return nil
}
