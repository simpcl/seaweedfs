package topology

import (
	"fmt"
	"sync"

	"weed/glog"
	"weed/storage"
)

/*
This package is created to resolve these replica placement issues:
1. growth factor for each replica level, e.g., add 10 volumes for 1 copy, 20 volumes for 2 copies, 30 volumes for 3 copies
2. in time of tight storage, how to reduce replica level
3. optimizing for hot data on faster disk, cold data on cheaper storage,
4. volume allocation for each bucket
*/

type VolumeGrowOption struct {
	Collection       string
	ReplicaPlacement *storage.ReplicaPlacement
	Ttl              *storage.TTL
	Prealloacte      int64
	DataCenter       string
	Rack             string
	DataNode         string
}

type VolumeGrowth struct {
	accessLock sync.Mutex
}

func (o *VolumeGrowOption) String() string {
	return fmt.Sprintf("Collection:%s, ReplicaPlacement:%v, Ttl:%v, DataCenter:%s, Rack:%s, DataNode:%s", o.Collection, o.ReplicaPlacement, o.Ttl, o.DataCenter, o.Rack, o.DataNode)
}

func NewDefaultVolumeGrowth() *VolumeGrowth {
	return &VolumeGrowth{}
}

// one replication type may need rp.GetCopyCount() actual volumes
// given copyCount, how many logical volumes to create
func (vg *VolumeGrowth) findVolumeCount(copyCount int) (count int) {
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

func (vg *VolumeGrowth) AutomaticGrowByType(option *VolumeGrowOption, topo *Topology) (count int, err error) {
	count, err = vg.GrowByCountAndType(vg.findVolumeCount(option.ReplicaPlacement.GetCopyCount()), option, topo)
	if count > 0 && count%option.ReplicaPlacement.GetCopyCount() == 0 {
		return count, nil
	}
	return count, err
}
func (vg *VolumeGrowth) GrowByCountAndType(targetCount int, option *VolumeGrowOption, topo *Topology) (counter int, err error) {
	vg.accessLock.Lock()
	defer vg.accessLock.Unlock()

	for i := 0; i < targetCount; i++ {
		if c, e := vg.findAndGrow(topo, option); e == nil {
			counter += c
		} else {
			return counter, e
		}
	}
	return
}

func (vg *VolumeGrowth) findAndGrow(topo *Topology, option *VolumeGrowOption) (int, error) {
	servers, e := topo.FindEmptySlotsForOneVolume(option)
	if e != nil {
		return 0, e
	}
	vid := topo.NextVolumeId()
	err := vg.grow(topo, vid, option, servers...)
	return len(servers), err
}

func (vg *VolumeGrowth) grow(topo *Topology, vid storage.VolumeId, option *VolumeGrowOption, servers ...*DataNode) error {
	for _, server := range servers {
		if err := AllocateVolume(server, vid, option); err == nil {
			vi := storage.VolumeInfo{
				Id:               vid,
				Size:             0,
				Collection:       option.Collection,
				ReplicaPlacement: option.ReplicaPlacement,
				Ttl:              option.Ttl,
				Version:          storage.CurrentVersion,
			}
			server.AddOrUpdateVolume(vi)
			topo.RegisterVolumeLayout(vi, server)
			glog.V(0).Infoln("Created Volume", vid, "on", server.NodeImpl.String())
		} else {
			glog.V(0).Infoln("Failed to assign volume", vid, "to", servers, "error", err)
			return fmt.Errorf("Failed to assign %d: %v", vid, err)
		}
	}
	return nil
}
