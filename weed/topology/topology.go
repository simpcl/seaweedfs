package topology

import (
	"errors"
	"fmt"
	"math/rand"
	"sync"

	"weed/glog"
	"weed/sequence"
	"weed/storage"
	"weed/util"

	"github.com/chrislusf/raft"
)

type Topology struct {
	NodeImpl

	collectionMap *util.ConcurrentReadMap

	pulse int64

	volumeSizeLimit uint64

	Sequence sequence.Sequencer

	chanFullVolumes chan storage.VolumeInfo

	Configuration *Configuration

	RaftServer raft.Server

	mut sync.Mutex
}

func NewTopology(id string, seq sequence.Sequencer, volumeSizeLimit uint64) *Topology {
	t := &Topology{}
	t.id = NodeId(id)
	t.nodeType = "Topology"
	t.NodeImpl.value = t
	t.children = make(map[NodeId]Node)
	t.collectionMap = util.NewConcurrentReadMap()
	t.volumeSizeLimit = volumeSizeLimit

	t.Sequence = seq

	t.chanFullVolumes = make(chan storage.VolumeInfo)

	t.Configuration = &Configuration{}

	return t
}

func (t *Topology) IsLeader() bool {
	if leader, e := t.Leader(); e == nil {
		return leader == t.RaftServer.Name()
	}
	return false
}

func (t *Topology) Leader() (string, error) {
	l := ""
	if t.RaftServer != nil {
		l = t.RaftServer.Leader()
	} else {
		return "", errors.New("Raft Server not ready yet!")
	}

	if l == "" {
		// We are a single node cluster, we are the leader
		return t.RaftServer.Name(), errors.New("Raft Server not initialized!")
	}

	return l, nil
}

func (t *Topology) Lookup(collection string, vid storage.VolumeId) []*DataNode {
	//maybe an issue if lots of collections?
	if collection == "" {
		for _, c := range t.collectionMap.Items() {
			if list := c.(*Collection).Lookup(vid); list != nil {
				return list
			}
		}
	} else {
		if c, ok := t.collectionMap.Find(collection); ok {
			return c.(*Collection).Lookup(vid)
		}
	}
	return nil
}

func (t *Topology) NextVolumeId() storage.VolumeId {
	vid := t.GetMaxVolumeId()
	next := vid.Next()
	go t.RaftServer.Do(NewMaxVolumeIdCommand(next))
	return next
}

func (t *Topology) HasWritableVolume(option *VolumeOption) bool {
	vl := t.GetVolumeLayout(option.Collection, option.ReplicaPlacement, option.Ttl)
	return vl.GetActiveVolumeCount(option) > 0
}

func (t *Topology) PickForWrite(count uint64, option *VolumeOption) (string, uint64, *DataNode, error) {
	vid, count, datanodes, err := t.GetVolumeLayout(option.Collection, option.ReplicaPlacement, option.Ttl).PickForWrite(count, option)
	if err != nil || datanodes.Length() == 0 {
		return "", 0, nil, errors.New("No writable volumes available!")
	}
	fileId, count := t.Sequence.NextFileId(count)
	return storage.NewFileId(*vid, fileId, rand.Uint32()).String(), count, datanodes.Head(), nil
}

func (t *Topology) GetVolumeLayout(collectionName string, rp *storage.ReplicaPlacement, ttl *storage.TTL) *VolumeLayout {
	return t.collectionMap.Get(collectionName, func() interface{} {
		return NewCollection(collectionName, t.volumeSizeLimit)
	}).(*Collection).GetOrCreateVolumeLayout(rp, ttl)
}

func (t *Topology) FindCollection(collectionName string) (*Collection, bool) {
	c, hasCollection := t.collectionMap.Find(collectionName)
	if !hasCollection {
		return nil, false
	}
	return c.(*Collection), hasCollection
}

func (t *Topology) DeleteCollection(collectionName string) {
	t.collectionMap.Delete(collectionName)
}

func (t *Topology) RegisterVolumeLayout(v storage.VolumeInfo, dn *DataNode) {
	t.GetVolumeLayout(v.Collection, v.ReplicaPlacement, v.Ttl).RegisterVolume(&v, dn)
}
func (t *Topology) UnRegisterVolumeLayout(v storage.VolumeInfo, dn *DataNode) {
	glog.Infof("removing volume info:%+v", v)
	t.GetVolumeLayout(v.Collection, v.ReplicaPlacement, v.Ttl).UnRegisterVolume(&v, dn)
}

func (t *Topology) GetOrCreateDataCenter(dcName string) *DataCenter {
	t.mut.Lock()
	defer t.mut.Unlock()
	for _, c := range t.Children() {
		dc := c.(*DataCenter)
		if string(dc.Id()) == dcName {
			return dc
		}
	}
	dc := NewDataCenter(dcName)
	t.LinkChildNode(dc)
	return dc
}

func (t *Topology) CheckFullVolumes() {
	t.CollectDeadNodeAndFullVolumes(t.volumeSizeLimit)
}

func (t *Topology) SetVolumeCapacityFull(volumeInfo storage.VolumeInfo) bool {
	vl := t.GetVolumeLayout(volumeInfo.Collection, volumeInfo.ReplicaPlacement, volumeInfo.Ttl)
	if !vl.SetVolumeCapacityFull(volumeInfo.Id) {
		return false
	}
	for _, dn := range vl.vid2location[volumeInfo.Id].list {
		if !volumeInfo.ReadOnly {
			dn.UpAdjustActiveVolumeCountDelta(-1)
		}
	}
	return true
}

func (t *Topology) UnRegisterDataNode(dn *DataNode) {
	for _, v := range dn.GetVolumes() {
		glog.V(0).Infoln("Removing Volume", v.Id, "from the dead volume server", dn.Id())
		vl := t.GetVolumeLayout(v.Collection, v.ReplicaPlacement, v.Ttl)
		vl.SetVolumeUnavailable(dn, v.Id)
	}
	dn.UpAdjustVolumeCountDelta(-dn.GetVolumeCount())
	dn.UpAdjustActiveVolumeCountDelta(-dn.GetActiveVolumeCount())
	dn.UpAdjustMaxVolumeCountDelta(-dn.GetMaxVolumeCount())
	if dn.Parent() != nil {
		dn.Parent().UnlinkChildNode(dn.Id())
	}
}

// 1. find the main data node
// 1.1 collect all data nodes that have 1 slots
// 2.2 collect all racks that have rp.SameRackCount+1
// 2.2 collect all data centers that have DiffRackCount+rp.SameRackCount+1
// 2. find rest data nodes
func (t *Topology) FindEmptySlotsForOneVolume(option *VolumeOption) (servers []*DataNode, err error) {
	//find main datacenter and other data centers
	rp := option.ReplicaPlacement
	mainDataCenter, otherDataCenters, dc_err := t.RandomlyPickNodes(rp.DiffDataCenterCount+1, func(node Node) error {
		if option.DataCenter != "" && node.IsDataCenter() && node.Id() != NodeId(option.DataCenter) {
			return fmt.Errorf("Not matching preferred data center:%s", option.DataCenter)
		}
		if len(node.Children()) < rp.DiffRackCount+1 {
			return fmt.Errorf("Only has %d racks, not enough for %d.", len(node.Children()), rp.DiffRackCount+1)
		}
		if node.FreeSpace() < rp.DiffRackCount+rp.SameRackCount+1 {
			return fmt.Errorf("Free:%d < Expected:%d", node.FreeSpace(), rp.DiffRackCount+rp.SameRackCount+1)
		}
		possibleRacksCount := 0
		for _, rack := range node.Children() {
			possibleDataNodesCount := 0
			for _, n := range rack.Children() {
				if n.FreeSpace() >= 1 {
					possibleDataNodesCount++
				}
			}
			if possibleDataNodesCount >= rp.SameRackCount+1 {
				possibleRacksCount++
			}
		}
		if possibleRacksCount < rp.DiffRackCount+1 {
			return fmt.Errorf("Only has %d racks with more than %d free data nodes, not enough for %d.", possibleRacksCount, rp.SameRackCount+1, rp.DiffRackCount+1)
		}
		return nil
	})
	if dc_err != nil {
		return nil, dc_err
	}

	//find main rack and other racks
	mainRack, otherRacks, rack_err := mainDataCenter.(*DataCenter).RandomlyPickNodes(rp.DiffRackCount+1, func(node Node) error {
		if option.Rack != "" && node.IsRack() && node.Id() != NodeId(option.Rack) {
			return fmt.Errorf("Not matching preferred rack:%s", option.Rack)
		}
		if node.FreeSpace() < rp.SameRackCount+1 {
			return fmt.Errorf("Free:%d < Expected:%d", node.FreeSpace(), rp.SameRackCount+1)
		}
		if len(node.Children()) < rp.SameRackCount+1 {
			// a bit faster way to test free racks
			return fmt.Errorf("Only has %d data nodes, not enough for %d.", len(node.Children()), rp.SameRackCount+1)
		}
		possibleDataNodesCount := 0
		for _, n := range node.Children() {
			if n.FreeSpace() >= 1 {
				possibleDataNodesCount++
			}
		}
		if possibleDataNodesCount < rp.SameRackCount+1 {
			return fmt.Errorf("Only has %d data nodes with a slot, not enough for %d.", possibleDataNodesCount, rp.SameRackCount+1)
		}
		return nil
	})
	if rack_err != nil {
		return nil, rack_err
	}

	//find main rack and other racks
	mainServer, otherServers, server_err := mainRack.(*Rack).RandomlyPickNodes(rp.SameRackCount+1, func(node Node) error {
		if option.DataNode != "" && node.IsDataNode() && node.Id() != NodeId(option.DataNode) {
			return fmt.Errorf("Not matching preferred data node:%s", option.DataNode)
		}
		if node.FreeSpace() < 1 {
			return fmt.Errorf("Free:%d < Expected:%d", node.FreeSpace(), 1)
		}
		return nil
	})
	if server_err != nil {
		return nil, server_err
	}

	servers = append(servers, mainServer.(*DataNode))
	for _, server := range otherServers {
		servers = append(servers, server.(*DataNode))
	}
	for _, rack := range otherRacks {
		r := rand.Intn(rack.FreeSpace())
		if server, e := rack.ReserveOneVolume(r); e == nil {
			servers = append(servers, server)
		} else {
			return servers, e
		}
	}
	for _, datacenter := range otherDataCenters {
		r := rand.Intn(datacenter.FreeSpace())
		if server, e := datacenter.ReserveOneVolume(r); e == nil {
			servers = append(servers, server)
		} else {
			return servers, e
		}
	}
	return
}
