package weed_server

import (
	"encoding/json"
	"fmt"
	hashicorpRaft "github.com/hashicorp/raft"
	"weed/glog"
	"weed/storage"
	"weed/topology"

	"github.com/seaweedfs/raft"
)

type MaxVolumeIdCommand struct {
	MaxVolumeId storage.VolumeId `json:"maxVolumeId"`
}

func NewMaxVolumeIdCommand(value storage.VolumeId) *MaxVolumeIdCommand {
	return &MaxVolumeIdCommand{
		MaxVolumeId: value,
	}
}

func (c *MaxVolumeIdCommand) CommandName() string {
	return "MaxVolumeId"
}

func (c *MaxVolumeIdCommand) Apply(server raft.Server) (interface{}, error) {
	topo := server.Context().(*topology.Topology)
	before := topo.GetMaxVolumeId()
	topo.UpAdjustMaxVolumeId(c.MaxVolumeId)

	glog.V(0).Infoln("max volume id", before, "==>", topo.GetMaxVolumeId())

	return nil, nil
}

func (s *MaxVolumeIdCommand) Persist(sink hashicorpRaft.SnapshotSink) error {
	b, err := json.Marshal(s)
	if err != nil {
		return fmt.Errorf("marshal: %v", err)
	}
	_, err = sink.Write(b)
	if err != nil {
		sink.Cancel()
		return fmt.Errorf("sink.Write(): %v", err)
	}
	return sink.Close()
}

func (s *MaxVolumeIdCommand) Release() {
}
