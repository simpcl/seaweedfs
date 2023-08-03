package weed_server

import (
	"encoding/json"
	"weed/glog"
	"weed/storage"
	"weed/topology"

	goRaft "github.com/seaweedfs/raft"
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

func (c *MaxVolumeIdCommand) Apply(server goRaft.Server) (interface{}, error) {
	topo := server.StateMachine().(*StateMachine).topo
	before := topo.GetMaxVolumeId()
	topo.UpAdjustMaxVolumeId(c.MaxVolumeId)

	glog.V(0).Infoln("max volume id", before, "==>", topo.GetMaxVolumeId())

	return nil, nil
}

type StateMachine struct {
	topo *topology.Topology
}

func (s *StateMachine) Save() ([]byte, error) { // for goraft
	state := MaxVolumeIdCommand{
		MaxVolumeId: s.topo.GetMaxVolumeId(),
	}
	glog.V(1).Infof("Save raft state %+v", state)
	return json.Marshal(state)
}

func (s *StateMachine) Recovery(data []byte) error { // for goraft
	state := MaxVolumeIdCommand{}
	err := json.Unmarshal(data, &state)
	if err != nil {
		return err
	}
	glog.V(1).Infof("Recovery raft state %+v", state)
	s.topo.UpAdjustMaxVolumeId(state.MaxVolumeId)
	return nil
}
