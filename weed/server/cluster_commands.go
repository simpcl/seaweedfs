package weed_server

import (
	"encoding/json"
	"fmt"
	"io"
	"weed/glog"
	"weed/storage"
	"weed/topology"

	hashicorpRaft "github.com/hashicorp/raft"
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

func (c *MaxVolumeIdCommand) Apply(server goRaft.Server) (interface{}, error) { // for goraft
	topo := server.StateMachine().(*StateMachine).topo
	before := topo.GetMaxVolumeId()
	topo.UpAdjustMaxVolumeId(c.MaxVolumeId)

	glog.V(2).Infoln("goraft apply: max volume id", before, "==>", topo.GetMaxVolumeId())

	return nil, nil
}

func (c *MaxVolumeIdCommand) Persist(sink hashicorpRaft.SnapshotSink) error { // for hashicorp raft
	b, err := json.Marshal(c)
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

func (c *MaxVolumeIdCommand) Release() { // for hashicorp raft
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

func (s *StateMachine) Apply(l *hashicorpRaft.Log) interface{} { // for hashicorp raft
	glog.V(4).Infof("Apply raft log to state machine")
	before := s.topo.GetMaxVolumeId()
	state := MaxVolumeIdCommand{}
	err := json.Unmarshal(l.Data, &state)
	if err != nil {
		return err
	}
	s.topo.UpAdjustMaxVolumeId(state.MaxVolumeId)

	glog.V(2).Infoln("Apply raft log: max volume id", before, "==>", s.topo.GetMaxVolumeId())
	return nil
}

func (s *StateMachine) Snapshot() (hashicorpRaft.FSMSnapshot, error) { // for hashicorp raft
	return &MaxVolumeIdCommand{
		MaxVolumeId: s.topo.GetMaxVolumeId(),
	}, nil
}

func (s *StateMachine) Restore(r io.ReadCloser) error { // for hashicorp raft
	glog.V(4).Infof("Restore raft state machine")
	data, err := io.ReadAll(r)
	if err != nil {
		return err
	}
	state := MaxVolumeIdCommand{}
	err = json.Unmarshal(data, &state)
	if err != nil {
		return err
	}
	glog.V(1).Infof("Restore raft state: %+v", state)
	s.topo.UpAdjustMaxVolumeId(state.MaxVolumeId)
	return nil
}
