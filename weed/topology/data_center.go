package topology

import "sync"

type DataCenter struct {
	NodeImpl
	mut sync.Mutex
}

func NewDataCenter(id string) *DataCenter {
	dc := &DataCenter{}
	dc.id = NodeId(id)
	dc.nodeType = "DataCenter"
	dc.children = make(map[NodeId]Node)
	dc.NodeImpl.value = dc
	return dc
}

func (dc *DataCenter) GetOrCreateRack(rackName string) *Rack {
	dc.mut.Lock()
	defer dc.mut.Unlock()
	for _, c := range dc.Children() {
		rack := c.(*Rack)
		if string(rack.Id()) == rackName {
			return rack
		}
	}
	rack := NewRack(rackName)
	dc.LinkChildNode(rack)
	return rack
}

func (dc *DataCenter) ToMap() interface{} {
	m := make(map[string]interface{})
	m["Id"] = dc.Id()
	m["Max"] = dc.GetMaxVolumeCount()
	m["Free"] = dc.FreeSpace()
	var racks []interface{}
	for _, c := range dc.Children() {
		rack := c.(*Rack)
		racks = append(racks, rack.ToMap())
	}
	m["Racks"] = racks
	return m
}
