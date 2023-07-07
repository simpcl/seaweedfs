package topology

import (
	"fmt"

	"weed/storage"
)

/*
This package is created to resolve these replica placement issues:
1. growth factor for each replica level, e.g., add 10 volumes for 1 copy, 20 volumes for 2 copies, 30 volumes for 3 copies
2. in time of tight storage, how to reduce replica level
3. optimizing for hot data on faster disk, cold data on cheaper storage,
4. volume allocation for each bucket
*/

type VolumeOption struct {
	Collection       string
	ReplicaPlacement *storage.ReplicaPlacement
	Ttl              *storage.TTL
	Preallocate      int64
	DataCenter       string
	Rack             string
	DataNode         string
}

func (o *VolumeOption) String() string {
	return fmt.Sprintf("Collection:%s, ReplicaPlacement:%v, Ttl:%v, DataCenter:%s, Rack:%s, DataNode:%s", o.Collection, o.ReplicaPlacement, o.Ttl, o.DataCenter, o.Rack, o.DataNode)
}
