package weed_server

import (
	"net"
	"strings"

	"weed/glog"
	"weed/pb/master_pb"
	"weed/storage"
	"weed/topology"

	"google.golang.org/grpc/peer"
)

func (ms *MasterServer) SendHeartbeat(stream master_pb.Seaweed_SendHeartbeatServer) error {
	var dn *topology.DataNode
	t := ms.Topo
	for {
		heartbeat, err := stream.Recv()
		if err != nil {
			if dn != nil {
				glog.Warningf("SendHeartbeat.Recv server %s:%d : %v", dn.Ip, dn.Port, err)
			} else {
				glog.Warningf("SendHeartbeat.Recv: %v", err)
			}
			return err
		}

		// tell the volume servers about the leader
		if ms.raftServer == nil {
			continue
		}

		if !ms.raftServer.IsLeader() {
			newLeader, err := ms.raftServer.Leader()
			if err != nil {
				glog.Warningf("SendHeartbeat find leader: %v", err)
				return err
			}
			if err := stream.Send(&master_pb.HeartbeatResponse{
				Leader: string(newLeader),
			}); err != nil {
				if dn != nil {
					glog.Warningf("SendHeartbeat.Send response to %s:%d %v", dn.Ip, dn.Port, err)
				} else {
					glog.Warningf("SendHeartbeat.Send response %v", err)
				}
				return err
			}
			continue
		}

		if dn == nil {
			t.Sequence.SetMax(heartbeat.MaxFileKey)
			if heartbeat.Ip == "" {
				if pr, ok := peer.FromContext(stream.Context()); ok {
					if pr.Addr != net.Addr(nil) {
						heartbeat.Ip = pr.Addr.String()[0:strings.LastIndex(pr.Addr.String(), ":")]
						glog.V(0).Infof("remote IP address is detected as %v", heartbeat.Ip)
					}
				}
			}
			dcName, rackName := t.Configuration.Locate(heartbeat.Ip, heartbeat.DataCenter, heartbeat.Rack)
			dc := t.GetOrCreateDataCenter(dcName)
			rack := dc.GetOrCreateRack(rackName)
			dn = rack.GetOrCreateDataNode(heartbeat.Ip,
				int(heartbeat.Port), heartbeat.PublicUrl,
				int(heartbeat.MaxVolumeCount))
			glog.V(0).Infof("added volume server %v:%d", heartbeat.GetIp(), heartbeat.GetPort())
			if err := stream.Send(&master_pb.HeartbeatResponse{
				VolumeSizeLimit: uint64(ms.volumeSizeLimitMB) * 1024 * 1024,
				SecretKey:       string(ms.guard.SecretKey),
			}); err != nil {
				return err
			}
		}

		var volumeInfos []storage.VolumeInfo
		for _, v := range heartbeat.Volumes {
			if vi, err := storage.NewVolumeInfo(v); err == nil {
				volumeInfos = append(volumeInfos, vi)
			} else {
				glog.V(0).Infof("Fail to convert joined volume information: %v", err)
			}
		}
		deletedVolumes := dn.UpdateVolumes(volumeInfos)
		for _, v := range volumeInfos {
			t.RegisterVolumeLayout(v, dn)
		}
		for _, v := range deletedVolumes {
			t.UnRegisterVolumeLayout(v, dn)
		}
	}
}

func (ms *MasterServer) KeepConnected(stream master_pb.Seaweed_KeepConnectedServer) error {
	for {
		_, err := stream.Recv()
		if err != nil {
			return err
		}
		if err := stream.Send(&master_pb.Empty{}); err != nil {
			return err
		}
	}
}
