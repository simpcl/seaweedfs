package weed_server

import (
	"net"
	"strings"
	"time"

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
			glog.V(0).Infof("SendHeartbeat stream recv error: %s", err.Error())
			if dn != nil {
				glog.V(0).Infof("unregister volume server %s:%d", dn.Ip, dn.Port)
				t.UnRegisterDataNode(dn)
			}
			return err
		}
		glog.V(1).Infof("SendHeartbeat stream recv from %v:%d", heartbeat.GetIp(), heartbeat.GetPort())

		if ms.raftServer == nil {
			time.Sleep(1 * time.Second)
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

		// tell the volume servers about the leader
		newLeader, err := ms.raftServer.Leader()
		if err == nil {
			if err := stream.Send(&master_pb.HeartbeatResponse{
				Leader: newLeader,
			}); err != nil {
				glog.V(0).Infof("SendHeartbeat stream send response to %s:%d error: %s", dn.Ip, dn.Port, err.Error())
				if dn != nil {
					glog.V(0).Infof("unregister volume server %s:%d", dn.Ip, dn.Port)
					t.UnRegisterDataNode(dn)
				}
				return err
			}
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
