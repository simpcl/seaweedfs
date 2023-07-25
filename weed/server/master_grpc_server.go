package weed_server

import (
	"errors"
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

	defer func() {
		if dn != nil {
			glog.V(0).Infof("SendHeartbeat before exit, unregister volume server %s:%d", dn.Ip, dn.Port)
			t.UnRegisterDataNode(dn)
			dn = nil
		}
	}()

	for {
		heartbeat, err := stream.Recv()
		if err != nil {
			glog.V(0).Infof("SendHeartbeat stream recv error: %s", err.Error())
			return err
		}
		if dn == nil {
			glog.V(0).Infof("SendHeartbeat stream recv from %v:%d when dn is nil", heartbeat.GetIp(), heartbeat.GetPort())
		} else {
			glog.V(2).Infof("SendHeartbeat stream recv from %v:%d, dn: %s:%d",
				heartbeat.GetIp(), heartbeat.GetPort(), dn.Ip, dn.Port)
		}

		if ms.raftServer == nil {
			err = errors.New("raftServer is nil")
			glog.V(0).Infof("SendHeartbeat error: %s", err.Error())
			return err
		}
		if !ms.raftServer.IsLeader() {
			glog.V(0).Infof("SendHeartbeat i am not leader, leader: %s", ms.raftServer.Leader())
			return stream.Send(&master_pb.HeartbeatResponse{
				Leader: ms.raftServer.Leader(),
			})
		}

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
		glog.V(2).Infof("added volume server %v:%d", heartbeat.GetIp(), heartbeat.GetPort())
		if err := stream.Send(&master_pb.HeartbeatResponse{
			VolumeSizeLimit: uint64(ms.volumeSizeLimitMB) * 1024 * 1024,
			SecretKey:       string(ms.guard.SecretKey),
			Leader:          ms.raftServer.Leader(),
		}); err != nil {
			glog.V(0).Infof("SendHeartbeat stream send response to %s:%d error: %s", heartbeat.GetIp(), heartbeat.GetPort(), err.Error())
			return err
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
