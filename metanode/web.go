package main

import (
	pbproto "github.com/golang/protobuf/proto"
	"github.com/tigcode/containerfs/logger"
	ns "github.com/tigcode/containerfs/metanode/namespace"
	"github.com/tigcode/containerfs/proto/mp"
	"golang.org/x/net/context"
	"time"

	"github.com/shirou/gopsutil/cpu"
	"github.com/shirou/gopsutil/disk"
	"github.com/shirou/gopsutil/load"
	"github.com/shirou/gopsutil/mem"
	"github.com/shirou/gopsutil/net"
)

// rpc ClusterInfo(ClusterInfoReq) returns (ClusterInfoAck){};
func (s *MetaNodeServer) ClusterInfo(ctx context.Context, in *mp.ClusterInfoReq) (*mp.ClusterInfoAck, error) {
	ack := mp.ClusterInfoAck{}
	ret, nameSpace := ns.GetNameSpace("Cluster")
	if ret != 0 {
		ack.Ret = ret
		return &ack, nil
	}

	ack.MetaNum = 3

	v, err := nameSpace.GetAllDatanode()
	if err != nil {
		logger.Error("GetAllDatanode Info failed:%v for ClusterInfo", err)
		ack.Ret = 1
		return &ack, nil
	}

	ack.DataNum = int32(len(v))

	var total int32
	var free int32

	for _, vv := range v {
		total = total + vv.Capacity
		free = free + vv.Free
	}
	ack.ClusterSpace = total
	ack.ClusterFreeSpace = free

	volumes, err := nameSpace.GetAllVolume()
	if err != nil {
		logger.Error("GetAllVolume Info failed:%v for ClusterInfo", err)
		ack.Ret = ret
		return &ack, nil
	}

	ack.VolNum = int32(len(volumes))

	logger.Debug("ClusterInfo: %v", ack)

	return &ack, nil
}

// rpc MetaNodeInfo(MetaNodeInfoReq) returns (MetaNodeInfoAck){};
func (s *MetaNodeServer) MetaNodeInfo(ctx context.Context, in *mp.MetaNodeInfoReq) (*mp.MetaNodeInfoAck, error) {
	ack := mp.MetaNodeInfoAck{}
	ack.MetaID = s.NodeID
	ack.IsLeader = s.RaftServer.IsLeader(s.NodeID)
	ack.AppliedIndex = s.RaftServer.AppliedIndex(s.NodeID)
	return &ack, nil
}

// rpc VolumeInfo(VolumeInfoReq) returns (VolumeInfoAck){};
func (s *MetaNodeServer) VolumeInfos(ctx context.Context, in *mp.VolumeInfosReq) (*mp.VolumeInfosAck, error) {
	ack := mp.VolumeInfosAck{}
	ret, nameSpace := ns.GetNameSpace("Cluster")
	if ret != 0 {
		ack.Ret = ret
		return &ack, nil
	}

	v, err := nameSpace.GetAllVolume()
	if err != nil {
		logger.Error("GetAllVolume Info failed:%v for VolumeInfo", err)
		ack.Ret = ret
		return &ack, nil
	}

	for _, vv := range v {
		volume := mp.Volume{}
		volume.RGID = vv.RGID
		volume.TotalSize = vv.TotalSize
		volume.AllocatedSize = vv.AllocatedSize
		volume.UUID = vv.UUID
		volume.Name = vv.Name
		volume.Tier = vv.Tier

		ack.Volumes = append(ack.Volumes, &volume)
	}

	logger.Debug("VolumeInfos: %v", ack.Volumes)

	return &ack, nil
}

func (s *MetaNodeServer) GetVolInfo(ctx context.Context, in *mp.GetVolInfoReq) (*mp.GetVolInfoAck, error) {
	ack := mp.GetVolInfoAck{}
	ret, nameSpace := ns.GetNameSpace("Cluster")
	if ret != 0 {
		logger.Error("Get Cluster NameSpace for GetVolInfo failed, ret:%v", ret)
		ack.Ret = ret
		return &ack, nil
	}

	/*
		v, err := nameSpace.RaftGroup.VOLGet(1, in.UUID)
		if err != nil {
			logger.Error("Get Volume:%v info failed for GetVolInfo, err:%v", in.UUID, err)
			return &ack, err
		}
		volume := mp.Volume{}
		err = pbproto.Unmarshal(v, &volume)
		if err != nil {
			return &ack, err
		}
		ack.VolInfo = &volume
	*/
	value, err := nameSpace.RaftGroup.BGPGetRange(1, in.UUID)
	if err != nil {
		logger.Error("Get Volume:%v BGPS info failed for GetVolInfo, err:%v", in.UUID, err)
		return &ack, err
	}

	tBGPS := make([]*mp.BGP, 0)
	for _, v := range value {
		bgp := &mp.BGP{}

		err := pbproto.Unmarshal(v.V, bgp)
		if err != nil {
			return &ack, err
		}
		tBGPS = append(tBGPS, bgp)
	}

	ack.BGPS = tBGPS
	ack.Ret = 0

	logger.Debug("GetVolInfo: %v", ack.BGPS)

	return &ack, nil
}

// rpc NodeMonitor(NodeMonitorReq) returns (NodeMonitorAck){};
func (s *MetaNodeServer) NodeMonitor(ctx context.Context, in *mp.NodeMonitorReq) (*mp.NodeMonitorAck, error) {
	ack := mp.NodeMonitorAck{NodeInfo: &mp.NodeInfo{}}

	cpuUsage, err := cpu.Percent(time.Millisecond*500, false)
	if err == nil {
		ack.NodeInfo.CpuUsage = cpuUsage[0]
	} else {
		logger.Error("NodeMonitor get cpu usage failed !")
	}

	cpuLoad, _ := load.Avg()
	ack.NodeInfo.CpuLoad = cpuLoad.Load1

	memv, _ := mem.VirtualMemory()
	ack.NodeInfo.TotalMem = memv.Total
	ack.NodeInfo.FreeMem = memv.Free
	ack.NodeInfo.MemUsedPercent = memv.UsedPercent

	diskUsage, _ := disk.Usage(MetaNodeServerAddr.waldir)
	ack.NodeInfo.PathUsedPercent = diskUsage.UsedPercent
	ack.NodeInfo.PathTotal = diskUsage.Total
	ack.NodeInfo.PathFree = diskUsage.Free

	disksIO, _ := disk.IOCounters()
	for _, v := range disksIO {
		diskio := mp.DiskIO{}
		diskio.IoTime = v.IoTime
		diskio.IopsInProgress = v.IopsInProgress
		diskio.Name = v.Name
		diskio.ReadBytes = v.ReadBytes
		diskio.ReadCount = v.ReadCount
		diskio.WeightedIO = v.WeightedIO
		diskio.WriteBytes = v.WriteBytes
		diskio.WriteCount = v.WriteCount
		ack.NodeInfo.DiskIOs = append(ack.NodeInfo.DiskIOs, &diskio)
	}

	NetsIO, _ := net.IOCounters(true)
	for _, v := range NetsIO {
		netio := mp.NetIO{}
		netio.BytesRecv = v.BytesRecv
		netio.BytesSent = v.BytesSent
		netio.Dropin = v.Dropin
		netio.Dropout = v.Dropout
		netio.Errin = v.Errin
		netio.Errout = v.Errout
		netio.Name = v.Name
		netio.PacketsRecv = v.PacketsRecv
		netio.PacketsSent = v.PacketsSent
		ack.NodeInfo.NetIOs = append(ack.NodeInfo.NetIOs, &netio)
	}

	logger.Debug("NodeMonitor: %v", ack.NodeInfo)

	return &ack, nil
}
