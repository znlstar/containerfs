package main

import (
	"github.com/tiglabs/containerfs/logger"
	"github.com/tiglabs/containerfs/proto/vp"
	"golang.org/x/net/context"
	"time"

	"github.com/shirou/gopsutil/cpu"
	"github.com/shirou/gopsutil/disk"
	"github.com/shirou/gopsutil/load"
	"github.com/shirou/gopsutil/mem"
	"github.com/shirou/gopsutil/net"
)

// rpc ClusterInfo(ClusterInfoReq) returns (ClusterInfoAck){};
func (s *VolMgrServer) ClusterInfo(ctx context.Context, in *vp.ClusterInfoReq) (*vp.ClusterInfoAck, error) {
	ack := vp.ClusterInfoAck{}
	ack.MetaNum = 3

	v, err := s.Cluster.RaftGroup.DataNodeGetAll(1)
	if err != nil {
		logger.Error("GetAllDataNode Info failed:%v for ClusterInfo", err)
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

	volumes, err := s.Cluster.RaftGroup.VolumeGetAll(1)
	if err != nil {
		logger.Error("GetAllVolume Info failed:%v for ClusterInfo", err)
		ack.Ret = -1
		return &ack, nil
	}

	ack.VolNum = int32(len(volumes))

	logger.Debug("ClusterInfo: %v", ack)

	return &ack, nil
}

//todo: not implemented yet
func (s *VolMgrServer) GetMetaNode(ctx context.Context, in *vp.GetAllMetaNodeReq) (*vp.GetAllMetaNodeAck, error) {
	ack := vp.GetAllMetaNodeAck{}
	if mns, err := s.Cluster.RaftGroup.MetaNodeGetAll(1); err != nil {
		ack.Ret = -1
		return &ack, err
	} else {
		ack.MetaNodes = mns
	}
	return &ack, nil
}

// todo: not implemented yet
// rpc MetaNodeInfo(MetaNodeInfoReq) returns (MetaNodeInfoAck){};
func (s *VolMgrServer) MetaNodeInfo(ctx context.Context, in *vp.MetaNodeInfoReq) (*vp.MetaNodeInfoAck, error) {
	ack := vp.MetaNodeInfoAck{}

	return &ack, nil
}

// rpc VolMgrInfo(VolMgrInfoReq) returns (VolMgrInfoAck){};
func (s *VolMgrServer) VolMgrInfo(ctx context.Context, in *vp.VolMgrInfoReq) (*vp.VolMgrInfoAck, error) {
	ack := vp.VolMgrInfoAck{}
	ack.VolMgrID = s.NodeID
	ack.IsLeader = s.RaftServer.IsLeader(s.NodeID)
	return &ack, nil
}

// rpc VolumeInfo(VolumeInfoReq) returns (VolumeInfoAck){};
func (s *VolMgrServer) VolumeInfos(ctx context.Context, in *vp.VolumeInfosReq) (*vp.VolumeInfosAck, error) {
	ack := vp.VolumeInfosAck{}

	v, err := s.Cluster.RaftGroup.VolumeGetAll(1)
	if err != nil {
		logger.Error("GetAllVolume Info failed:%v for VolumeInfo", err)
		ack.Ret = -1
		return &ack, nil
	}

	for _, vv := range v {
		volume := vp.Volume{}
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

func (s *VolMgrServer) GetVolInfo(ctx context.Context, in *vp.GetVolInfoReq) (*vp.GetVolInfoAck, error) {
	ack := vp.GetVolInfoAck{}

	volume, err := s.Cluster.RaftGroup.VolumeGet(1, in.UUID)
	if err != nil {
		return &ack, nil
	}

	ack.Volume = volume
	ack.Ret = 0

	return &ack, nil
}
func (s *VolMgrServer) GetBlockGroupInfo(ctx context.Context, in *vp.GetBlockGroupInfoReq) (*vp.GetBlockGroupInfoAck, error) {
	ack := vp.GetBlockGroupInfoAck{}
	blockGroup, err := s.Cluster.RaftGroup.BlockGroupGet(in.BGID)
	if err != nil {
		return &ack, err
	}

	ack.BlockGroup = blockGroup
	return &ack, nil
}

// rpc NodeMonitor(NodeMonitorReq) returns (NodeMonitorAck){};
func (s *VolMgrServer) NodeMonitor(ctx context.Context, in *vp.NodeMonitorReq) (*vp.NodeMonitorAck, error) {
	ack := vp.NodeMonitorAck{NodeInfo: &vp.NodeInfo{}}

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

	diskUsage, _ := disk.Usage(VolMgrServerAddr.waldir)
	ack.NodeInfo.PathUsedPercent = diskUsage.UsedPercent
	ack.NodeInfo.PathTotal = diskUsage.Total
	ack.NodeInfo.PathFree = diskUsage.Free

	disksIO, _ := disk.IOCounters()
	for _, v := range disksIO {
		diskio := vp.DiskIO{}
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
		netio := vp.NetIO{}
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
