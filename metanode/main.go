package main

import (
	"flag"
	"fmt"
	pbproto "github.com/golang/protobuf/proto"
	"github.com/tigcode/containerfs/logger"
	ns "github.com/tigcode/containerfs/metanode/namespace"
	"github.com/tigcode/containerfs/metanode/raftopt"
	dp "github.com/tigcode/containerfs/proto/dp"
	mp "github.com/tigcode/containerfs/proto/mp"
	"github.com/tigcode/containerfs/utils"
	"github.com/tigcode/raft"
	"github.com/tigcode/raft/proto"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	//"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/reflection"
	"math/rand"
	"net"
	"net/http"
	_ "net/http/pprof"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"
)

type addr struct {
	host   string
	nodeID uint64
	peers  []proto.Peer
	ips    []string
	waldir string
	log    string
}

const (
	BlkSizeG      = 5
	BlkSize       = 5 * 1024 * 1024 * 1024  /*one blksize 5G*/
	OneExpandSize = 30 * 1024 * 1024 * 1024 /*allocated volumesize 30G for each time*/
)

// MetaNodeServerAddr ...
var MetaNodeServerAddr addr

// MetaNodeServer ...
type MetaNodeServer struct {
	NodeID     uint64
	Addr       *raftopt.Address
	Resolver   *raftopt.Resolver
	RaftServer *raft.RaftServer
	sync.Mutex
}

// GetMetaLeader ...
func (s *MetaNodeServer) GetMetaLeader(ctx context.Context, in *mp.GetMetaLeaderReq) (*mp.GetMetaLeaderAck, error) {
	ack := mp.GetMetaLeaderAck{}
	ret, nameSpace := ns.GetNameSpace(in.VolID)
	if ret != 0 {
		ack.Ret = ret
		return &ack, nil
	}
	leaderID, _ := s.RaftServer.LeaderTerm(nameSpace.RaftGroupID)
	if leaderID <= 0 {
		ack.Ret = 1
		return &ack, nil
	}
	ack.Ret = 0
	ack.Leader = raftopt.AddrDatabase[leaderID].Grpc
	return &ack, nil
}

//CreateNameSpace ...
func (s *MetaNodeServer) CreateNameSpace(ctx context.Context, in *mp.CreateNameSpaceReq) (*mp.CreateNameSpaceAck, error) {
	ack := mp.CreateNameSpaceAck{}
	ack.Ret = ns.CreateNameSpace(s.RaftServer, MetaNodeServerAddr.peers, MetaNodeServerAddr.nodeID, MetaNodeServerAddr.waldir, in.VolID, in.RaftGroupID, false)
	return &ack, nil
}

//ExpandNameSpace ...
func (s *MetaNodeServer) ExpandNameSpace(ctx context.Context, in *mp.ExpandNameSpaceReq) (*mp.ExpandNameSpaceAck, error) {

	ack := mp.ExpandNameSpaceAck{}

	ret, nameSpace := ns.GetNameSpace(in.VolID)
	if ret != 0 {
		ack.Ret = ret
		return &ack, nil
	}
	ack.Ret = nameSpace.ExpandNameSpace(in.BlockGroups)

	return &ack, nil
}

// SnapShootNameSpace ...
func (s *MetaNodeServer) SnapShootNameSpace(ctx context.Context, in *mp.SnapShootNameSpaceReq) (*mp.SnapShootNameSpaceAck, error) {
	ack := mp.SnapShootNameSpaceAck{}
	ack.Ret = ns.SnapShootNameSpace(s.RaftServer, in.VolID, MetaNodeServerAddr.waldir)
	// send to follower metadatas to SnapShoot
	if in.Type == 0 {
		for _, addr := range raftopt.AddrDatabase {
			if addr.Grpc == s.Addr.Grpc {
				continue
			}
			conn2, err2 := grpc.Dial(addr.Grpc, grpc.WithInsecure())
			if err2 != nil {
				logger.Error("told peers to SnapShoot NameSpace Failed ...")
				continue
			}
			defer conn2.Close()
			mc := mp.NewMetaNodeClient(conn2)
			pmSnapShootNameSpaceReq := &mp.SnapShootNameSpaceReq{
				VolID: in.VolID,
				Type:  1,
			}
			pmSnapShootNameSpaceAck, ret := mc.SnapShootNameSpace(context.Background(), pmSnapShootNameSpaceReq)
			if ret != nil {
				logger.Error("told peers to SnapShoot NameSpace Failed ...")
				continue
			}
			if pmSnapShootNameSpaceAck.Ret != 0 {
				logger.Error("told peers to SnapShoot NameSpace Failed ...")
				continue
			}
		}
	}
	return &ack, nil
}

// DeleteNameSpace ...
func (s *MetaNodeServer) DeleteNameSpace(ctx context.Context, in *mp.DeleteNameSpaceReq) (*mp.DeleteNameSpaceAck, error) {
	ack := mp.DeleteNameSpaceAck{}
	ack.Ret = ns.DeleteNameSpace(s.RaftServer, in.VolID)

	// send to follower metadatas to delete
	if in.Type == 0 {
		for _, addr := range raftopt.AddrDatabase {
			if addr.Grpc == s.Addr.Grpc {
				continue
			}
			conn2, err2 := grpc.Dial(addr.Grpc, grpc.WithInsecure())
			if err2 != nil {
				logger.Error("told peers to  delete NameSpace Failed ...")
				continue
			}
			defer conn2.Close()
			mc := mp.NewMetaNodeClient(conn2)
			pmDeleteNameSpaceReq := &mp.DeleteNameSpaceReq{
				VolID: in.VolID,
				Type:  1,
			}
			pmDeleteNameSpaceAck, ret := mc.DeleteNameSpace(context.Background(), pmDeleteNameSpaceReq)
			if ret != nil {
				logger.Error("told peers to  delete NameSpace Failed ...")
				continue
			}
			if pmDeleteNameSpaceAck.Ret != 0 {
				logger.Error("told peers to  delete NameSpace Failed ...")
				continue
			}
		}
	}

	return &ack, nil
}

//GetFSInfo ...
func (s *MetaNodeServer) GetFSInfo(ctx context.Context, in *mp.GetFSInfoReq) (*mp.GetFSInfoAck, error) {
	ack := mp.GetFSInfoAck{}

	ret, nameSpace := ns.GetNameSpace(in.VolID)
	if ret != 0 {
		ack.Ret = ret
		return &ack, nil
	}

	ack = nameSpace.GetFSInfo(in.VolID)
	return &ack, nil
}

//CreateDirDirect ...
func (s *MetaNodeServer) CreateDirDirect(ctx context.Context, in *mp.CreateDirDirectReq) (*mp.CreateDirDirectAck, error) {
	ack := mp.CreateDirDirectAck{}
	ret, nameSpace := ns.GetNameSpace(in.VolID)
	if ret != 0 {
		ack.Ret = ret
		return &ack, nil
	}
	ack.Ret, ack.Inode = nameSpace.CreateDirDirect(in.PInode, in.Name)
	return &ack, nil
}

//GetInodeInfoDirect ...
func (s *MetaNodeServer) GetInodeInfoDirect(ctx context.Context, in *mp.GetInodeInfoDirectReq) (*mp.GetInodeInfoDirectAck, error) {
	ack := mp.GetInodeInfoDirectAck{}
	ret, nameSpace := ns.GetNameSpace(in.VolID)
	if ret != 0 {
		ack.Ret = ret
		return &ack, nil
	}
	ack.Ret, ack.InodeInfo, ack.Inode = nameSpace.GetInodeInfoDirect(in.PInode, in.Name)
	return &ack, nil
}

//StatDirect ...
func (s *MetaNodeServer) StatDirect(ctx context.Context, in *mp.StatDirectReq) (*mp.StatDirectAck, error) {
	ack := mp.StatDirectAck{}
	ret, nameSpace := ns.GetNameSpace(in.VolID)
	if ret != 0 {
		ack.Ret = ret
		return &ack, nil
	}
	ack.InodeType, ack.Inode, ack.Ret = nameSpace.StatDirect(in.PInode, in.Name)
	return &ack, nil
}

//ListDirect ...
func (s *MetaNodeServer) ListDirect(ctx context.Context, in *mp.ListDirectReq) (*mp.ListDirectAck, error) {
	ack := mp.ListDirectAck{}

	ret, nameSpace := ns.GetNameSpace(in.VolID)
	if ret != 0 {
		ack.Ret = ret
		return &ack, nil
	}
	ack.Dirents, ack.Ret = nameSpace.ListDirect(in.PInode)
	return &ack, nil
}

// DeleteDirDirect ...
func (s *MetaNodeServer) DeleteDirDirect(ctx context.Context, in *mp.DeleteDirDirectReq) (*mp.DeleteDirDirectAck, error) {

	ack := mp.DeleteDirDirectAck{}

	ret, nameSpace := ns.GetNameSpace(in.VolID)
	if ret != 0 {
		ack.Ret = ret
		return &ack, nil
	}
	ack.Ret = nameSpace.DeleteDirDirect(in.PInode, in.Name)
	return &ack, nil

}

// RenameDirect ...
func (s *MetaNodeServer) RenameDirect(ctx context.Context, in *mp.RenameDirectReq) (*mp.RenameDirectAck, error) {
	ack := mp.RenameDirectAck{}

	ret, nameSpace := ns.GetNameSpace(in.VolID)
	if ret != 0 {
		ack.Ret = ret
		return &ack, nil
	}
	ack.Ret = nameSpace.RenameDirect(in.OldPInode, in.OldName, in.NewPInode, in.NewName)
	return &ack, nil
}

//CreateFileDirect ...
func (s *MetaNodeServer) CreateFileDirect(ctx context.Context, in *mp.CreateFileDirectReq) (*mp.CreateFileDirectAck, error) {
	ack := mp.CreateFileDirectAck{}
	ret, nameSpace := ns.GetNameSpace(in.VolID)
	if ret != 0 {
		ack.Ret = ret
		return &ack, nil
	}
	ack.Ret, ack.Inode = nameSpace.CreateFileDirect(in.PInode, in.Name)
	return &ack, nil
}

// DeleteFileDirect ...
func (s *MetaNodeServer) DeleteFileDirect(ctx context.Context, in *mp.DeleteFileDirectReq) (*mp.DeleteFileDirectAck, error) {

	ack := mp.DeleteFileDirectAck{}

	ret, nameSpace := ns.GetNameSpace(in.VolID)
	if ret != 0 {
		ack.Ret = ret
		return &ack, nil
	}
	ack.Ret = nameSpace.DeleteFileDirect(in.PInode, in.Name)
	return &ack, nil

}

// GetFileChunksDirect ...
func (s *MetaNodeServer) GetFileChunksDirect(ctx context.Context, in *mp.GetFileChunksDirectReq) (*mp.GetFileChunksDirectAck, error) {
	ack := mp.GetFileChunksDirectAck{}

	ret, nameSpace := ns.GetNameSpace(in.VolID)
	if ret != 0 {
		ack.Ret = ret
		return &ack, nil
	}
	ok, chunkInfos, inode := nameSpace.GetFileChunksDirect(in.PInode, in.Name)
	if ok != 0 {
		ack.Ret = ok
		return &ack, nil
	}

	ret, nameSpace = ns.GetNameSpace("Cluster")
	if ret != 0 {
		ack.Ret = ret
		return &ack, nil
	}
	for _, v := range chunkInfos {
		var chunkInfoWithBG mp.ChunkInfoWithBG
		chunkInfoWithBG.ChunkID = v.ChunkID
		chunkInfoWithBG.ChunkSize = v.ChunkSize

		bgKey := in.VolID + fmt.Sprintf("-%d", v.BlockGroupID)

		blockGroup, err := nameSpace.RaftGroup.BGPGet(1, bgKey)
		if err != nil {
			ack.Ret = 1
			return &ack, nil
		}

		bgp := &mp.BGP{}

		err = pbproto.Unmarshal(blockGroup, bgp)
		if err != nil {
			ack.Ret = 1
			return &ack, nil
		}

		chunkInfoWithBG.BGP = bgp
		ack.ChunkInfos = append(ack.ChunkInfos, &chunkInfoWithBG)

	}

	ack.Ret = 0
	ack.Inode = inode

	return &ack, nil
}

// AllocateChunk ...
func (s *MetaNodeServer) AllocateChunk(ctx context.Context, in *mp.AllocateChunkReq) (*mp.AllocateChunkAck, error) {
	ack := mp.AllocateChunkAck{}

	ret, nameSpace := ns.GetNameSpace(in.VolID)
	if ret != 0 {
		ack.Ret = ret
		return &ack, nil
	}
	ret, chunkInfo := nameSpace.AllocateChunk()
	if ret != 0 {
		ack.Ret = ret
		return &ack, nil
	}

	ack.ChunkInfo = chunkInfo
	return &ack, nil
}

// SyncChunk ...
func (s *MetaNodeServer) SyncChunk(ctx context.Context, in *mp.SyncChunkReq) (*mp.SyncChunkAck, error) {
	ack := mp.SyncChunkAck{}
	chunkinfo := in.ChunkInfo
	ret, nameSpace := ns.GetNameSpace(in.VolID)
	if ret != 0 {
		ack.Ret = ret
		return &ack, nil
	}
	ack.Ret = nameSpace.SyncChunk(in.ParentInodeID, in.Name, chunkinfo)
	return &ack, nil
}

/*

// UpdateBlockGroup ...
func (s *MetaNodeServer) UpdateBlockGroup(ctx context.Context, in *mp.UpdateBlockGroupReq) (*mp.UpdateBlockGroupAck, error) {
	ack := mp.UpdateBlockGroupAck{}
	ret, nameSpace := ns.GetNameSpace(in.VolID)
	if ret != 0 {
		ack.Ret = ret
		return &ack, nil
	}
	ack.Ret = nameSpace.UpdateBlockGroup(in.BlockInfos)
	return &ack, nil
}

// MigrateBlockGroup ...
func (s *MetaNodeServer) MigrateBlockGroup(ctx context.Context, in *mp.MigrateBlockGroupReq) (*mp.MigrateBlockGroupAck, error) {
	ack := mp.MigrateBlockGroupAck{}
	ret, nameSpace := ns.GetNameSpace(in.VolID)
	if ret != 0 {
		ack.Ret = ret
		return &ack, nil
	}
	ack.Ret = nameSpace.MigrateBlockGroup(in.BlockGroupID, in.OldBlockID, in.NewBlock)
	return &ack, nil
}
*/

// for Datanode registry
func (s *MetaNodeServer) DatanodeRegistry(ctx context.Context, in *mp.Datanode) (*mp.DatanodeRegistryAck, error) {
	ack := mp.DatanodeRegistryAck{}
	ret, nameSpace := ns.GetNameSpace("Cluster")
	if ret != 0 {
		logger.Error("Get Cluster MetaNode NameSpace err for DatanodeRegistry")
		ack.Ret = ret
		return &ack, nil
	}
	ack.Ret = nameSpace.DatanodeRegistry(in)
	return &ack, nil
}

func (s *MetaNodeServer) GetAllDatanode(ctx context.Context, in *mp.GetAllDatanodeReq) (*mp.GetAllDatanodeAck, error) {
	ack := mp.GetAllDatanodeAck{}
	ret, nameSpace := ns.GetNameSpace("Cluster")
	if ret != 0 {
		logger.Error("Get Cluster MetaNode NameSpace err for GetAllDatanode")
		ack.Ret = ret
		return &ack, nil
	}

	v, err := nameSpace.GetAllDatanode()
	if err != nil {
		logger.Error("GetAllDatanode Info failed:%v", err)
		return &ack, err
	}

	ack.Datanodes = v
	ack.Ret = 0
	return &ack, nil
}

func (s *MetaNodeServer) DelDatanode(ctx context.Context, in *mp.DelDatanodeReq) (*mp.DelDatanodeAck, error) {
	ack := mp.DelDatanodeAck{}
	ret, nameSpace := ns.GetNameSpace("Cluster")
	if ret != 0 {
		logger.Error("Get Cluster MetaNode NameSpace err for DelDatanode")
		ack.Ret = ret
		return &ack, nil
	}

	port, _ := strconv.Atoi(in.Port)
	err := nameSpace.DataNodeDel(in.Ip, port)
	if err != nil {
		logger.Error("Delete DataNode(%v:%v) failed, err:%v", in.Ip, port, err)
		return &ack, err
	}
	ack.Ret = 0
	return &ack, nil
}

func generateRandomNumber(start int, end int, count int) []int {
	if end < start || (end-start) < count {
		return nil
	}

	nums := make([]int, 0)
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	for len(nums) < count {
		num := r.Intn((end - start)) + start

		exist := false
		for _, v := range nums {
			if v == num {
				exist = true
				break
			}
		}

		if !exist {
			nums = append(nums, num)
		}
	}

	return nums
}

func (s *MetaNodeServer) CreateVol(ctx context.Context, in *mp.CreateVolReq) (*mp.CreateVolAck, error) {

	s.Lock()
	defer s.Unlock()

	ack := mp.CreateVolAck{}
	voluuid, err := utils.GenUUID()
	if err != nil {
		logger.Error("Create volume uuid err:%v", err)
		return &ack, err
	}

	//the volume need block group total numbers
	var blkgrpnum int32
	if in.SpaceQuota%BlkSizeG == 0 {
		blkgrpnum = in.SpaceQuota / BlkSizeG
	} else {
		blkgrpnum = in.SpaceQuota/BlkSizeG + 1
		in.SpaceQuota = blkgrpnum * BlkSizeG
	}
	if blkgrpnum > 6 {
		blkgrpnum = 6
	}

	ret, nameSpace := ns.GetNameSpace("Cluster")
	if ret != 0 {
		logger.Error("Get Cluster NameSpace for CreateVol failed, ret:%v", ret)
		ack.Ret = ret
		return &ack, nil
	}

	rgID, err := nameSpace.AllocateRGID()
	if err != nil {
		logger.Error("Create Volume name:%v uuid:%v raftGroupID error:%v", voluuid, in.VolName, err)
		return &ack, err
	}

	vv, err := nameSpace.GetAllDatanode()
	if err != nil {
		logger.Error("GetAllDatanode Info failed:%v for CreateVol", err)
		return &ack, err
	}

	inuseNodes := make(map[string][]*mp.Datanode)
	allip := make([]string, 0)

	// pass bad status and free not enough datanodes
	for _, v := range vv {
		if v.Status != 0 || v.Free < 30 {
			continue
		}
		k := v.Ip
		inuseNodes[k] = append(inuseNodes[k], v)
	}

	for k, _ := range inuseNodes {
		allip = append(allip, k)
	}

	if len(allip) < 3 {
		logger.Error("Create Volume:%v but datanode nums:%v less than 3, so forbid CreateVol", voluuid, len(allip))
		ack.Ret = -1
		return &ack, nil
	}

	for i := int32(0); i < blkgrpnum; i++ {
		bgID, err := nameSpace.AllocateBGID()
		if err != nil {
			logger.Error("AllocateBGID for CreateVol failed, err:%v", err)
			return &ack, err
		}
		bg := &mp.BGP{Blocks: make([]*mp.Block, 0)}
		idxs := generateRandomNumber(0, len(allip), 3)
		if len(idxs) != 3 {
			ack.Ret = -1
			return &ack, nil
		}

		for n := 0; n < 3; n++ {
			block := mp.Block{}
			ipkey := allip[idxs[n]]
			idx := generateRandomNumber(0, len(inuseNodes[ipkey]), 1)
			if len(idx) <= 0 {
				ack.Ret = -1
				return &ack, nil
			}

			blockID, err := nameSpace.AllocateBlockID()
			if err != nil {
				logger.Error("AllocateBlockID for CreateVol failed, err:%v", err)
				return &ack, err
			}
			block.BlkID = blockID
			block.Ip = inuseNodes[ipkey][idx[0]].Ip
			block.Port = inuseNodes[ipkey][idx[0]].Port
			block.Path = inuseNodes[ipkey][idx[0]].MountPoint
			block.Status = inuseNodes[ipkey][idx[0]].Status
			block.BGID = bgID
			block.VolID = voluuid
			k := block.Ip + fmt.Sprintf(":%d", block.Port) + fmt.Sprintf("-%d", block.BlkID)
			v, _ := pbproto.Marshal(&block)
			err = nameSpace.RaftGroup.BlockSet(1, k, v)
			if err != nil {
				logger.Error("Create Volume:%v allocated blockgroup:%v block:%v failed:%v", voluuid, bgID, blockID, err)
				return &ack, err
			}
			bg.Blocks = append(bg.Blocks, &block)
			//update this datanode freesize
			inuseNodes[ipkey][idx[0]].Free = inuseNodes[ipkey][idx[0]].Free - 5
			key := ipkey + fmt.Sprintf(":%d", block.Port)
			val, _ := pbproto.Marshal(inuseNodes[ipkey][idx[0]])
			nameSpace.RaftGroup.DataNodeSet(1, key, val)

		}
		key := voluuid + fmt.Sprintf("-%d", bgID)
		val, _ := pbproto.Marshal(bg)
		err = nameSpace.RaftGroup.BGPSet(1, key, val)
		if err != nil {
			logger.Error("Create Volume:%v Set blockgroup:%v blocks:%v failed:%v", voluuid, bgID, bg, err)
			return &ack, err
		}
		logger.Debug("Create Volume:%v Set one blockgroup:%v blocks:%v to Cluster Map success", voluuid, bgID, bg)
	}

	vol := &mp.Volume{
		UUID:          voluuid,
		Name:          in.VolName,
		TotalSize:     in.SpaceQuota,
		AllocatedSize: blkgrpnum * 5,
		RGID:          rgID,
	}

	val, _ := pbproto.Marshal(vol)
	nameSpace.RaftGroup.VOLSet(1, voluuid, val)
	ack.Ret = 0
	ack.UUID = voluuid
	ack.RaftGroupID = rgID

	retv := ns.CreateNameSpace(s.RaftServer, MetaNodeServerAddr.peers, MetaNodeServerAddr.nodeID, MetaNodeServerAddr.waldir, voluuid, rgID, false)
	if retv != 0 {
		logger.Error("CreateNameSpace local metanode failed for CreateVol, ret:%v", retv)
		ack.Ret = -1
		return &ack, nil
	}
	// send to follower metadatas to create
	for _, addr := range raftopt.AddrDatabase {

		logger.Debug("CreateNameSpace peer addr.Grpc %v s.Addr.Grpc %v", addr.Grpc, s.Addr.Grpc)

		if addr.Grpc == s.Addr.Grpc {
			continue
		}

		conn2, err2 := grpc.Dial(addr.Grpc, grpc.WithInsecure())
		if err2 != nil {
			logger.Error("told peers to  create NameSpace Failed ,err %v", err2)
			return &ack, err2
		}
		defer conn2.Close()
		mc := mp.NewMetaNodeClient(conn2)
		pmCreateNameSpaceReq := &mp.CreateNameSpaceReq{
			VolID:       voluuid,
			RaftGroupID: rgID,
			Type:        1,
		}
		pmCreateNameSpaceAck, err3 := mc.CreateNameSpace(context.Background(), pmCreateNameSpaceReq)
		if err3 != nil {
			logger.Error("told peers to  create NameSpace Failed ,err %v", err3)
			return &ack, err3
		}
		if pmCreateNameSpaceAck.Ret != 0 {
			logger.Error("told peers to create NameSpace Failed ...")
			ack.Ret = -1
			return &ack, nil
		}
	}

	return &ack, nil
}

func (s *MetaNodeServer) ExpandVolTS(ctx context.Context, in *mp.ExpandVolTSReq) (*mp.ExpandVolTSAck, error) {
	ack := mp.ExpandVolTSAck{}
	ret, nameSpace := ns.GetNameSpace("Cluster")
	if ret != 0 {
		logger.Error("Get Cluster NameSpace for ExpandVolTS failed, ret:%v", ret)
		ack.Ret = ret
		return &ack, nil
	}

	v, err := nameSpace.RaftGroup.VOLGet(1, in.VolID)
	if err != nil {
		logger.Error("Get volume info for ExpandVolTS failed, err:%v", err)
		return &ack, err
	}
	volume := mp.Volume{}
	err = pbproto.Unmarshal(v, &volume)
	if err != nil {
		return &ack, err
	}

	if in.ExpandQuota%BlkSizeG != 0 {
		blkgrpnum := in.ExpandQuota/BlkSizeG + 1
		in.ExpandQuota = blkgrpnum * BlkSizeG
	}

	volume.TotalSize = volume.TotalSize + in.ExpandQuota

	val, _ := pbproto.Marshal(&volume)
	err = nameSpace.RaftGroup.VOLSet(1, in.VolID, val)
	if err != nil {
		logger.Error("Update volume info for ExpandVolTS failed, err:%v", err)
		return &ack, err
	}
	ack.Ret = 0
	return &ack, nil
}

func (s *MetaNodeServer) ExpandVolRS(ctx context.Context, in *mp.ExpandVolRSReq) (*mp.ExpandVolRSAck, error) {

	s.Lock()
	defer s.Unlock()

	ack := mp.ExpandVolRSAck{}
	ret, cnameSpace := ns.GetNameSpace("Cluster")
	if ret != 0 {
		logger.Error("Get Cluster NameSpace for ExpandVolRS failed, ret:%v", ret)
		ack.Ret = ret
		return &ack, nil
	}

	v, err := cnameSpace.RaftGroup.VOLGet(1, in.VolID)
	if err != nil {
		logger.Error("Get volume info for ExpandVolRS failed, err:%v", err)
		return &ack, err
	}
	volume := mp.Volume{}
	err = pbproto.Unmarshal(v, &volume)
	if err != nil {
		return &ack, err
	}
	needExpandSize := volume.TotalSize - volume.AllocatedSize
	if needExpandSize <= 0 {
		ack.Ret = 0
		return &ack, nil
	}

	var blkgrpnum int32
	if needExpandSize%BlkSizeG == 0 {
		blkgrpnum = needExpandSize / BlkSizeG
	} else {
		blkgrpnum = needExpandSize/BlkSizeG + 1
		needExpandSize = blkgrpnum * BlkSizeG
	}
	if blkgrpnum > 6 {
		blkgrpnum = 6
	}

	vv, err := cnameSpace.GetAllDatanode()
	if err != nil {
		logger.Error("GetAllDatanode Info failed:%v for ExpandVolRS", err)
		return &ack, err
	}

	inuseNodes := make(map[string][]*mp.Datanode)
	allip := make([]string, 0)

	// pass bad status and free not enough datanodes
	for _, v := range vv {
		if v.Status != 0 || v.Free < 30 {
			continue
		}
		k := v.Ip
		inuseNodes[k] = append(inuseNodes[k], v)
	}

	for k, _ := range inuseNodes {
		allip = append(allip, k)
	}

	if len(allip) < 3 {
		logger.Error("Expand Volume:%v but datanode nums:%v less than 3, so forbid ExpandVol", in.VolID, len(allip))
		ack.Ret = -1
		return &ack, nil
	}

	bgps := []*mp.BGP{}

	for i := int32(0); i < blkgrpnum; i++ {
		bgID, err := cnameSpace.AllocateBGID()
		if err != nil {
			logger.Error("AllocateBGID for ExpandVolRS failed, err:%v", err)
			return &ack, err
		}
		bg := &mp.BGP{Blocks: make([]*mp.Block, 0)}
		idxs := generateRandomNumber(0, len(allip), 3)
		if len(idxs) != 3 {
			ack.Ret = -1
			return &ack, nil
		}

		for n := 0; n < 3; n++ {
			block := mp.Block{}
			ipkey := allip[idxs[n]]
			idx := generateRandomNumber(0, len(inuseNodes[ipkey]), 1)
			if len(idx) <= 0 {
				ack.Ret = -1
				return &ack, nil
			}
			blockID, err := cnameSpace.AllocateBlockID()
			if err != nil {
				logger.Error("AllocateBlockID for ExpandVolRS failed, err:%v", err)
				return &ack, err
			}
			block.BlkID = blockID
			block.Ip = inuseNodes[ipkey][idx[0]].Ip
			block.Port = inuseNodes[ipkey][idx[0]].Port
			block.Path = inuseNodes[ipkey][idx[0]].MountPoint
			block.Status = inuseNodes[ipkey][idx[0]].Status
			block.BGID = bgID
			block.VolID = in.VolID
			k := block.Ip + fmt.Sprintf(":%d", block.Port) + fmt.Sprintf("-%d", block.BlkID)
			v, _ := pbproto.Marshal(&block)
			err = cnameSpace.RaftGroup.BlockSet(1, k, v)
			if err != nil {
				logger.Error("Expand Volume:%v allocated blockgroup:%v block:%v failed:%v for ExpandVolRS", in.VolID, bgID, blockID, err)
				return &ack, err
			}
			bg.Blocks = append(bg.Blocks, &block)
			//update this datanode freesize
			inuseNodes[ipkey][idx[0]].Free = inuseNodes[ipkey][idx[0]].Free - 5
			key := ipkey + fmt.Sprintf(":%d", block.Port)
			val, _ := pbproto.Marshal(inuseNodes[ipkey][idx[0]])
			cnameSpace.RaftGroup.DataNodeSet(1, key, val)

		}
		key := in.VolID + fmt.Sprintf("-%d", bgID)
		val, _ := pbproto.Marshal(bg)
		err = cnameSpace.RaftGroup.BGPSet(1, key, val)
		if err != nil {
			logger.Error("Expand Volume:%v Set blockgroup:%v blocks:%v failed:%v", in.VolID, bgID, bg, err)
			return &ack, err
		}
		bgps = append(bgps, bg)
		logger.Debug("Expand Volume:%v Set one blockgroup:%v blocks:%v to Cluster Map success", in.VolID, bgID, bg)
	}

	volume.AllocatedSize = volume.AllocatedSize + blkgrpnum*5

	val, _ := pbproto.Marshal(&volume)
	cnameSpace.RaftGroup.VOLSet(1, in.VolID, val)

	ack.Ret = 1
	ack.BGPS = bgps
	return &ack, nil
}

func (s *MetaNodeServer) DelVolRSForExpand(ctx context.Context, in *mp.DelVolRSForExpandReq) (*mp.DelVolRSForExpandAck, error) {
	ack := mp.DelVolRSForExpandAck{}
	ret, nameSpace := ns.GetNameSpace("Cluster")
	if ret != 0 {
		logger.Error("Get Cluster NameSpace for DelVolRSForExpand failed, ret:%v", ret)
		ack.Ret = ret
		return &ack, nil
	}

	delBGBadNum := 0
	for _, v := range in.BGPS {
		delBlkBadNum := 0
		for _, vv := range v.Blocks {
			blkKey := vv.Ip + fmt.Sprintf(":%d-%d", vv.Port, vv.BlkID)
			err := nameSpace.RaftGroup.BlockDel(1, blkKey)
			if err != nil {
				delBlkBadNum += 1
				continue
			}

			tmpDatanode := &mp.Datanode{}
			datanodeKey := vv.Ip + fmt.Sprintf(":%d", vv.Port)
			v, err := nameSpace.RaftGroup.DatanodeGet(1, datanodeKey)
			if err != nil {
				delBlkBadNum += 1
				continue
			}
			err = pbproto.Unmarshal(v, tmpDatanode)
			if err != nil {
				delBlkBadNum += 1
				continue
			}

			tmpDatanode.Free = tmpDatanode.Free + 5
			val, err := pbproto.Marshal(tmpDatanode)
			if err != nil {
				delBlkBadNum += 1
				continue
			}
			err = nameSpace.RaftGroup.DataNodeSet(1, datanodeKey, val)
			if err != nil {
				delBlkBadNum += 1
				continue
			}
		}
		bgKey := in.UUID + fmt.Sprintf("-%d", v.Blocks[0].BGID)
		err := nameSpace.RaftGroup.BGPDel(1, bgKey)
		if err != nil || delBlkBadNum != 0 {
			delBGBadNum += 1
		}
	}

	if delBGBadNum != 0 {
		ack.Ret = -1
		return &ack, nil
	}
	logger.Debug("Delete Volume:%v BlockGroups:%v Cluster Metadata Success for DelVolRSForExpand", in.UUID, in.BGPS)
	return &ack, nil
}

func (s *MetaNodeServer) DeleteVol(ctx context.Context, in *mp.DeleteVolReq) (*mp.DeleteVolAck, error) {
	ack := mp.DeleteVolAck{}
	ret, nameSpace := ns.GetNameSpace("Cluster")
	if ret != 0 {
		logger.Error("Get Cluster NameSpace for DeleteVol failed, ret:%v", ret)
		ack.Ret = ret
		return &ack, nil
	}

	err := nameSpace.RaftGroup.VOLDel(1, in.UUID)
	if err != nil {
		logger.Error("Delete Volume:%v map from Cluster MetaNodeAddr failed, err:%v", in.UUID, err)
		return &ack, err
	}

	value, err := nameSpace.RaftGroup.BGPGetRange(1, in.UUID)
	if err != nil {
		logger.Error("Get BGPS map from Cluster MetaNodeAddr for DeleteVol:%v failed, err:%v", in.UUID, err)
		return &ack, err
	}

	delBGBadNum := 0

	for _, v := range value {
		bgp := &mp.BGP{Blocks: make([]*mp.Block, 0)}

		err := pbproto.Unmarshal(v.V, bgp)
		if err != nil {
			continue
		}

		delBlkBadNum := 0
		for _, vv := range bgp.Blocks {
			blkKey := vv.Ip + fmt.Sprintf(":%d-%d", vv.Port, vv.BlkID)
			err := nameSpace.RaftGroup.BlockDel(1, blkKey)
			if err != nil {
				logger.Error("Delete Block:%v map from Cluster MetaNodeAddr for DeleteVol failed, err:%v", blkKey, err)
				delBlkBadNum += 1
				continue
			}

			tmpDatanode := &mp.Datanode{}
			datanodeKey := vv.Ip + fmt.Sprintf(":%d", vv.Port)
			v, err := nameSpace.RaftGroup.DatanodeGet(1, datanodeKey)
			if err != nil {
				logger.Error("Get DataNode:%v map from Cluster MetaNodeAddr for Update Free+5:%v failed for DeleteVol, err:%v", datanodeKey, err)
				delBlkBadNum += 1
				continue
			}
			err = pbproto.Unmarshal(v, tmpDatanode)
			if err != nil {
				delBlkBadNum += 1
				continue
			}

			tmpDatanode.Free = tmpDatanode.Free + 5
			val, err := pbproto.Marshal(tmpDatanode)
			if err != nil {
				delBlkBadNum += 1
				continue
			}
			err = nameSpace.RaftGroup.DataNodeSet(1, datanodeKey, val)
			if err != nil {
				logger.Error("Set DataNode:%v map value.Free Add 5G from Cluster MetaNodeAddr for failed for DeleteVol, err:%v", datanodeKey, err)
				delBlkBadNum += 1
				continue
			}
		}

		bgKey := in.UUID + fmt.Sprintf("-%d", bgp.Blocks[0].BGID)
		err = nameSpace.RaftGroup.BGPDel(1, bgKey)
		if err != nil || delBlkBadNum != 0 {
			logger.Error("Delete BG:%v from Cluster MetaNodeAddr failed", bgKey)
			delBGBadNum += 1
		}
	}
	if delBGBadNum != 0 {
		ack.Ret = -1
		return &ack, nil
	}
	logger.Debug("Delete Volume:%v from Cluster MetadataAddr Success", in.UUID)
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
	return &ack, nil
}

func (s *MetaNodeServer) Migrate(ctx context.Context, in *mp.MigrateReq) (*mp.MigrateAck, error) {

	ack := mp.MigrateAck{}

	go func() {
		dnIP := in.DataNodeIP
		dnPort := in.DataNodePort
		ret, nameSpace := ns.GetNameSpace("Cluster")
		if ret != 0 {
			logger.Error("Get Cluster NameSpace for Migrate failed, ret:%v", ret)
			ack.Ret = ret
			return
		}

		minKey := dnIP + fmt.Sprintf(":%d", dnPort)
		result, err := nameSpace.RaftGroup.BlockGetRange(1, minKey)
		if err != nil {
			logger.Error("Get Datanode %v Need Migrate Blocks failed, err:%v", minKey, err)
			return
		}
		totalNum := len(result)

		var successNum int
		var failedNum int

		logger.Debug("Migrating DataNode(%v:%v) Blocks Start ---------->>>>>>>>>>>>>>> Total nums:%v", dnIP, dnPort, totalNum)

		for i, v := range result {
			tBlk := &mp.Block{}
			err := pbproto.Unmarshal(v.V, tBlk)
			if err != nil {
				continue
			}

			ret := s.BeginMigrate(tBlk)
			if ret != 0 {
				failedNum++
				logger.Error("Migrating DataNode(%v:%v) Block:%v failed ----->>>>>  Total num:%v , cur index:%v", dnIP, dnPort, tBlk, totalNum, i)
			} else {
				successNum++
				logger.Debug("Migrating DataNode(%v:%v) Block:%v success ----->>>>>  Total num:%v , cur index:%v", dnIP, dnPort, tBlk, totalNum, i)
			}
		}

		logger.Debug("Migrating DataNode(%v:%v) Blocks Done ----------<<<<<<<<<<<<<<<<< Total num:%v , Success num:%v , Failed num:%v", dnIP, dnPort, totalNum, successNum, failedNum)

	}()

	ack.Ret = 0
	return &ack, nil
}

func (s *MetaNodeServer) BeginMigrate(in *mp.Block) int {

	s.Lock()
	defer s.Unlock()

	ret, nameSpace := ns.GetNameSpace("Cluster")
	if ret != 0 {
		logger.Error("Get Cluster NameSpace for BeginMigrate failed, ret:%v", ret)
		return -1
	}

	bgKey := in.VolID + fmt.Sprintf("-%d", in.BGID)
	v, err := nameSpace.RaftGroup.BGPGet(1, bgKey)
	if err != nil {
		return -1
	}

	tbg := &mp.BGP{}
	err = pbproto.Unmarshal(v, tbg)
	if err != nil {
		return -1
	}

	blks := make([]*mp.Block, 0)
	for _, vv := range tbg.Blocks {
		if vv.BlkID == in.BlkID {
			continue
		}
		blks = append(blks, vv)
	}

	if len(blks) != 2 {
		logger.Error("Need Migrate Block:%v but the Backup BlockNum:%v not equal 2, so stop this Block Migrate", in, len(blks))
		return -1
	}

	datanodes, err := nameSpace.GetAllDatanode()
	if err != nil {
		logger.Error("GetAllDatanode Info failed:%v for Migrate Block", err)
		return -1
	}

	inuseNodes := make(map[string][]*mp.Datanode)
	allip := make([]string, 0)

	// pass bad status and free not enough datanodes
	for _, v := range datanodes {
		if v.Status != 0 || v.Free < 10 || v.Ip == blks[0].Ip {
			continue
		}
		k := v.Ip
		inuseNodes[k] = append(inuseNodes[k], v)
	}

	for k, _ := range inuseNodes {
		allip = append(allip, k)
	}

	if len(allip) < 1 {
		logger.Error("Migrate block to New datanode but datanode nums:%v less than 1, so forbid Migrate", len(allip))
		return -1
	}

	idxs := generateRandomNumber(0, len(allip), 1)
	if len(idxs) != 1 {
		return -1
	}

	ipkey := allip[idxs[0]]
	idx := generateRandomNumber(0, len(inuseNodes[ipkey]), 1)
	if len(idx) <= 0 {
		return -1
	}

	newBlk := mp.Block{}
	blockID, err := nameSpace.AllocateBlockID()
	newBlk.BlkID = blockID
	newBlk.Ip = inuseNodes[ipkey][idx[0]].Ip
	newBlk.Port = inuseNodes[ipkey][idx[0]].Port
	newBlk.Path = inuseNodes[ipkey][idx[0]].MountPoint
	newBlk.Status = inuseNodes[ipkey][idx[0]].Status
	newBlk.BGID = in.BGID
	newBlk.VolID = in.VolID
	k := newBlk.Ip + fmt.Sprintf(":%d", newBlk.Port) + fmt.Sprintf("-%d", newBlk.BlkID)
	val, _ := pbproto.Marshal(&newBlk)
	err = nameSpace.RaftGroup.BlockSet(1, k, val)
	if err != nil {
		return -1
	}

	//update this datanode freesize
	inuseNodes[ipkey][idx[0]].Free = inuseNodes[ipkey][idx[0]].Free - 5
	key := ipkey + fmt.Sprintf(":%d", newBlk.Port)
	val, _ = pbproto.Marshal(inuseNodes[ipkey][idx[0]])
	nameSpace.RaftGroup.DataNodeSet(1, key, val)

	var okflag int
	for _, v := range blks {
		if v.Status != 0 {
			continue
		}
		logger.Debug("Migrate Block:%v copydata from BackBlock:%v to NewBlock:%v", in, v, newBlk)
		ret := beginMigrateBlk(v.BlkID, v.Ip, v.Port, v.Path, newBlk.BlkID, newBlk.Ip, newBlk.Port, newBlk.Path)
		if ret == 0 {
			blks = append(blks, &newBlk)
			pbg := &mp.BGP{}
			pbg.Blocks = blks
			val, _ := pbproto.Marshal(pbg)
			err1 := nameSpace.RaftGroup.BGPSet(1, bgKey, val)
			oldblkKey := in.Ip + fmt.Sprintf(":%d", in.Port) + fmt.Sprintf("-%d", in.BlkID)
			err2 := nameSpace.RaftGroup.BlockDel(1, oldblkKey)
			if err1 == nil && err2 == nil {
				logger.Debug("Migrate OldBlock:%v to NewBlock:%v Copydata from BackBlock:%v Success -- migrateUpdateMeta Success -- migrateUpdateDb Success!", in, newBlk, v)
				okflag = 1
			} else {
				logger.Debug("Migrate OldBlock:%v to NewBlock:%v Copydata from BackBlock:%v Success --  migrateUpdateMeta Success -- migrateUpdateDb Failed!", in, newBlk, v)
			}
			break

		} else {
			logger.Error("Migrate OldBlk:%v to NewBlk:%v Copydata from BackupBlk:%v Failed", in, newBlk, v)
			break
		}
	}
	if okflag == 1 {
		return 0
	} else {
		return -1
	}
}

func beginMigrateBlk(sid uint64, sip string, sport int32, smount string, did uint64, dip string, dport int32, dmount string) int32 {
	sDnAddr := sip + fmt.Sprintf(":%d", sport)
	conn, err := grpc.Dial(sDnAddr, grpc.WithInsecure())

	if err != nil {
		logger.Error("Migrate failed : Dial to DestDataNode:%v failed:%v !", sDnAddr, err)
		return -1
	}
	defer conn.Close()
	dc := dp.NewDataNodeClient(conn)
	tRecvMigrateReq := &dp.RecvMigrateReq{
		SrcBlkID: sid,
		SrcMount: smount,
		DstIP:    dip,
		DstPort:  dport,
		DstBlkID: did,
		DstMount: dmount,
	}

	ctx, _ := context.WithTimeout(context.Background(), 5*time.Minute)
	tRecvMigrateAck, err := dc.RecvMigrateMsg(ctx, tRecvMigrateReq)
	if err != nil {
		logger.Error("Migrate failed : DestDataNode:%v exec RecvMigrate function failed:%v !", sDnAddr, err)
		return -1
	}

	return tRecvMigrateAck.Ret
}

func startMetaDataService(metaServer *MetaNodeServer) {

	lis, err := net.Listen("tcp", metaServer.Addr.Grpc)
	if err != nil {
		panic(fmt.Sprintf("Failed to listen on:%v", metaServer.Addr.Grpc))
	}
	s := grpc.NewServer()
	mp.RegisterMetaNodeServer(s, metaServer)
	// Register reflection service on gRPC server.
	reflection.Register(s)
	if err := s.Serve(lis); err != nil {
		panic("Failed to serve")
	}
}

func loadMetaData(rs *raft.RaftServer) int32 {
	ns.CreateGNameSpace()

	ns.CreateClusterNameSpace(rs, MetaNodeServerAddr.peers, MetaNodeServerAddr.nodeID, MetaNodeServerAddr.waldir)

	ret, vols := ns.GetVolList()
	if ret != 0 {
		logger.Error("loadMetaData,GetVolList failed,ret:%v", ret)
		return ret
	}
	for _, v := range vols {
		logger.Debug("loadMetaData,Vol:%v", v)
		ns.CreateNameSpace(rs, MetaNodeServerAddr.peers, MetaNodeServerAddr.nodeID, MetaNodeServerAddr.waldir, v.UUID, v.RGID, true)
	}
	return 0
}

func init() {

	flag.StringVar(&MetaNodeServerAddr.host, "metanode", "127.0.0.1", "ContainerFS Metanode Host")
	nodeid := flag.Int64("nodeid", 1, "ContainerFS Metanode ID")
	peers := flag.String("nodepeer", "1,2,3", "ContainerFS metanode peers")
	ips := flag.String("nodeips", "127.0.0.1,127.0.0.1,127.0.0.1", "ContainerFS metanode ips")
	flag.StringVar(&MetaNodeServerAddr.waldir, "wal", "/export/containerfs/metanode/data", "ContainerFS Meta waldir")
	flag.StringVar(&MetaNodeServerAddr.log, "logpath", "/export/Logs/containerfs/logs/", "ContainerFS Meta log")
	loglevel := flag.String("loglevel", "error", "ContainerFS metanode log level")

	flag.Parse()

	MetaNodeServerAddr.nodeID = uint64(*nodeid)
	MetaNodeServerAddr.ips = strings.Split(*ips, ",")
	peerarray := strings.Split(*peers, ",")
	var err error
	MetaNodeServerAddr.peers, err = parsePeers(peerarray)
	if err != nil {
		logger.Error("parse peers failed!. peers=%v", peers)
	}

	logger.SetConsole(true)
	logger.SetRollingFile(MetaNodeServerAddr.log, "metanode.log", 10, 100, logger.MB) //each 100M rolling
	switch *loglevel {
	case "error":
		logger.SetLevel(logger.ERROR)
	case "debug":
		logger.SetLevel(logger.DEBUG)
	case "info":
		logger.SetLevel(logger.INFO)
	default:
		logger.SetLevel(logger.ERROR)
	}

}

func parsePeers(peersstr []string) (peers []proto.Peer, err error) {
	for _, s := range peersstr {
		p, err := strconv.Atoi(s)
		if err != nil {
			return nil, err
		}
		peers = append(peers, proto.Peer{ID: uint64(p)})
	}
	return
}

func showLeaders(s *MetaNodeServer) {

	ret, vols := ns.GetVolList()
	if ret != 0 {
		logger.Error("showLeaders,GetVolList failed,ret:%v", ret)
		return
	}
	for _, v := range vols {
		_, nameSpace := ns.GetNameSpace(v.UUID)
		if nameSpace != nil {
			l, t := s.RaftServer.LeaderTerm(nameSpace.RaftGroupID)
			logger.Debug("--------- Volume UUID %v,RaftGroup LeaderID %v Term %v ---------", v.UUID, l, t)
		}
	}
	return

}

func detectDataNodes(metaServer *MetaNodeServer) {
	ret, nameSpace := ns.GetNameSpace("Cluster")
	if ret != 0 {
		logger.Error("Get Cluster MetaNode NameSpace err for detectDataNodes")
		return
	}

	vv, err := nameSpace.GetAllDatanode()
	if err != nil {
		logger.Error("GetAllDatanode Info failed:%v for detectDatanodes", err)
		return
	}

	for _, v := range vv {
		go DetectDatanode(metaServer, v)
	}
}

func DetectDatanode(metaServer *MetaNodeServer, v *mp.Datanode) {
	dnAddr := v.Ip + fmt.Sprintf(":%d", v.Port)
	conn, err := grpc.Dial(dnAddr, grpc.WithInsecure())
	if err != nil {
		if v.Status == 0 {
			logger.Error("Detect DataNode:%v failed : Dial to datanode failed !", dnAddr)
			v.Status = 1
			SetDatanodeMap(v)
			logger.Debug("Detect Datanode(%v) status from good to bad, set datanode map success", dnAddr)
			//UpdateBlock(metaServer, v.Ip, v.Port, 1)
		}
		return
	}
	defer conn.Close()
	c := dp.NewDataNodeClient(conn)
	var DatanodeHealthCheckReq dp.DatanodeHealthCheckReq
	pDatanodeHealthCheckAck, err := c.DatanodeHealthCheck(context.Background(), &DatanodeHealthCheckReq)
	if err != nil {
		if v.Status == 0 {
			v.Status = 1
			SetDatanodeMap(v)
			logger.Debug("Detect Datanode(%v) status from good to bad, set datanode map success", dnAddr)
			//UpdateBlock(metaServer, v.Ip, v.Port, 1)
		}
		return
	}

	if pDatanodeHealthCheckAck.Status != 0 {
		if v.Status == 0 {
			v.Status = pDatanodeHealthCheckAck.Status
			v.Used = pDatanodeHealthCheckAck.Used
			SetDatanodeMap(v)
			logger.Debug("Detect Datanode(%v) status from good to bad, set datanode map success", dnAddr)
			//UpdateBlock(metaServer, v.Ip, v.Port, 1)
		}
		return
	}
	if v.Status != 0 {
		v.Status = 0
		logger.Debug("Detect Datanode(%v) status from bad to good, set datanode map success", dnAddr)
		//UpdateBlock(metaServer, v.Ip, v.Port, 0)
	}
	v.Used = pDatanodeHealthCheckAck.Used
	SetDatanodeMap(v)
	return
}

func SetDatanodeMap(v *mp.Datanode) int {
	ret, nameSpace := ns.GetNameSpace("Cluster")
	if ret != 0 {
		logger.Error("Get Cluster MetaNode NameSpace err for SetDatanodeMap")
		return -1
	}
	key := v.Ip + fmt.Sprintf(":%d", v.Port)
	val, _ := pbproto.Marshal(v)
	err := nameSpace.RaftGroup.DataNodeSet(1, key, val)
	if err != nil {
		logger.Error("Datanode set value:%v err:%v", val, err)
		return -1
	}
	return 0
}

/*
func UpdateBlock(metaServer *MetaNodeServer, ip string, port int32, status int32) int {
	ret, nameSpace := ns.GetNameSpace("Cluster")
	if ret != 0 {
		logger.Error("Get Cluster MetaNode NameSpace err for UpdateBlock")
		return -1
	}

	minKey := ip + fmt.Sprintf(":%d", port)
	value, err := nameSpace.RaftGroup.BlockGetRange(1, minKey)
	if err != nil {
		logger.Error("BlockGetRange for UpdateBlock err:%v", err)
		return -1
	}

	for _, v := range value {
		block := &mp.Block{}
		err := pbproto.Unmarshal(v.V, block)
		if err != nil {
			continue
		}
		block.Status = status
		if ok := UpdateBlockDB(metaServer, block); ok != 0 {
			continue
		}
	}
	return 0
}

func UpdateBlockDB(metaServer *MetaNodeServer, block *mp.Block) int {
	conn, err := DialMeta(metaServer, block.VolID)
	if err != nil {
		logger.Error("updateBlockToMeta: Dail Meta Failed err:%v", err)
		return -1
	}
	defer conn.Close()
	mc := mp.NewMetaNodeClient(conn)

	ack, err := mc.UpdateBlkDB(context.Background(), block)
	if err != nil || ack.Ret != 0 {
		logger.Error("UpdateBlkDB  failed!")
		return -1
	}
	return 0
}

func (s *MetaNodeServer) UpdateBlkDB(ctx context.Context, in *mp.Block) (*mp.UpdateBlkDBAck, error) {
	ack := mp.UpdateBlkDBAck{}
	ret, nameSpace := ns.GetNameSpace(in.VolID)
	if ret != 0 {
		ack.Ret = -1
		return &ack, nil
	}

	nameSpace.Lock()
	ok, blockGroup := nameSpace.BlockGroupDBGet(uint32(in.BGID))
	if !ok {
		ack.Ret = -1
		return &ack, nil
	}
	for i, vv := range blockGroup.BlockInfos {
		if uint64(vv.BlockID) == in.BlkID {
			blockGroup.BlockInfos[i].Status = in.Status
			break
		}
	}
	nameSpace.BlockGroupDBSet(uint32(in.BGID), blockGroup)

	nameSpace.Unlock()
	ack.Ret = 0
	return &ack, nil
}
func GetLeader(metaServer *MetaNodeServer, volumeID string) string {
	var leader string
	ret, nameSpace := ns.GetNameSpace(volumeID)
	if ret != 0 {
		return ""
	}
	leaderID, _ := metaServer.RaftServer.LeaderTerm(nameSpace.RaftGroupID)
	if leaderID <= 0 {
		return ""
	}
	leader = raftopt.AddrDatabase[leaderID].Grpc

	return leader
}

// DialMeta ...
func DialMeta(metaServer *MetaNodeServer, volumeID string) (*grpc.ClientConn, error) {
	var conn *grpc.ClientConn
	var err error
	var leader string

	leader = GetLeader(metaServer, volumeID)
	conn, err = grpc.Dial(leader, grpc.WithInsecure(), grpc.FailOnNonTempDialError(true))
	if err != nil {
		time.Sleep(500 * time.Millisecond)
		leader = GetLeader(metaServer, volumeID)
		conn, err = grpc.Dial(leader, grpc.WithInsecure(), grpc.FailOnNonTempDialError(true))
		if err != nil {
			time.Sleep(500 * time.Millisecond)
			leader = GetLeader(metaServer, volumeID)
			conn, err = grpc.Dial(leader, grpc.WithInsecure(), grpc.FailOnNonTempDialError(true))
		}
	}
	return conn, err
}
*/

func main() {

	//for multi-cpu scheduling
	numCPU := runtime.NumCPU()
	runtime.GOMAXPROCS(numCPU)

	raftopt.AddInit(MetaNodeServerAddr.ips)

	fmt.Println("MetaNodeServerAddr:")
	fmt.Println(MetaNodeServerAddr)

	var metaServer MetaNodeServer

	// resolver
	r := raftopt.NewResolver()
	metaServer.Resolver = r

	// address
	addrInfo, ok := raftopt.AddrDatabase[MetaNodeServerAddr.nodeID]
	if !ok {
		logger.Error("no such address info. nodeId: %d", MetaNodeServerAddr.nodeID)
	}
	metaServer.Addr = addrInfo

	//  new raft server
	err := raftopt.StartRaftServer(&metaServer.RaftServer, metaServer.Resolver, addrInfo, MetaNodeServerAddr.nodeID)
	if err != nil {
		logger.Error("StartRaftServer failed ...")
		os.Exit(1)
	}
	logger.Debug("StartRaftServer success ...")

	// parse peers
	for _, p := range MetaNodeServerAddr.peers {
		r.AddNode(p.ID)
	}
	logger.Debug("AddNode success ...")

	ret := loadMetaData(metaServer.RaftServer)
	if ret != 0 {
		if ret == 1 {
			logger.Debug("loadMetaData  no volumes")
		} else {
			logger.Error("loadMetaData failed ...")
			os.Exit(1)
		}
	}

	go func() {
		http.ListenAndServe("127.0.0.1:10000", nil)
	}()

	ticker := time.NewTicker(time.Second * 10)
	go func() {
		for range ticker.C {
			//showLeaders(&metaServer)
		}
	}()

	t := time.NewTicker(time.Second * 30)
	go func() {
		for range t.C {
			if metaServer.RaftServer.IsLeader(1) {
				detectDataNodes(&metaServer)
			}
		}
	}()

	startMetaDataService(&metaServer)
}
