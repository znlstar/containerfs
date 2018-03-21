package main

import (
	"flag"
	"fmt"
	//pbproto "github.com/golang/protobuf/proto"
	"github.com/tiglabs/containerfs/logger"
	ns "github.com/tiglabs/containerfs/metanode/namespace"
	"github.com/tiglabs/containerfs/proto/mp"
	"github.com/tiglabs/containerfs/proto/vp"
	"github.com/tiglabs/containerfs/raftopt"
	com "github.com/tiglabs/containerfs/raftopt/common"
	"github.com/tiglabs/containerfs/utils"
	"github.com/tiglabs/raft"
	"github.com/tiglabs/raft/proto"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
	"net"
	"net/http"
	_ "net/http/pprof"
	"os"
	"runtime"
	"strings"
	"sync"
	"time"
)

type addr struct {
	host        string
	nodeID      uint64
	waldir      string
	log         string
	volmgrHosts []string
}

// MetaNodeServerAddr ...
var MetaNodeServerAddr addr

// MetaNodeServer ...
type MetaNodeServer struct {
	NodeID     uint64
	Addr       *com.Address
	Resolver   com.Resolver
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
	for id, addr := range raftopt.VolumeAddrDatabase {
		logger.Debug("id:%v addr:%v", id, *addr)
	}
	leaderID, _ := s.RaftServer.LeaderTerm(nameSpace.RaftGroupID)
	if leaderID <= 0 {
		ack.Ret = 1
		return &ack, nil
	}
	ack.Ret = 0
	ack.Leader = raftopt.VolumeAddrDatabase[leaderID].Grpc
	return &ack, nil
}

//CreateNameSpace ...
func (s *MetaNodeServer) CreateNameSpace(ctx context.Context, in *mp.CreateNameSpaceReq) (*mp.CreateNameSpaceAck, error) {
	ack := mp.CreateNameSpaceAck{}

	for _, v := range in.Volume.VolumePeers {
		addr := &com.Address{
			Grpc:      v.Host + ":9901",
			Heartbeat: v.Host + ":9902",
			Replicate: v.Host + ":9903",
			Pprof:     v.Host + ":9904",
		}
		s.Resolver.AddNode(v.NodeID, addr)
	}

	var peers []proto.Peer
	for _, v := range in.Volume.VolumePeers {
		peers = append(peers, proto.Peer{ID: v.NodeID})
	}

	ack.Ret = ns.CreateNameSpace(s.RaftServer, peers, MetaNodeServerAddr.nodeID, MetaNodeServerAddr.waldir, in.VolID, in.Volume.RaftGroupID, in.Volume.BlockGroups, false)
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
func (s *MetaNodeServer) SnapShotNameSpace(ctx context.Context, in *mp.SnapShotNameSpaceReq) (*mp.SnapShotNameSpaceAck, error) {
	go ns.SnapShotNameSpace(s.RaftServer, in.VolID, MetaNodeServerAddr.waldir)
	return &mp.SnapShotNameSpaceAck{Ret: 0}, nil
}

// DeleteNameSpace ...
func (s *MetaNodeServer) DeleteNameSpace(ctx context.Context, in *mp.DeleteNameSpaceReq) (*mp.DeleteNameSpaceAck, error) {
	ack := mp.DeleteNameSpaceAck{}
	ack.Ret = ns.DeleteNameSpace(s.RaftServer, in.VolID)
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
	if in.PInode > 0 {
		ack.Ret, _ = nameSpace.DentryDBGet(in.GInode, in.Name)
		if ack.Ret > 0 {
			return &ack, nil
		}
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

// DeleteFileDirect ...
func (s *MetaNodeServer) DeleteSymLinkDirect(ctx context.Context, in *mp.DeleteSymLinkDirectReq) (*mp.DeleteSymLinkDirectAck, error) {

	ack := mp.DeleteSymLinkDirectAck{}

	ret, nameSpace := ns.GetNameSpace(in.VolID)
	if ret != 0 {
		ack.Ret = ret
		return &ack, nil
	}
	ack.Ret = nameSpace.DeleteSymLinkDirect(in.PInode, in.Name)
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
	gRet, chunkInfos, inode := nameSpace.GetFileChunksDirect(in.PInode, in.Name)
	if gRet != 0 {
		ack.Ret = gRet
		return &ack, nil
	}

	vc := vp.NewVolMgrClient(ns.VolMgrConn)
	pGetBlockGroupByIDReq := &vp.GetBlockGroupByIDReq{}

	for _, v := range chunkInfos {
		var chunkInfoWithBG mp.ChunkInfoWithBG
		chunkInfoWithBG.ChunkID = v.ChunkID
		chunkInfoWithBG.ChunkSize = v.ChunkSize

		blockGroup := &mp.BlockGroupWithHost{}

		pGetBlockGroupByIDReq.BlockGroupID = v.BlockGroupID
		ctx, _ := context.WithTimeout(context.Background(), 3*time.Second)
		pGetBlockGroupByIDAck, err := vc.GetBlockGroupByID(ctx, pGetBlockGroupByIDReq)
		if err != nil || pGetBlockGroupByIDAck.Ret != 0 {
			ack.Ret = -1
			return &ack, nil
		} else {
			blockGroup.BlockGroupID = v.BlockGroupID
			blockGroup.Hosts = pGetBlockGroupByIDAck.BlockGroup.Hosts
		}

		chunkInfoWithBG.BlockGroupWithHost = blockGroup
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
		logger.Error("AllocateChunk Failed ret %v", ret)
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

// AsyncChunk ...
func (s *MetaNodeServer) AsyncChunk(ctx context.Context, in *mp.AsyncChunkReq) (*mp.AsyncChunkAck, error) {
	ack := mp.AsyncChunkAck{}
	ret, nameSpace := ns.GetNameSpace(in.VolID)
	if ret != 0 {
		ack.Ret = ret
		return &ack, nil
	}
	ack.Ret = nameSpace.AsyncChunk(in.ParentInodeID, in.Name, in.ChunkID, in.CommitSize, in.BlockGroupID)
	return &ack, nil
}

// SymLink ...
func (s *MetaNodeServer) SymLink(ctx context.Context, in *mp.SymLinkReq) (*mp.SymLinkAck, error) {
	ack := mp.SymLinkAck{}
	ret, nameSpace := ns.GetNameSpace(in.VolID)
	if ret != 0 {
		ack.Ret = ret
		return &ack, nil
	}
	ack.Ret, ack.Inode = nameSpace.SymLink(in.PInode, in.Name, in.Target)
	return &ack, nil
}

// ReadLink ...
func (s *MetaNodeServer) ReadLink(ctx context.Context, in *mp.ReadLinkReq) (*mp.ReadLinkAck, error) {
	ack := mp.ReadLinkAck{}
	ret, nameSpace := ns.GetNameSpace(in.VolID)
	if ret != 0 {
		ack.Ret = ret
		return &ack, nil
	}
	ack.Ret, ack.Target = nameSpace.ReadLink(in.Inode)
	return &ack, nil
}

//GetSymLinkInfoDirect ...
func (s *MetaNodeServer) GetSymLinkInfoDirect(ctx context.Context, in *mp.GetSymLinkInfoDirectReq) (*mp.GetSymLinkInfoDirectAck, error) {
	ack := mp.GetSymLinkInfoDirectAck{}
	ret, nameSpace := ns.GetNameSpace(in.VolID)
	if ret != 0 {
		ack.Ret = ret
		return &ack, nil
	}
	ack.Ret, ack.Inode = nameSpace.GetSymLinkInfoDirect(in.PInode, in.Name)
	return &ack, nil
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

func (ms *MetaNodeServer) loadMetaData() int32 {

	vc := vp.NewVolMgrClient(ns.VolMgrConn)
	pGetMetaNodeRGPeersReq := &vp.GetMetaNodeRGPeersReq{
		MetaNodeID: MetaNodeServerAddr.nodeID,
	}
	ctx, _ := context.WithTimeout(context.Background(), 3*time.Second)
	pGetMetaNodeRGPeersAck, err := vc.GetMetaNodeRGPeers(ctx, pGetMetaNodeRGPeersReq)
	if err != nil || pGetMetaNodeRGPeersAck.Ret != 0 {
		logger.Error("loadMetaData GetMetaNodeRGPeers failed ...")
		return -1
	}

	for _, v := range pGetMetaNodeRGPeersAck.RaftGroups {
		for _, vv := range v.MetaNodes {
			addr := &com.Address{
				Grpc:      vv.Host + ":9901",
				Heartbeat: vv.Host + ":9902",
				Replicate: vv.Host + ":9903",
				Pprof:     vv.Host + ":9904",
			}
			ms.Resolver.AddNode(vv.Id, addr)
		}
	}

	for _, v := range pGetMetaNodeRGPeersAck.RaftGroups {
		logger.Debug("loadMetaData,Vol:%v", v)

		var peers []proto.Peer
		for _, vv := range v.MetaNodes {
			peers = append(peers, proto.Peer{ID: vv.Id})
		}

		ns.CreateNameSpace(ms.RaftServer, peers, MetaNodeServerAddr.nodeID, MetaNodeServerAddr.waldir, v.UUID, v.RGID, nil, true)
	}
	return 0
}

func (ms *MetaNodeServer) MetaNodeHealthCheck(ctx context.Context, in *mp.MetaNodeHealthCheckReq) (*mp.MetaNodeHealthCheckAck, error) {
	ack := mp.MetaNodeHealthCheckAck{}
	return &ack, nil
}

func (ms *MetaNodeServer) GetBlockGroupInfo(ctx context.Context, in *mp.GetBlockGroupInfoReq) (*mp.GetBlockGroupInfoAck, error) {

	ack := mp.GetBlockGroupInfoAck{}
	ret, nameSpace := ns.GetNameSpace(in.VolID)
	if ret != 0 {
		ack.Ret = ret
		return &ack, nil
	}
	ok, blockGroup := nameSpace.BlockGroupDBGet(in.BGID)
	if ok {
		ack.BlockGroup = blockGroup
		return &ack, nil
	}
	ack.Ret = -2
	return &ack, nil
}

func init() {

	var volmgrHostString string
	var nodeid uint64
	var loglevel string

	flag.StringVar(&MetaNodeServerAddr.host, "host", "127.0.0.1", "ContainerFS Metanode Host")
	flag.StringVar(&volmgrHostString, "volmgr", "10.8.64.216,10.8.64.217,10.8.64.218", "ContainerFS VolMgr Host")
	flag.Uint64Var(&nodeid, "nodeid", 1, "ContainerFS Metanode ID")
	flag.StringVar(&MetaNodeServerAddr.waldir, "wal", "/export/containerfs/metanode/data", "ContainerFS Meta waldir")
	flag.StringVar(&MetaNodeServerAddr.log, "logpath", "/export/Logs/containerfs/logs/", "ContainerFS Meta log")
	flag.StringVar(&loglevel, "loglevel", "error", "ContainerFS metanode log level")

	flag.Parse()
	if len(os.Args) >= 2 && (os.Args[1] == "version") {
		fmt.Println(utils.Version())
		os.Exit(0)
	}

	tmp := strings.Split(volmgrHostString, ",")

	MetaNodeServerAddr.volmgrHosts = make([]string, 3)
	MetaNodeServerAddr.volmgrHosts[0] = tmp[0] + ":7703"
	MetaNodeServerAddr.volmgrHosts[1] = tmp[1] + ":7713"
	MetaNodeServerAddr.volmgrHosts[2] = tmp[2] + ":7723"

	MetaNodeServerAddr.nodeID = nodeid
	ns.VolMgrHosts = MetaNodeServerAddr.volmgrHosts

	logger.SetConsole(true)
	logger.SetRollingFile(MetaNodeServerAddr.log, "metanode.log", 10, 100, logger.MB) //each 100M rolling
	switch loglevel {
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

/*
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
*/

func main() {

	//for multi-cpu scheduling
	runtime.GOMAXPROCS(runtime.NumCPU())

	var metaServer MetaNodeServer
	// resolver
	r := raftopt.NewVolumeResolver()
	metaServer.Resolver = r

	//  new raft server
	addr := &com.Address{
		Grpc:      MetaNodeServerAddr.host + ":9901",
		Heartbeat: MetaNodeServerAddr.host + ":9902",
		Replicate: MetaNodeServerAddr.host + ":9903",
		Pprof:     MetaNodeServerAddr.host + ":9904",
	}
	metaServer.Addr = addr
	err := com.StartRaftServer(&metaServer.RaftServer, metaServer.Resolver, addr, MetaNodeServerAddr.nodeID)
	if err != nil {
		logger.Error("StartRaftServer failed ...")
		os.Exit(1)
	}
	logger.Debug("StartRaftServer success ...")

	if ret := registryToVolMgr(metaServer); ret != 0 {
		os.Exit(1)
	}

	logger.Debug("AddNode success ...")

	ret := metaServer.loadMetaData()
	if ret != 0 {
		if ret == 1 {
			logger.Debug("loadMetaData  no volumes")
		} else {
			logger.Error("loadMetaData failed ...")
			os.Exit(1)
		}
	}

	go func() {
		http.ListenAndServe(addr.Pprof, nil)
	}()

	// ticker := time.NewTicker(time.Second * 10)
	// go func() {
	// 	for range ticker.C {
	// 		showLeaders(&metaServer)
	// 	}
	// }()

	startMetaDataService(&metaServer)

}

func registryToVolMgr(metaServer MetaNodeServer) int {

	_, conn, err := utils.DialVolMgr(MetaNodeServerAddr.volmgrHosts)
	if err != nil {
		logger.Error("registryToVolMgr DialVolMgr failed ...")
		return -1
	}

	vc := vp.NewVolMgrClient(conn)

	pMetaNode := &vp.MetaNode{
		Id:   MetaNodeServerAddr.nodeID,
		Host: MetaNodeServerAddr.host,
		Mem:  utils.MemStat().Free,
	}
	ctx, _ := context.WithTimeout(context.Background(), time.Second)
	pMetaNodeAck, err := vc.MetaNodeRegistry(ctx, pMetaNode)
	if err != nil || pMetaNodeAck.Ret != 0 {
		logger.Error("registryToVolMgr MetaNodeRegistry failed ...")
		return -1
	}

	ns.VolMgrLeader, err = utils.GetVolMgrLeader(MetaNodeServerAddr.volmgrHosts)
	if err != nil {
		return -1
	}

	_, ns.VolMgrConn, err = utils.DialVolMgr(MetaNodeServerAddr.volmgrHosts)
	if ns.VolMgrConn == nil || err != nil {
		return -1
	}

	ns.VolMgrInit = true

	return 0
}
