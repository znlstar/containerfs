package main

import (
	"fmt"
	"github.com/ipdcode/containerfs/logger"
	ns "github.com/ipdcode/containerfs/metanode/namespace"
	"github.com/ipdcode/containerfs/metanode/raftopt"
	mp "github.com/ipdcode/containerfs/proto/mp"
	"github.com/lxmgo/config"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
	"jd.com/sharkstore/raft"
	"jd.com/sharkstore/raft/proto"
	"net"
	"net/http"
	_ "net/http/pprof"
	"os"
	"runtime"
	"runtime/debug"
	"strconv"
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

// MetaNodeServerAddr ...
var MetaNodeServerAddr addr

// MetaNodeServer ...
type MetaNodeServer struct {
	NodeID     uint64
	Addr       *raftopt.Address
	Resolver   *raftopt.Resolver
	RaftServer *raft.RaftServer
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
	// send to follower metadatas to create
	if in.Type == 0 {
		for _, addr := range raftopt.AddrDatabase {
			if addr.Grpc == s.Addr.Grpc {
				continue
			}
			conn2, err2 := grpc.Dial(addr.Grpc, grpc.WithInsecure())
			if err2 != nil {
				logger.Error("told peers to  create NameSpace Failed ...")
				continue
			}
			defer conn2.Close()
			mc := mp.NewMetaNodeClient(conn2)
			pmCreateNameSpaceReq := &mp.CreateNameSpaceReq{
				VolID:       in.VolID,
				RaftGroupID: in.RaftGroupID,
				Type:        1,
			}
			pmCreateNameSpaceAck, ret := mc.CreateNameSpace(context.Background(), pmCreateNameSpaceReq)
			if ret != nil {
				logger.Error("told peers to  create NameSpace Failed ...")
				continue
			}
			if pmCreateNameSpaceAck.Ret != 0 {
				logger.Error("told peers to create NameSpace Failed ...")
				continue
			}
		}
	}
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
		for _, addr := range MetaNodeServerAddr.ips {
			if addr == MetaNodeServerAddr.host {
				continue
			}
			conn2, err2 := grpc.Dial(addr, grpc.WithInsecure())
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

//CreateDir ...
func (s *MetaNodeServer) CreateDir(ctx context.Context, in *mp.CreateDirReq) (*mp.CreateDirAck, error) {
	ack := mp.CreateDirAck{}
	fullPathName := in.FullPathName
	volID := in.VolID
	ret, nameSpace := ns.GetNameSpace(volID)
	if ret != 0 {
		ack.Ret = ret
		return &ack, nil
	}
	ack.Ret = nameSpace.CreateDir(fullPathName)
	return &ack, nil
}

//Stat ...
func (s *MetaNodeServer) Stat(ctx context.Context, in *mp.StatReq) (*mp.StatAck, error) {
	ack := mp.StatAck{}
	fullPathName := in.FullPathName
	volID := in.VolID
	ret, nameSpace := ns.GetNameSpace(volID)
	if ret != 0 {
		ack.Ret = ret
		return &ack, nil
	}
	ack.InodeInfo, ack.Ret = nameSpace.Stat(fullPathName)
	return &ack, nil
}

//List ...
func (s *MetaNodeServer) List(ctx context.Context, in *mp.ListReq) (*mp.ListAck, error) {
	ack := mp.ListAck{}
	fullPathName := in.FullPathName
	volID := in.VolID
	ret, nameSpace := ns.GetNameSpace(volID)
	if ret != 0 {
		ack.Ret = ret
		return &ack, nil
	}
	ack.InodeInfos, ack.Ret = nameSpace.List(fullPathName)
	return &ack, nil
}

// DeleteDir ...
func (s *MetaNodeServer) DeleteDir(ctx context.Context, in *mp.DeleteDirReq) (*mp.DeleteDirAck, error) {

	ack := mp.DeleteDirAck{}
	fullPathName := in.FullPathName
	volID := in.VolID
	ret, nameSpace := ns.GetNameSpace(volID)
	if ret != 0 {
		ack.Ret = ret
		return &ack, nil
	}
	ack.Ret = nameSpace.DeleteDir(fullPathName)
	return &ack, nil

}

// Rename ...
func (s *MetaNodeServer) Rename(ctx context.Context, in *mp.RenameReq) (*mp.RenameAck, error) {
	ack := mp.RenameAck{}
	fullPathName1 := in.FullPathName1
	fullPathName2 := in.FullPathName2
	volID := in.VolID
	ret, nameSpace := ns.GetNameSpace(volID)
	if ret != 0 {
		ack.Ret = ret
		return &ack, nil
	}
	ack.Ret = nameSpace.Rename(fullPathName1, fullPathName2)
	return &ack, nil

}

//CreateFile ...
func (s *MetaNodeServer) CreateFile(ctx context.Context, in *mp.CreateFileReq) (*mp.CreateFileAck, error) {
	ack := mp.CreateFileAck{}
	fullPathName := in.FullPathName
	volID := in.VolID
	ret, nameSpace := ns.GetNameSpace(volID)
	if ret != 0 {
		ack.Ret = ret
		return &ack, nil
	}
	ack.Ret = nameSpace.CreateFile(fullPathName)
	return &ack, nil
}

// DeleteFile ...
func (s *MetaNodeServer) DeleteFile(ctx context.Context, in *mp.DeleteFileReq) (*mp.DeleteFileAck, error) {

	ack := mp.DeleteFileAck{}
	fullPathName := in.FullPathName
	volID := in.VolID
	ret, nameSpace := ns.GetNameSpace(volID)
	if ret != 0 {
		ack.Ret = ret
		return &ack, nil
	}
	ack.Ret = nameSpace.DeleteFile(fullPathName)
	return &ack, nil

}

// AllocateChunk ...
func (s *MetaNodeServer) AllocateChunk(ctx context.Context, in *mp.AllocateChunkReq) (*mp.AllocateChunkAck, error) {
	ack := mp.AllocateChunkAck{}
	fileName := in.FileName
	volID := in.VolID

	ack.SequenceID = in.SequenceID

	ret, nameSpace := ns.GetNameSpace(volID)
	if ret != 0 {
		ack.Ret = ret
		return &ack, nil
	}
	ret, chunkInfo := nameSpace.AllocateChunk(fileName)
	if ret != 0 {
		ack.Ret = ret
		return &ack, nil
	}

	ok1, blockGroup := nameSpace.BlockGroupDBGet(chunkInfo.BlockGroupID)
	if !ok1 {
		ack.Ret = 1
		return &ack, nil
	}

	var tmpChunkInfo mp.ChunkInfoWithBG
	tmpChunkInfo.ChunkID = chunkInfo.ChunkID
	tmpChunkInfo.ChunkSize = chunkInfo.ChunkSize
	tmpChunkInfo.BlockGroup = nameSpace.BlockGroupVp2Mp(blockGroup)

	ack.ChunkInfo = &tmpChunkInfo
	return &ack, nil
}

// GetFileChunks ...
func (s *MetaNodeServer) GetFileChunks(ctx context.Context, in *mp.GetFileChunksReq) (*mp.GetFileChunksAck, error) {
	ack := mp.GetFileChunksAck{}
	fileName := in.FileName
	volID := in.VolID
	ret, nameSpace := ns.GetNameSpace(volID)
	if ret != 0 {
		ack.Ret = ret
		return &ack, nil
	}
	ok, chunkInfos := nameSpace.GetFileChunks(fileName)
	if ok != 0 {
		ack.Ret = ok
		return &ack, nil
	}

	for _, v := range chunkInfos {
		var chunkInfoWithBG mp.ChunkInfoWithBG
		chunkInfoWithBG.ChunkID = v.ChunkID
		chunkInfoWithBG.ChunkSize = v.ChunkSize
		chunkInfoWithBG.Status = v.Status

		ok1, blockGroup := nameSpace.BlockGroupDBGet(v.BlockGroupID)
		if !ok1 {
			continue
		}
		chunkInfoWithBG.BlockGroup = nameSpace.BlockGroupVp2Mp(blockGroup)

		ack.ChunkInfos = append(ack.ChunkInfos, &chunkInfoWithBG)

	}
	ack.Ret = 0

	return &ack, nil
}

// SyncChunk ...
func (s *MetaNodeServer) SyncChunk(ctx context.Context, in *mp.SyncChunkReq) (*mp.SyncChunkAck, error) {
	ack := mp.SyncChunkAck{}
	fileName := in.FileName
	volID := in.VolID
	chunkinfo := in.ChunkInfo
	ret, nameSpace := ns.GetNameSpace(volID)
	if ret != 0 {
		ack.Ret = ret
		return &ack, nil
	}
	ack.Ret = nameSpace.SyncChunk(fileName, chunkinfo)
	return &ack, nil
}

// UpdateChunkInfo ...
func (s *MetaNodeServer) UpdateChunkInfo(ctx context.Context, in *mp.UpdateChunkInfoReq) (*mp.UpdateChunkInfoAck, error) {
	ack := mp.UpdateChunkInfoAck{}
	ret, nameSpace := ns.GetNameSpace(in.VolID)
	if ret != 0 {
		ack.Ret = ret
		return &ack, nil
	}
	ack.Ret = nameSpace.UpdateChunkInfo(in)
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

func loadMetaData(rs *raft.RaftServer) int32 {
	ns.CreateGNameSpace()
	ret, vols := ns.GetVolList()
	if ret != 0 {
		logger.Error("loadMetaData,GetVolList failed,ret:%v", ret)
		return ret
	}
	for _, v := range vols {
		logger.Debug("loadMetaData,Vol:%v", v)
		ns.CreateNameSpace(rs, MetaNodeServerAddr.peers, MetaNodeServerAddr.nodeID, MetaNodeServerAddr.waldir, v.UUID, v.RaftGroupID, true)
	}
	return 0
}

func init() {

	c, err := config.NewConfig(os.Args[1])
	if err != nil {
		fmt.Println("NewConfig err")
		os.Exit(1)
	}

	ns.VolMgrAddress = c.String("volmgr::host")
	MetaNodeServerAddr.host = c.String("metanode::host")
	tmpNodeID, err := c.Int("metanode::nodeid")
	MetaNodeServerAddr.nodeID = uint64(tmpNodeID)
	MetaNodeServerAddr.peers, err = parsePeers(c.Strings("metanode::peers"))
	if err != nil {
		logger.Error("parse peers failed!. peers=%v", c.String("metanode::peers"))
	}

	MetaNodeServerAddr.ips = c.Strings("metanode::ips")
	MetaNodeServerAddr.waldir = c.String("metanode::waldir")
	MetaNodeServerAddr.log = c.String("metanode::log")

	logger.SetConsole(true)
	logger.SetRollingFile(MetaNodeServerAddr.log, "metanode.log", 10, 100, logger.MB) //each 100M rolling
	switch level := c.String("metanode::loglevel"); level {
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

func main() {

	//for multi-cpu scheduling
	numCPU := runtime.NumCPU()
	runtime.GOMAXPROCS(numCPU)

	defer func() {
		if err := recover(); err != nil {
			logger.Error("panic !!! :%v", err)
			logger.Error("stacks:%v", string(debug.Stack()))
		}
	}()

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
		for _ = range ticker.C {
			showLeaders(&metaServer)
		}
	}()

	startMetaDataService(&metaServer)
}
