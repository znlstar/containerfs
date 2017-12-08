package main

import (
	"flag"
	"fmt"
	pbproto "github.com/golang/protobuf/proto"
	"github.com/tigcode/containerfs/logger"
	ns "github.com/tigcode/containerfs/metanode/namespace"
	"github.com/tigcode/containerfs/metanode/raftopt"
	mp "github.com/tigcode/containerfs/proto/mp"
	"github.com/tigcode/raft"
	"github.com/tigcode/raft/proto"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
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

// SnapShotNameSpace ...
func (s *MetaNodeServer) SnapShotNameSpace(ctx context.Context, in *mp.SnapShootNameSpaceReq) (*mp.SnapShootNameSpaceAck, error) {
	ack := mp.SnapShootNameSpaceAck{}
	ack.Ret = ns.SnapShotNameSpace(s.RaftServer, in.VolID, MetaNodeServerAddr.waldir)
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
			pmSnapShootNameSpaceAck, ret := mc.SnapShotNameSpace(context.Background(), pmSnapShootNameSpaceReq)
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
		logger.Error("GetFileChunksDirect GetNameSpace err ... ")
		return &ack, nil
	}
	ok, chunkInfos, inode := nameSpace.GetFileChunksDirect(in.PInode, in.Name)
	if ok != 0 {
		ack.Ret = ok
		logger.Error("GetFileChunksDirect nameSpace.GetFileChunksDirect err %v", ok)
		return &ack, nil
	}

	ret, nameSpace = ns.GetNameSpace("Cluster")
	if ret != 0 {
		ack.Ret = ret
		logger.Error("GetFileChunksDirect GetNameSpace(Cluster) err ret %v", ret)
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
			logger.Error("GetFileChunksDirect nameSpace.RaftGroup.BGPGet(1, bgKey) err %v", err)
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
		http.ListenAndServe(":10000", nil)
	}()

	ticker := time.NewTicker(time.Second * 10)
	go func() {
		for range ticker.C {
			showLeaders(&metaServer)
		}
	}()

	t := time.NewTicker(time.Second * 30)
	go func() {
		for range t.C {
			if metaServer.RaftServer.IsLeader(1) {
				DetectDataNodes(&metaServer)
			}
		}
	}()

	startMetaDataService(&metaServer)

}
