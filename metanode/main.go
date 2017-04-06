package main

import (
	"fmt"
	"github.com/ipdcode/containerfs/logger"

	ns "github.com/ipdcode/containerfs/metanode/namespace"
	mRaft "github.com/ipdcode/containerfs/metanode/raft"
	mp "github.com/ipdcode/containerfs/proto/mp"

	"github.com/ipdcode/containerfs/utils"
	"github.com/lxmgo/config"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
	"net"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"
)

type addr struct {
	host     string
	port     int
	peer     []string
	domain   string
	log      string
	hadesurl string
}

var MetaNodeServerAddr addr

type MetaNodeServer struct {
	Mutex sync.Mutex
}

/*
rpc CreateNameSpace(CreateNameSpaceReq) returns (CreateNameSpaceAck){};
*/
func (s *MetaNodeServer) CreateNameSpace(ctx context.Context, in *mp.CreateNameSpaceReq) (*mp.CreateNameSpaceAck, error) {
	fmt.Println("CreateNameSpace...")
	ack := mp.CreateNameSpaceAck{}
	ns.Wg.Add(1)
	ack.Ret = ns.CreateNameSpace(in.VolID, false)

	if mRaft.RaftInfo.R.IsLeader() {
		// send to follower metadatas to registry a new map

		for _, addr := range MetaNodeServerAddr.peer {
			conn2, err2 := grpc.Dial(addr, grpc.WithInsecure())
			if err2 != nil {
				logger.Error("Leader told Follower to create NameSpace Failed ...")
				continue
			}
			defer conn2.Close()
			mc := mp.NewMetaNodeClient(conn2)
			pmCreateNameSpaceReq := &mp.CreateNameSpaceReq{
				VolID: in.VolID,
			}
			pmCreateNameSpaceAck, ret := mc.CreateNameSpace(context.Background(), pmCreateNameSpaceReq)
			if ret != nil {
				logger.Error("Leader told Follower to create NameSpace Failed ...")
				continue
			}
			if pmCreateNameSpaceAck.Ret != 0 {
				logger.Error("Leader told Follower to create NameSpace Failed ...")
				continue
			}
		}

	}
	return &ack, nil
}

/*
rpc GetFSInfo(GetFSInfoReq) returns (GetFSInfoAck){};
*/
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

/*
rpc CreateDir(CreateDirReq) returns (CreateDirAck){};
*/
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

/*
rpc StatDir(StatDirReq) returns (StatDirAck){};
*/
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

/*
rpc ListDir(StatDirReq) returns (ListDirAck){};
*/
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

/*
rpc DeleteDir(DeleteDirReq) returns (DeleteDirAck){};
*/
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

/*
rpc Rename(RenameReq) returns (RenameAck){};
*/
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

/*
rpc CreateFile(CreateFileReq) returns (CreateFileAck){};
*/
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

/*
rpc DeleteFile(DeleteFileReq) returns (DeleteFileAck){};
*/
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

/*
rpc AllocateChunk(AllocateChunkReq) returns (AllocateChunkAck){};
*/
func (s *MetaNodeServer) AllocateChunk(ctx context.Context, in *mp.AllocateChunkReq) (*mp.AllocateChunkAck, error) {
	ack := mp.AllocateChunkAck{}
	fileName := in.FileName
	volID := in.VolID
	ret, nameSpace := ns.GetNameSpace(volID)
	if ret != 0 {
		ack.Ret = ret
		return &ack, nil
	}
	ack.Ret, ack.ChunkInfo = nameSpace.AllocateChunk(fileName)
	ack.SequenceID = in.SequenceID
	return &ack, nil
}

/*
rpc GetChunks(GetChunksReq) returns (GetChunksAck){};
*/
func (s *MetaNodeServer) GetFileChunks(ctx context.Context, in *mp.GetFileChunksReq) (*mp.GetFileChunksAck, error) {
	ack := mp.GetFileChunksAck{}
	fileName := in.FileName
	volID := in.VolID
	ret, nameSpace := ns.GetNameSpace(volID)
	if ret != 0 {
		ack.Ret = ret
		return &ack, nil
	}
	ack.Ret, ack.ChunkInfos = nameSpace.GetFileChunks(fileName)
	return &ack, nil
}

/*
rpc GetChunks(GetChunksReq) returns (GetChunksAck){};
*/
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

func startMetaDataService() {

	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", MetaNodeServerAddr.port))
	if err != nil {
		panic(fmt.Sprintf("Failed to listen on:%v", MetaNodeServerAddr.port))
	}
	s := grpc.NewServer()
	mp.RegisterMetaNodeServer(s, &MetaNodeServer{})
	// Register reflection service on gRPC server.
	reflection.Register(s)
	if err := s.Serve(lis); err != nil {
		panic("Failed to serve")
	}
}

func loadMetaData() {
	ns.CreateGNameSpace()
	ret, vols := ns.GetVolList()
	if ret != 0 {
		logger.Error("loadMetaData,GetVolList failed,ret:%v", ret)
	}
	fmt.Println(vols)
	for _, v := range vols {
		ns.Wg.Add(1)
		go ns.CreateNameSpace(v, true)
	}
	ns.Wg.Wait()
}

func loadNewMetaData() {

	ret, vols := ns.GetVolList()
	if ret != 0 {
		logger.Error("loadMetaData,GetVolList failed,ret:%v", ret)
	}
	fmt.Println(vols)
	for _, v := range vols {
		ns.Wg.Add(1)
		go ns.LoadNewVolMeta(v)
	}
	ns.Wg.Wait()
}

func init() {
	//ns.CreateGNameSpace()

	c, err := config.NewConfig(os.Args[1])
	if err != nil {
		fmt.Println("NewConfig err")
		os.Exit(1)
	}

	MetaNodeServerAddr.host = c.String("host")
	port, _ := c.Int("port")
	MetaNodeServerAddr.port = port
	MetaNodeServerAddr.peer = c.Strings("peer")
	MetaNodeServerAddr.domain = c.String("hades::domain") + ".hades.local"
	ns.Domain = MetaNodeServerAddr.domain
	MetaNodeServerAddr.log = c.String("log")
	url := "http://" + c.String("hades::host") + "/hades/api/" + c.String("hades::domain") + "?token=" + c.String("hades::token")
	fmt.Println("hades url:")
	fmt.Println(url)
	MetaNodeServerAddr.hadesurl = url
	ns.VolMgrAddress = c.String("volmgr::host")

	mRaft.RaftInfo.Me = c.String("raft::me")
	s := strings.Split(mRaft.RaftInfo.Me, ":")
	mRaft.RaftInfo.MePort, _ = strconv.Atoi(s[1])
	mRaft.RaftInfo.Peer = c.Strings("raft::peer")

	logger.SetConsole(true)
	logger.SetRollingFile(MetaNodeServerAddr.log, "metanode.log", 10, 100, logger.MB) //each 100M rolling
	switch level := c.String("loglevel"); level {
	case "error":
		logger.SetLevel(logger.ERROR)
	case "debug":
		logger.SetLevel(logger.DEBUG)
	case "info":
		logger.SetLevel(logger.INFO)
	default:
		logger.SetLevel(logger.ERROR)
	}

	endPoints := strings.Split(c.String("etcd::etcd.endpoints"), ",")
	fmt.Println(endPoints)
	err = ns.EtcdClient.InitEtcd(endPoints)
	if err != nil {
		fmt.Println("connect etcd failed")
		os.Exit(0)
	}

	mRaft.StartRaftService()
	loadMetaData()

}

func main() {

	//for multi-cpu scheduling
	numCPU := runtime.NumCPU()
	runtime.GOMAXPROCS(numCPU)

	ticker1 := time.NewTicker(time.Millisecond * 100)
	var myRole bool = true
	var count int64
	go func() {
		for _ = range ticker1.C {
			if mRaft.RaftInfo.R.IsLeader() {
				count += 1
				if count == 1 {
					utils.DelHades(MetaNodeServerAddr.hadesurl)
					utils.PostHades(MetaNodeServerAddr.hadesurl, MetaNodeServerAddr.host, MetaNodeServerAddr.port)
				}
				if myRole != mRaft.RaftInfo.R.IsLeader() {
					// role change , load new meta immediately
					//loadNewMetaData()
					utils.DelHades(MetaNodeServerAddr.hadesurl)
					loadMetaData()
					utils.PostHades(MetaNodeServerAddr.hadesurl, MetaNodeServerAddr.host, MetaNodeServerAddr.port)
				}
				myRole = true
			} else {
				myRole = false
			}
		}
	}()

	ticker2 := time.NewTicker(time.Second * 5)
	go func() {
		for _ = range ticker2.C {
			if mRaft.RaftInfo.R.IsLeader() {
				fmt.Println("I'm leader...")
			} else {
				fmt.Println("I'm follower...")
				//fmt.Println("I'm follower, I will watch etcd again ...")
				// time to load new meta
				//loadNewMetaData()
			}
		}
	}()
	startMetaDataService()
}
