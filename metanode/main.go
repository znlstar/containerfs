package main

import (
	"fmt"
	"github.com/lxmgo/config"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
	"ipd.org/containerfs/logger"
	ns "ipd.org/containerfs/metanode/namespace"
	mp "ipd.org/containerfs/proto/mp"
	"net"
	"os"
	"runtime"
	"sync"
	"time"
)

type addr struct {
	host string
	port int
	log  string
}

var MetaNodeServerAddr addr

type MetaNodeServer struct {
	Mutex sync.Mutex
}

const (
	metaPath         = "/home/metadata"
	blkgrpMetafile   = "blkgrp.meta"
	chunkMetafile    = "chunk.meta"
	inodeMetafile    = "inode.meta"
	basechunkNumfile = "basechunk.num"
	baseinodeNumfile = "baseinode.num"
)

/*
rpc CreateNameSpace(CreateNameSpaceReq) returns (CreateNameSpaceAck){};
*/
func (s *MetaNodeServer) CreateNameSpace(ctx context.Context, in *mp.CreateNameSpaceReq) (*mp.CreateNameSpaceAck, error) {
	ack := mp.CreateNameSpaceAck{}
	ack.Ret = ns.CreateNameSpace(in.VolID, 1, 1)
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

// todo:save all vol meta
func saveMetaData() {
}

// todo:load all vol meta
func loadMetaData() {
	ns.CreateGNameSpace()
}

func init() {
	ns.CreateGNameSpace()
	//loadMetaData()

	c, err := config.NewConfig(os.Args[1])
	if err != nil {
		fmt.Println("NewConfig err")
		os.Exit(1)
	}
	port, _ := c.Int("port")
	MetaNodeServerAddr.port = port
	MetaNodeServerAddr.log = c.String("log")
	MetaNodeServerAddr.host = c.String("host")

	ns.VolMgrAddress = c.String("volmgr::volmgr.host")

	logger.SetConsole(true)
	logger.SetRollingFile(MetaNodeServerAddr.log, "metanode.log", 10, 100, logger.MB) //each 100M rolling
	logger.SetLevel(logger.DEBUG)
}

func main() {

	//for multi-cpu scheduling
	numCPU := runtime.NumCPU()
	runtime.GOMAXPROCS(numCPU)

	ticker := time.NewTicker(time.Second * 60)
	go func() {
		for _ = range ticker.C {
			saveMetaData()
		}
	}()

	startMetaDataService()
}
