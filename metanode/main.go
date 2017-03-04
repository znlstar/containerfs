package main

import (
	"fmt"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
	ns "ipd.org/containerfs/metanode/namespace"
	mp "ipd.org/containerfs/proto/mp"
	"net"
	"runtime"
	"sync"
	"time"
)

type RpcConfigOpts struct {
	ListenPort uint16 `gcfg:"listen-port"`
	ClientPort uint16 `gcfg:"client-port"`
}

var g_RpcConfig RpcConfigOpts

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
rpc CreateDir(CreateDirReq) returns (CreateDirAck){};
*/
func (s *MetaNodeServer) CreateDir(ctx context.Context, in *mp.CreateDirReq) (*mp.CreateDirAck, error) {
	//fmt.Printf("CreateDir...")
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

	fmt.Println("DeleteFile in main ...")
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
	//fmt.Println("AllocateChunk...")
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
	//fmt.Println("getfilechunks ... ")
	//fmt.Println(ack)
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
	g_RpcConfig.ListenPort = 10002
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", g_RpcConfig.ListenPort))
	if err != nil {
		panic(fmt.Sprintf("Failed to listen on:%v", g_RpcConfig.ListenPort))
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
	/*
		nameSpace := ns.GetNameSpace("UUID1")
		//fmt.Println(nameSpace.InodeDB)
		nameSpace.Mutex.RLock()
		b, _ := json.Marshal(nameSpace.InodeDB)
		nameSpace.Mutex.RUnlock()
		err := ioutil.WriteFile("/home/meta.data", b, 0666)
		if err != nil {
			panic(err)
		}

		b1, _ := json.Marshal(nameSpace.BaseInodeID.Cur)
		err1 := ioutil.WriteFile("/home/inodenum.data", b1, 0666)
		if err1 != nil {
			panic(err1)
		}

		b2, _ := json.Marshal(nameSpace.BaseChunkID.Cur)
		err2 := ioutil.WriteFile("/home/chunknum.data", b2, 0666)
		if err1 != nil {
			panic(err2)
		}
	*/
}

// todo:load all vol meta
func loadMetaData() {

	ns.CreateGNameSpace()
	/*
		dir, err := ioutil.ReadDir(metaPath)
		if err != nil {
			return
		}
		for i := range dir {
			// etc. 27727e1040b9f5278062646fe6b74cbf
			if len(dir[i]) != 32 {
				continue
			}
			b1, err1 := ioutil.ReadFile(metaPath + "/" + dir[i] + inodeMetafile)
			if err1 != nil {
				continue
			}
			b2, err2 := ioutil.ReadFile(metaPath + "/" + dir[i] + blkgrpMetafile)
			if err2 != nil {
				continue
			}
			b3, err3 := ioutil.ReadFile(metaPath + "/" + dir[i] + chunkMetafile)
			if err3 != nil {
				continue
			}
			b4, err4 := ioutil.ReadFile(metaPath + "/" + dir[i] + baseinodeNumfile)
			if err4 != nil {
				continue
			}
			b5, err5 := ioutil.ReadFile(metaPath + "/" + dir[i] + basechunkNumfile)
			if err5 != nil {
				continue
			}
			var inodenum int64
			var chunknum int64
			json.Unmarshal(b4, &inodenum)
			json.Unmarshal(b5, &chunknum)

			ret1 := ns.CreateNameSpace(dir[i], inodenum+1, chunknum+1)
			if ret1 != 0 {
				return
			}

			re2t, nameSpace := ns.GetNameSpace(dir[i])
			if ret != 0 {
				return
			}
			json.Unmarshal(b1, &nameSpace.InodeDB)
			json.Unmarshal(b2, &nameSpace.BlockBroupDB)
			json.Unmarshal(b3, &nameSpace.ChunkDB)
		}
	*/
}

func init() {
	ns.CreateGNameSpace()
	//loadMetaData()

	/*
		var inodeID int64
		fmt.Println("CreateNameSpace...")
		ns.CreateNameSpace("UUID1", 1, 1)
		fmt.Println("CreateNameSpace......")
		nameSpace := ns.GetNameSpace("UUID1")
		inodeID = 0
		tmpInodeInfo := mp.InodeInfo{InodeID: inodeID, Name: "/", AccessTime: time.Now().Unix(), ModifiTime: time.Now().Unix(), InodeType: false}
		nameSpace.Set("0", &tmpInodeInfo)
	*/
}

func main() {

	//for multi-cpu scheduling
	numCPU := runtime.NumCPU()
	runtime.GOMAXPROCS(numCPU)

	ticker := time.NewTicker(time.Second * 60)
	go func() {
		for _ = range ticker.C {
			//fmt.Printf("ticked at %v", time.Now())
			saveMetaData()
		}
	}()

	startMetaDataService()
}
