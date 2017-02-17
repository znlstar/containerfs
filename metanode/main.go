package main

import (
	"encoding/json"
	"fmt"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
	"io/ioutil"
	ns "ipd.org/containerfs/metanode/namespace"
	"ipd.org/containerfs/metanode/protobuf"
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

/*
rpc CreateNameSpace(CreateNameSpaceReq) returns (CreateNameSpaceAck){};
*/
func (s *MetaNodeServer) CreateNameSpace(ctx context.Context, in *protobuf.CreateNameSpaceReq) (*protobuf.CreateNameSpaceAck, error) {
	ack := protobuf.CreateNameSpaceAck{}
	ns.CreateNameSpace(in.VolID, 1)
	ack.Ret = 0
	return &ack, nil
}

/*
rpc CreateDir(CreateDirReq) returns (CreateDirAck){};
*/
func (s *MetaNodeServer) CreateDir(ctx context.Context, in *protobuf.CreateDirReq) (*protobuf.CreateDirAck, error) {
	//fmt.Printf("CreateDir...")
	ack := protobuf.CreateDirAck{}
	fullPathName := in.FullPathName
	volID := in.VolID
	nameSpace := ns.GetNameSpace(volID)
	ack.Ret = nameSpace.CreateDir(fullPathName)
	return &ack, nil
}

/*
rpc StatDir(StatDirReq) returns (StatDirAck){};
*/
func (s *MetaNodeServer) Stat(ctx context.Context, in *protobuf.StatReq) (*protobuf.StatAck, error) {
	ack := protobuf.StatAck{}
	fullPathName := in.FullPathName
	volID := in.VolID
	nameSpace := ns.GetNameSpace(volID)
	ack.InodeInfo, ack.Ret = nameSpace.Stat(fullPathName)
	return &ack, nil
}

/*
rpc ListDir(StatDirReq) returns (ListDirAck){};
*/
func (s *MetaNodeServer) List(ctx context.Context, in *protobuf.ListReq) (*protobuf.ListAck, error) {
	ack := protobuf.ListAck{}
	fullPathName := in.FullPathName
	volID := in.VolID
	nameSpace := ns.GetNameSpace(volID)
	ack.InodeInfos, ack.Ret = nameSpace.List(fullPathName)
	return &ack, nil
}

/*
rpc DeleteDir(DeleteDirReq) returns (DeleteDirAck){};
*/
func (s *MetaNodeServer) DeleteDir(ctx context.Context, in *protobuf.DeleteDirReq) (*protobuf.DeleteDirAck, error) {

	ack := protobuf.DeleteDirAck{}
	fullPathName := in.FullPathName
	volID := in.VolID
	nameSpace := ns.GetNameSpace(volID)
	ack.Ret = nameSpace.DeleteDir(fullPathName)
	return &ack, nil

}

/*
rpc DeleteDir(DeleteDirReq) returns (DeleteDirAck){};
*/
func (s *MetaNodeServer) Rename(ctx context.Context, in *protobuf.RenameReq) (*protobuf.RenameAck, error) {
	ack := protobuf.RenameAck{}
	fullPathName1 := in.FullPathName1
	fullPathName2 := in.FullPathName2
	volID := in.VolID
	nameSpace := ns.GetNameSpace(volID)
	ack.Ret = nameSpace.Rename(fullPathName1, fullPathName2)
	return &ack, nil

}

/*
rpc CreateFile(CreateFileReq) returns (CreateFileAck){};
*/
func (s *MetaNodeServer) CreateFile(ctx context.Context, in *protobuf.CreateFileReq) (*protobuf.CreateFileAck, error) {
	ack := protobuf.CreateFileAck{}
	fullPathName := in.FullPathName
	volID := in.VolID
	nameSpace := ns.GetNameSpace(volID)
	ack.Ret = nameSpace.CreateFile(fullPathName)
	return &ack, nil
}

func startMetaDataService() {
	g_RpcConfig.ListenPort = 10002
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", g_RpcConfig.ListenPort))
	if err != nil {
		panic(fmt.Sprintf("Failed to listen on:%v", g_RpcConfig.ListenPort))
	}
	s := grpc.NewServer()
	protobuf.RegisterMetaNodeServer(s, &MetaNodeServer{})
	// Register reflection service on gRPC server.
	reflection.Register(s)
	if err := s.Serve(lis); err != nil {
		panic("Failed to serve")
	}
}

// todo:save all vol meta
func saveMetaData() {
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

}

// todo:load all vol meta
func loadMetaData() {
	b, _ := ioutil.ReadFile("/home/meta.data")
	b1, _ := ioutil.ReadFile("/home/inodenum.data")

	var inodenum int64
	json.Unmarshal(b1, &inodenum)

	ns.CreateNameSpace("UUID1", inodenum+1)

	nameSpace := ns.GetNameSpace("UUID1")
	json.Unmarshal(b, &nameSpace.InodeDB)
	//fmt.Println(nameSpace.InodeDB)

}

func init() {
	ns.CreateGNameSpace()
	//loadMetaData()

	var inodeID int64
	fmt.Println("CreateNameSpace...")
	ns.CreateNameSpace("UUID1", 1)
	fmt.Println("CreateNameSpace......")
	nameSpace := ns.GetNameSpace("UUID1")
	inodeID = 0
	tmpInodeInfo := protobuf.InodeInfo{InodeID: inodeID, Name: "/", AccessTime: time.Now().Unix(), ModifiTime: time.Now().Unix(), InodeType: false}
	nameSpace.Set("0", &tmpInodeInfo)

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
