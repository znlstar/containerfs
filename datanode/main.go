package main

import (
	//"encoding/json"
	"fmt"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
	//"io/ioutil"
	"bufio"
	dp "ipd.org/containerfs/proto/dp"
	vp "ipd.org/containerfs/proto/vp"
	"ipd.org/containerfs/utils"
	"net"
	"os"
	"runtime"
	"strconv"
	"sync"
	"time"
)

type RpcConfigOpts struct {
	ListenPort int32 `gcfg:"listen-port"`
	ClientPort int32 `gcfg:"client-port"`
}

var g_RpcConfig RpcConfigOpts

type DataNodeServer struct {
	Mutex sync.Mutex
}

type addr struct {
	Ipnr  net.IP
	IpInt int32
	IpStr string
	Port  int32
	Path  string
	Flag  string
}

var DataNodeServerAddr addr

func startDataService() {
	g_RpcConfig.ListenPort = DataNodeServerAddr.Port
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", g_RpcConfig.ListenPort))
	if err != nil {
		panic(fmt.Sprintf("Failed to listen on:%v", g_RpcConfig.ListenPort))
	}
	s := grpc.NewServer()
	dp.RegisterDataNodeServer(s, &DataNodeServer{})
	// Register reflection service on gRPC server.
	reflection.Register(s)
	if err := s.Serve(lis); err != nil {
		panic("Failed to serve")
	}
}

func registryToVolMgr() {
	fmt.Println(os.Args[4])
	conn, err := grpc.Dial(os.Args[4]+":10001", grpc.WithInsecure())
	if err != nil {
		fmt.Printf("did not connect: %v", err)
	}
	defer conn.Close()
	c := vp.NewVolMgrClient(conn)

	var datanodeRegistryReq vp.DatanodeRegistryReq
	datanodeRegistryReq.Ip = DataNodeServerAddr.IpInt
	datanodeRegistryReq.Port = DataNodeServerAddr.Port
	diskInfo := utils.DiskUsage(DataNodeServerAddr.Path)
	capacity := int32(float64(diskInfo.All) / float64(1024*1024*1024))
	datanodeRegistryReq.Capacity = capacity
	datanodeRegistryReq.MountPoint = DataNodeServerAddr.Path

	pDatanodeRegistryAck, _ := c.DatanodeRegistry(context.Background(), &datanodeRegistryReq)
	if pDatanodeRegistryAck.Ret == 0 {
		fmt.Println(pDatanodeRegistryAck)
		fmt.Println("registry success!")
		os.Create(DataNodeServerAddr.Flag)
		for i := pDatanodeRegistryAck.StartBlockID; i <= pDatanodeRegistryAck.EndBlockID; i++ {
			os.MkdirAll(DataNodeServerAddr.Path+"/block-"+strconv.Itoa(int(i)), 0777)
		}
	} else {
		fmt.Println("data node statup failed : registry to volmgr failed !")
		os.Exit(1)
	}

	return

}

func heartbeatToVolMgr() {

	conn, err := grpc.Dial(os.Args[4]+":10001", grpc.WithInsecure())
	if err != nil {
		fmt.Printf("did not connect: %v", err)
	}
	defer conn.Close()
	c := vp.NewVolMgrClient(conn)

	diskInfo := utils.DiskUsage(DataNodeServerAddr.Path)
	free := int32(float64(diskInfo.Free) / float64(1024*1024*1024))
	used := int32(float64(diskInfo.Used) / float64(1024*1024*1024))

	var datanodeHeartbeatReq vp.DatanodeHeartbeatReq
	datanodeHeartbeatReq.Ip = DataNodeServerAddr.IpInt
	datanodeHeartbeatReq.Port = DataNodeServerAddr.Port
	datanodeHeartbeatReq.Free = free
	datanodeHeartbeatReq.Used = used
	datanodeHeartbeatReq.Status = 0

	c.DatanodeHeartbeat(context.Background(), &datanodeHeartbeatReq)
}

/*
func isFileExist(filename string) bool {
	_, err := os.Stat(filename)
	return err == nil || os.IsExist(err)
}
*/

/*
rpc GetChunks(GetChunksReq) returns (GetChunksAck){};
*/
func (s *DataNodeServer) WriteChunk(ctx context.Context, in *dp.WriteChunkReq) (*dp.WriteChunkAck, error) {
	var f *os.File
	var err error

	//fmt.Println("writechunking in datanode ...")
	ack := dp.WriteChunkAck{}
	chunkID := in.ChunkID
	blockID := in.BlockID

	chunkFileName := DataNodeServerAddr.Path + "/block-" + strconv.Itoa(int(blockID)) + "/chunk-" + strconv.Itoa(int(chunkID))
	fmt.Println(chunkFileName)

	f, err = os.OpenFile(chunkFileName, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644)
	defer f.Close()
	if err != nil {
		ack.Ret = -1
		return &ack, nil
	}
	fmt.Println(len(in.Databuf))
	f.WriteString(in.Databuf)
	ack.Ret = 0
	return &ack, nil
}

func (s *DataNodeServer) ReadChunk(ctx context.Context, in *dp.ReadChunkReq) (*dp.ReadChunkAck, error) {
	ack := dp.ReadChunkAck{}
	chunkID := in.ChunkID
	blockID := in.BlockID
	readsize := in.Readlen
	offset := in.Offset

	chunkFileName := DataNodeServerAddr.Path + "/block-" + strconv.Itoa(int(blockID)) + "/chunk-" + strconv.Itoa(int(chunkID))
	f, err := os.Open(chunkFileName)
	defer f.Close()
	if err != nil {
		ack.Ret = -1
		return &ack, nil
	}
	_, err = f.Seek(offset, 0)
	if err != nil {
		ack.Ret = -1
		return &ack, nil
	}
	
	buf := make([]byte, readsize)
	bfRd := bufio.NewReader(f)
	for {
		n, err := bfRd.Read(buf)
		if err != nil {
			ack.Ret = -1
			return &ack, nil
		}
		fmt.Printf("#### buflen:%v #### bufcap:%v ####\n",len(buf),cap(buf))
		ack.Databuf = utils.B2S(buf)
		ack.Ret = 1
		ack.Readsize = int64(n)
		return &ack, nil
	}
}

func init() {
	argNum := len(os.Args)
	if argNum != 5 {
		fmt.Println("data node statup failed : arg num err !")
		fmt.Println(argNum)
		os.Exit(1)
	}

	DataNodeServerAddr.IpStr = os.Args[1]
	ipnr := net.ParseIP(DataNodeServerAddr.IpStr)
	DataNodeServerAddr.Ipnr = ipnr
	ipint := utils.Inet_aton(ipnr)
	DataNodeServerAddr.IpInt = ipint
	port, _ := strconv.Atoi(os.Args[2])
	DataNodeServerAddr.Port = int32(port)
	DataNodeServerAddr.Path = os.Args[3]
	DataNodeServerAddr.Flag = DataNodeServerAddr.Path + "/.registryflag"
	if ok, _ := utils.LocalPathExists(DataNodeServerAddr.Flag); !ok {
		fmt.Println("registy ...")
		registryToVolMgr()
		fmt.Println("registy end ...")
	} else {
		fmt.Println("already registied")
	}

}

func main() {

	numCPU := runtime.NumCPU()
	runtime.GOMAXPROCS(numCPU)

	ticker := time.NewTicker(time.Second * 5)
	go func() {
		for _ = range ticker.C {
			////fmt.Printf("ticked at %v", time.Now())
			heartbeatToVolMgr()
		}
	}()
	startDataService()
}
