package main

import (
	"bufio"
	"flag"
	"fmt"
	"github.com/ipdcode/containerfs/logger"
	dp "github.com/ipdcode/containerfs/proto/dp"
	vp "github.com/ipdcode/containerfs/proto/vp"
	"github.com/ipdcode/containerfs/utils"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
	"net"
	"os"
	"io"
	"runtime"
	"runtime/debug"
	"strconv"
	"sync"
	"time"
)

// DataNodeServer ...
type DataNodeServer struct {
	Mutex sync.Mutex
}

type addr struct {
	Ipnr  net.IP
	IPInt int32
	IPStr string
	Port  int32
	Path  string
	Flag  string
	Log   string

	VolMgrHost string
}

// DataNodeServerAddr ...
var DataNodeServerAddr addr

func startDataService() {

	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", DataNodeServerAddr.Port))
	if err != nil {
		panic(fmt.Sprintf("Failed to listen on:%v", DataNodeServerAddr.Port))
	}
	s := grpc.NewServer()
	dp.RegisterDataNodeServer(s, &DataNodeServer{})
	reflection.Register(s)
	if err := s.Serve(lis); err != nil {
		panic("Failed to serve")
	}
}

func registryToVolMgr() {
	conn, err := grpc.Dial(DataNodeServerAddr.VolMgrHost, grpc.WithInsecure())
	if err != nil {
		logger.Debug("data node statup failed : Dial to volmgr failed !")
		os.Exit(1)
	}
	defer conn.Close()
	c := vp.NewVolMgrClient(conn)

	var datanodeRegistryReq vp.DatanodeRegistryReq
	datanodeRegistryReq.Ip = DataNodeServerAddr.IPInt
	datanodeRegistryReq.Port = DataNodeServerAddr.Port
	diskInfo := utils.DiskUsage(DataNodeServerAddr.Path)
	capacity := int32(float64(diskInfo.All) / float64(1024*1024*1024))
	datanodeRegistryReq.Capacity = capacity
	datanodeRegistryReq.MountPoint = DataNodeServerAddr.Path

	_, err = os.Stat(datanodeRegistryReq.MountPoint)
	if err != nil {
		logger.Error("data node statup failed : DataNodeServerAddr.Path not exist !")
		os.Exit(1)
	}

	pDatanodeRegistryAck, _ := c.DatanodeRegistry(context.Background(), &datanodeRegistryReq)
	if pDatanodeRegistryAck.Ret == 0 {
		logger.Debug("registry success!")
		os.Create(DataNodeServerAddr.Flag)
	} else {
		logger.Debug("data node statup failed : registry to volmgr failed !")
		os.Exit(1)
	}

	return

}

func heartbeatToVolMgr() {

	conn, err := grpc.Dial(DataNodeServerAddr.VolMgrHost, grpc.WithInsecure())
	if err != nil {
		logger.Debug("HearBeat failed : Dial to volmgr failed !")
	}
	defer conn.Close()
	c := vp.NewVolMgrClient(conn)

	diskInfo := utils.DiskUsage(DataNodeServerAddr.Path)
	free := int32(float64(diskInfo.Free) / float64(1024*1024*1024))
	used := int32(float64(diskInfo.Used) / float64(1024*1024*1024))

	var datanodeHeartbeatReq vp.DatanodeHeartbeatReq
	datanodeHeartbeatReq.Ip = DataNodeServerAddr.IPInt
	datanodeHeartbeatReq.Port = DataNodeServerAddr.Port
	datanodeHeartbeatReq.Free = free
	datanodeHeartbeatReq.Used = used
	//datanodeHeartbeatReq.Status = 0

	f, err := os.OpenFile(DataNodeServerAddr.Path+"/health", os.O_RDWR|os.O_TRUNC|os.O_CREATE, 0666)
	if err != nil {
		logger.Error("Open datanode check health file error:%v", err)
		datanodeHeartbeatReq.Status = 2
	}
	defer f.Close()

	_, err = f.WriteString("ok")
	if err != nil {
		logger.Error("Write datanode check health file error:%v", err)
		datanodeHeartbeatReq.Status = 2
	} else {
		datanodeHeartbeatReq.Status = 0
	}

	c.DatanodeHeartbeat(context.Background(), &datanodeHeartbeatReq)
}

// DatanodeHealthCheck rpc GetChunks(GetChunksReq) returns (GetChunksAck){};
func (s *DataNodeServer) DatanodeHealthCheck(ctx context.Context, in *dp.DatanodeHealthCheckReq) (*dp.DatanodeHealthCheckAck, error) {
	ack := dp.DatanodeHealthCheckAck{}
	ack.Ret = 1
	return &ack, nil
}

// WriteChunk ...
func (s *DataNodeServer) WriteChunk(ctx context.Context, in *dp.WriteChunkReq) (*dp.WriteChunkAck, error) {
	var f *os.File
	var err error

	ack := dp.WriteChunkAck{}
	chunkID := in.ChunkID
	blockID := in.BlockID

	path := DataNodeServerAddr.Path + "/block-" + strconv.Itoa(int(blockID))
	if ok, err := utils.LocalPathExists(path); !ok && err == nil {
		os.MkdirAll(path, 0777)
	}

	chunkFileName := path + "/chunk-" + strconv.Itoa(int(chunkID))

	f, err = os.OpenFile(chunkFileName, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0660)
	defer f.Close()
	if err != nil {
		ack.Ret = -1
		return &ack, nil
	}
	w := bufio.NewWriter(f)
	w.Write(in.Databuf)
	w.Flush()

	ack.Ret = 0
	return &ack, nil
}

/*rpc WriteChunkStream(stream WriteChunkReq) returns (WriteChunkAck){}; */
/*
func (s *DataNodeServer) WriteChunkStream(stream dp.DataNode_WriteChunkStreamServer) error {

	var f *os.File
	var err error
	//fmt.Println("writechunking in datanode ...")
	ack := dp.WriteChunkAck{}
	if err != nil {
		ack.Ret = -1
		return stream.SendAndClose(&ack)
	}
	for {
		in, err := stream.Recv()
		if err == io.EOF {
			ack.Ret = 0
			return stream.SendAndClose(&ack)
		}
		if err != nil {
			ack.Ret = 1
			return stream.SendAndClose(&ack)
		}
		chunkID := in.ChunkID
		blockID := in.BlockID
		chunkFileName := DataNodeServerAddr.Path + "/block-" + strconv.Itoa(int(blockID)) + "/chunk-" + strconv.Itoa(int(chunkID))
		f, err = os.OpenFile(chunkFileName, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644)
		f.WriteString(in.Databuf)
		f.Close()
	}
}
*/
/*
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
		ack.Databuf = utils.B2S(buf)
		ack.Ret = 1
		ack.Readsize = int64(n)
		return &ack, nil
	}
}
*/

// StreamReadChunk ...
func (s *DataNodeServer) StreamReadChunk(in *dp.StreamReadChunkReq, stream dp.DataNode_StreamReadChunkServer) error {
	chunkID := in.ChunkID
	blockID := in.BlockID
	//offset := in.Offset
	readsize := in.Readsize

	chunkFileName := DataNodeServerAddr.Path + "/block-" + strconv.Itoa(int(blockID)) + "/chunk-" + strconv.Itoa(int(chunkID))
	f, err := os.Open(chunkFileName)
	defer f.Close()
	if err != nil {
		return err
	}

	var ack dp.StreamReadChunkAck
	var totalsize int64
	buf := make([]byte, 2*1024*1024)
	
	bfRd := bufio.NewReader(f)
	for {
		n, err := bfRd.Read(buf)
		if err != nil && err != io.EOF {
			logger.Error("read chunkfile:%v error:%v",chunkFileName,err)
			return err
		}

		if n == 0 {
            logger.Debug("read chunkfile:%v endsize:%v",chunkFileName,totalsize)
            break
        }
		totalsize += int64(n)
		
		ack.Databuf = buf[:n]
		if err := stream.Send(&ack); err != nil {
			logger.Error("Send stream data to fuse error:%v", err)
			return err
		}

		if totalsize == readsize {
            logger.Debug("Request chunkFileName:%v size:%v equal real ret size:%v", chunkFileName, readsize, totalsize)
            break
        }
	}

	return nil
}

//DeleteChunk rpc DeleteChunks(eleteChunksReq) returns (eleteChunksAck){};
func (s *DataNodeServer) DeleteChunk(ctx context.Context, in *dp.DeleteChunkReq) (*dp.DeleteChunkAck, error) {
	var err error

	ack := dp.DeleteChunkAck{}
	chunkID := in.ChunkID
	blockID := in.BlockID

	chunkFileName := DataNodeServerAddr.Path + "/block-" + strconv.Itoa(int(blockID)) + "/chunk-" + strconv.Itoa(int(chunkID))

	err = os.Remove(chunkFileName)
	if err != nil {
		ack.Ret = 0
	} else {
		ack.Ret = 0
	}
	ack.Ret = 0
	return &ack, nil
}

func init() {

	var loglevel string
	var port int

	flag.StringVar(&DataNodeServerAddr.IPStr, "host", "127.0.0.1", "ContainerFS DataNode Host")
	flag.IntVar(&port, "port", 8000, "ContainerFS DataNode Port")
	flag.StringVar(&DataNodeServerAddr.Path, "datapath", "", "ContainerFS DataNode Data Path")
	flag.StringVar(&DataNodeServerAddr.VolMgrHost, "volmgr", "127.0.0.1:7000", "ContainerFS VolMgr Host")
	flag.StringVar(&DataNodeServerAddr.Log, "logpath", "/export/Logs/containerfs/logs/", "ContainerFS Log Path")
	flag.StringVar(&loglevel, "loglevel", "error", "ContainerFS Log Level")

	flag.Parse()

	DataNodeServerAddr.Port = int32(port)
	ipnr := net.ParseIP(DataNodeServerAddr.IPStr)
	DataNodeServerAddr.Ipnr = ipnr
	ipint := utils.InetAton(ipnr)
	DataNodeServerAddr.IPInt = ipint
	DataNodeServerAddr.Flag = DataNodeServerAddr.Path + "/.registryflag"

	logger.SetConsole(true)
	logger.SetRollingFile(DataNodeServerAddr.Log, "datanode.log", 10, 100, logger.MB) //each 100M rolling

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

	if ok, _ := utils.LocalPathExists(DataNodeServerAddr.Flag); !ok {
		logger.Debug("Start registry to volmgr ...")
		registryToVolMgr()
		logger.Debug("registry to volmgr success")
	} else {
		logger.Debug("already registied")
	}

}

func main() {

	numCPU := runtime.NumCPU()
	runtime.GOMAXPROCS(numCPU)

	defer func() {
		if err := recover(); err != nil {
			logger.Error("panic !!! :%v", err)
			logger.Error("stacks:%v", string(debug.Stack()))
		}
	}()

	heartbeatToVolMgr()
	ticker := time.NewTicker(time.Second * 60)
	go func() {
		for range ticker.C {
			heartbeatToVolMgr()
		}
	}()
	startDataService()
}
