package main

import (
	"bufio"
	"errors"
	"flag"
	"fmt"
	"github.com/tigcode/containerfs/logger"
	dp "github.com/tigcode/containerfs/proto/dp"
	mp "github.com/tigcode/containerfs/proto/mp"
	"github.com/tigcode/containerfs/utils"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	//"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/reflection"
	"io"
	"io/ioutil"
	"net"
	"os"
	"runtime"
	"runtime/debug"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/shirou/gopsutil/cpu"
	"github.com/shirou/gopsutil/disk"
	"github.com/shirou/gopsutil/load"
	"github.com/shirou/gopsutil/mem"
	utilnet "github.com/shirou/gopsutil/net"
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
	Tier  string
}

var MetaNodePeers []string
var MetaNodeAddr string

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

func registryToMeta() {
	conn, err := DialMeta("Cluster")
	if err != nil {
		logger.Error("registryToMeta: Dail Meta Failed err:%v", err)
		os.Exit(1)
	}
	defer conn.Close()
	mc := mp.NewMetaNodeClient(conn)

	dnIP := utils.InetNtoa(DataNodeServerAddr.IPInt)

	var datanodeRegistryReq mp.Datanode
	datanodeRegistryReq.Ip = dnIP.String()
	datanodeRegistryReq.Port = DataNodeServerAddr.Port
	diskInfo := utils.DiskUsage(DataNodeServerAddr.Path)
	datanodeRegistryReq.Capacity = int32(float64(diskInfo.All) / float64(1024*1024*1024))
	datanodeRegistryReq.Free = int32(float64(diskInfo.Free) / float64(1024*1024*1024))
	datanodeRegistryReq.Used = int32(float64(diskInfo.Used) / float64(1024*1024*1024))
	datanodeRegistryReq.MountPoint = DataNodeServerAddr.Path
	datanodeRegistryReq.Tier = DataNodeServerAddr.Tier
	datanodeRegistryReq.Status = 0

	ack, err := mc.DatanodeRegistry(context.Background(), &datanodeRegistryReq)
	if err != nil {
		logger.Debug("datanode statup failed : registry to metanode failed ! err %v", err)
		os.Exit(1)
	}
	if ack.Ret == 0 {
		logger.Debug("registry this datanode to metanode success!")
		os.Create(DataNodeServerAddr.Flag)
	} else {
		logger.Debug("datanode statup failed : registry to metanode failed !")
		os.Exit(1)
	}

	return
}

// DatanodeHealthCheck rpc GetChunks(GetChunksReq) returns (GetChunksAck){};
func (s *DataNodeServer) DatanodeHealthCheck(ctx context.Context, in *dp.DatanodeHealthCheckReq) (*dp.DatanodeHealthCheckAck, error) {
	ack := dp.DatanodeHealthCheckAck{}
	f, err := os.OpenFile(DataNodeServerAddr.Path+"/health", os.O_RDWR|os.O_TRUNC|os.O_CREATE, 0666)
	if err != nil {
		logger.Error("Open datanode check health file error:%v", err)
		ack.Status = 2
	} else {
		_, err = f.WriteString("ok")
		if err != nil {
			logger.Error("Write datanode check health file error:%v", err)
			ack.Status = 2
		}
	}

	diskInfo := utils.DiskUsage(DataNodeServerAddr.Path)
	ack.Used = int32(float64(diskInfo.Used) / float64(1024*1024*1024))
	ack.Ret = 0
	return &ack, nil
}

// SeekWriteChunk ...
func (s *DataNodeServer) SeekWriteChunk(ctx context.Context, in *dp.SeekWriteChunkReq) (*dp.WriteChunkAck, error) {
	var f *os.File
	var err error
	var sret int64
	var ret int

	ack := dp.WriteChunkAck{}
	chunkID := in.ChunkID
	blockID := in.BlockID
	chunkOffset := in.ChunkOffset

	path := DataNodeServerAddr.Path + "/block-" + strconv.Itoa(int(blockID))
	if ok, err := utils.LocalPathExists(path); !ok && err == nil {
		os.MkdirAll(path, 0777)
	}

	chunkFileName := path + "/chunk-" + strconv.Itoa(int(chunkID))

	logger.Debug("write file %v with offset %v and len %v", chunkFileName, chunkOffset, len(in.Databuf))

	f, err = os.OpenFile(chunkFileName, os.O_RDWR|os.O_CREATE, 0660)
	defer f.Close()
	if err != nil {
		logger.Error("Openfile:%v  error:%v ", chunkFileName, err)
		ack.Ret = -1
		return &ack, nil
	}

	sret, err = f.Seek(chunkOffset, 0)
	if sret != chunkOffset || err != nil {
		logger.Error("%v Seek to:%v ret:%v error:%v ", chunkFileName, chunkOffset, sret, err)
		ack.Ret = -1
		return &ack, nil
	}

	ret, err = f.Write(in.Databuf)
	if ret != len(in.Databuf) || err != nil {
		logger.Error("%v Write len:%v ret:%v error:%v ", chunkFileName, len(in.Databuf), ret, err)
		ack.Ret = -1
		return &ack, nil
	}

	/*w := bufio.NewWriter(f)
	_, err = w.Write(in.Databuf)
	if err != nil {
		logger.Error("%v Write error:%v ", chunkFileName, err)
		ack.Ret = -1
		return &ack, nil
	}

	err = w.Flush()
	if err != nil {
		logger.Error("%v Flush error:%v ", chunkFileName, err)
		ack.Ret = -1
		return &ack, nil
	}*/

	ack.Ret = 0
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
		logger.Error("Openfile:%v  error:%v ", chunkFileName, err)
		ack.Ret = -1
		return &ack, nil
	}
	w := bufio.NewWriter(f)
	_, err = w.Write(in.Databuf)
	if err != nil {
		ack.Ret = -1
		return &ack, nil
	}

	err = w.Flush()
	if err != nil {
		ack.Ret = -1
		return &ack, nil
	}

	ack.Ret = 0
	return &ack, nil
}

// StreamReadChunk ...
func (s *DataNodeServer) StreamReadChunk(in *dp.StreamReadChunkReq, stream dp.DataNode_StreamReadChunkServer) error {
	chunkID := in.ChunkID
	blockID := in.BlockID
	offset := in.Offset
	readsize := in.Readsize

	chunkFileName := DataNodeServerAddr.Path + "/block-" + strconv.Itoa(int(blockID)) + "/chunk-" + strconv.Itoa(int(chunkID))
	f, err := os.Open(chunkFileName)
	defer f.Close()
	if err != nil {
		return err
	}

	sret, err := f.Seek(offset, 0)
	if sret != offset || err != nil {
		logger.Error("%v Seek to:%v ret:%v error:%v ", chunkFileName, offset, sret, err)
		return err
	}

	var ack dp.StreamReadChunkAck
	var totalsize int64
	bufsize := readsize
	if bufsize > 2*1024*1024 {
		bufsize = 2 * 1024 * 1024
	}
	buf := make([]byte, bufsize)

	bfRd := bufio.NewReader(f)
	for {
		n, err := bfRd.Read(buf)
		if err != nil && err != io.EOF {
			logger.Error("read chunkfile:%v error:%v", chunkFileName, err)
			return err
		}

		if n == 0 {
			logger.Debug("read chunkfile:%v endsize:%v", chunkFileName, totalsize)
			break
		}

		totalsize += int64(n)

		if totalsize >= readsize {

			n = n - int((totalsize - readsize))
			ack.Databuf = buf[:n]
			if err := stream.Send(&ack); err != nil {
				logger.Error("Send stream data to fuse error:%v", err)
				return err
			}
			break

		} else {
			ack.Databuf = buf[:n]
			if err := stream.Send(&ack); err != nil {
				logger.Error("Send stream data to fuse error:%v", err)
				return err
			}
		}

	}

	return nil
}

func (s *DataNodeServer) RecvMigrateMsg(ctx context.Context, in *dp.RecvMigrateReq) (*dp.RecvMigrateAck, error) {
	ack := dp.RecvMigrateAck{}
	sid := in.SrcBlkID
	smount := in.SrcMount
	did := in.DstBlkID
	dip := in.DstIP
	dport := in.DstPort
	dmount := in.DstMount

	dDnAddr := dip + fmt.Sprintf(":%d", dport)
	/*
		conn, err := grpc.Dial(dDnAddr, grpc.WithInsecure(), grpc.WithKeepaliveParams(keepalive.ClientParameters{
			Timeout: 5 * time.Minute,
		}))
	*/
	conn, err := grpc.Dial(dDnAddr, grpc.WithInsecure())
	if err != nil {
		logger.Error("Migrate failed : Dial to DestDataNode:%v failed:%v !", dDnAddr, err)
		ack.Ret = -1
		return &ack, err
	}
	defer conn.Close()
	dc := dp.NewDataNodeClient(conn)

	ctxtmp, _ := context.WithTimeout(context.Background(), 5*time.Minute)

	stream, err := dc.SendMigrateData(ctxtmp)
	if err != nil {
		logger.Error("SendMigrate to DestDataNode:%v err:%v", dDnAddr, err)
		ack.Ret = -1
		return &ack, err
	}

	blkpath := smount + fmt.Sprintf("/block-%d", sid)
	if ok, err := utils.LocalPathExists(blkpath); !ok && err == nil {
		logger.Debug("The Block:%v no chunkdata, so dont need copydata for Migrate", sid)
		ack.Ret = 0
		return &ack, nil
	}
	dirs, err := ioutil.ReadDir(blkpath)
	if err != nil {
		logger.Error("List SrcBlk:%v failed:%v for Migrate", blkpath, err)
		ack.Ret = -1
		return &ack, err
	}

	for _, v := range dirs {
		if v.IsDir() {
			continue
		}
		fInfo := dp.FInfo{}
		fInfo.FName = v.Name()
		fInfo.DstBlkID = did
		fInfo.DstMount = dmount
		fPath := blkpath + "/" + v.Name()

		fd, err := os.Open(fPath)
		if err != nil {
			logger.Error("Open SrcBlkFile:%v failed:%v for Migrate", fPath, err)
			ack.Ret = -1
			return &ack, err
		}
		buf := make([]byte, 2*1024*1024)
		r := bufio.NewReader(fd)

		for {
			n, err := r.Read(buf)
			if err != nil && err != io.EOF {
				logger.Error("Read SrcBlkFile:%v failed:%v for Migrate", fPath, err)
				ack.Ret = -1
				fd.Close()
				return &ack, err
			}

			if n == 0 {
				break
			}
			fInfo.DataBuf = buf[:n]
			err = stream.Send(&fInfo)
			if err == io.EOF {
				logger.Debug("Send SrcBlkFile:%v to DstBlk success for Migrate because chunkdata IoEOF", fPath)
				continue
			}
			if err != nil {
				logger.Error("Send SrcBlkFile:%v to DstBlk failed:%v for Migrate", fPath, err)
				ack.Ret = -1
				fd.Close()
				return &ack, err
			}
		}
		fd.Close()
	}

	_, err = stream.CloseAndRecv()
	if err != nil {
		logger.Error("CloseAndRecv SrcBlkPath:%v to DstBlk failed:%v for Migrate", blkpath, err)
		ack.Ret = -1
		return &ack, err
	}

	ack.Ret = 0
	return &ack, nil
}

func (s *DataNodeServer) SendMigrateData(stream dp.DataNode_SendMigrateDataServer) error {
	for {
		finfo, err := stream.Recv()
		if err == io.EOF {
			return stream.SendAndClose(&dp.SendAck{Ret: 0})
		}
		if err != nil {
			logger.Error("Recv from SrcBlk for Migrate Blk err:%v", err)
			return err
		}

		path := finfo.DstMount + fmt.Sprintf("/block-%d", finfo.DstBlkID)
		if ok, err := utils.LocalPathExists(path); !ok && err == nil {
			os.MkdirAll(path, 0777)
		}

		chunkFileName := path + "/" + finfo.FName
		f, err := os.OpenFile(chunkFileName, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0660)
		defer f.Close()
		if err != nil {
			return err
		}
		w := bufio.NewWriter(f)
		_, err = w.Write(finfo.DataBuf)
		if err != nil {
			return err
		}
		err = w.Flush()
		if err != nil {
			return err
		}
		logger.Debug("Write Blk:%v One ChunkFile:%v for Migrate BLK Success!", finfo.DstBlkID, chunkFileName)
	}
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

// rpc NodeMonitor(NodeMonitorReq) returns (NodeMonitorAck){};
func (s *DataNodeServer) NodeMonitor(ctx context.Context, in *dp.NodeMonitorReq) (*dp.NodeMonitorAck, error) {
	ack := dp.NodeMonitorAck{NodeInfo: &dp.NodeInfo{}}

	cpuUsage, err := cpu.Percent(time.Millisecond*300, false)
	if err == nil {
		ack.NodeInfo.CpuUsage = cpuUsage[0]
	} else {
		logger.Error("NodeMonitor get cpu usage failed !")
	}

	cpuLoad, _ := load.Avg()
	ack.NodeInfo.CpuLoad = cpuLoad.Load1

	memv, _ := mem.VirtualMemory()
	ack.NodeInfo.TotalMem = memv.Total
	ack.NodeInfo.FreeMem = memv.Free
	ack.NodeInfo.MemUsedPercent = memv.UsedPercent

	diskUsage, _ := disk.Usage(DataNodeServerAddr.Path)
	ack.NodeInfo.PathUsedPercent = diskUsage.UsedPercent
	ack.NodeInfo.PathTotal = diskUsage.Total
	ack.NodeInfo.PathFree = diskUsage.Free

	disksIO, _ := disk.IOCounters()
	for _, v := range disksIO {
		diskio := dp.DiskIO{}
		diskio.IoTime = v.IoTime
		diskio.IopsInProgress = v.IopsInProgress
		diskio.Name = v.Name
		diskio.ReadBytes = diskio.ReadBytes
		diskio.ReadCount = diskio.ReadCount
		diskio.WeightedIO = diskio.WeightedIO
		diskio.WriteBytes = diskio.WriteBytes
		diskio.WriteCount = diskio.WriteCount
		ack.NodeInfo.DiskIOs = append(ack.NodeInfo.DiskIOs, &diskio)
	}

	NetsIO, _ := utilnet.IOCounters(true)
	for _, v := range NetsIO {
		netio := dp.NetIO{}
		netio.BytesRecv = v.BytesRecv
		netio.BytesSent = v.BytesSent
		netio.Dropin = v.Dropin
		netio.Dropout = v.Dropout
		netio.Errin = v.Errin
		netio.Errout = v.Errout
		netio.Name = v.Name
		netio.PacketsRecv = v.PacketsRecv
		netio.PacketsSent = v.PacketsSent
		ack.NodeInfo.NetIOs = append(ack.NodeInfo.NetIOs, &netio)
	}

	logger.Debug("NodeMonitor: %v", ack.NodeInfo)

	return &ack, nil
}

// GetLeader Get Cluster Metadata Leader Node
func GetLeader(volumeID string) (string, error) {
	var leader string
	var flag bool
	for _, ip := range MetaNodePeers {
		conn, err := grpc.Dial(ip, grpc.WithInsecure(), grpc.WithBlock(), grpc.WithTimeout(time.Millisecond*300), grpc.FailOnNonTempDialError(true))
		if err != nil {
			continue
		}
		defer conn.Close()
		mc := mp.NewMetaNodeClient(conn)
		pmGetMetaLeaderReq := &mp.GetMetaLeaderReq{
			VolID: volumeID,
		}
		ctx, _ := context.WithTimeout(context.Background(), 5*time.Second)
		pmGetMetaLeaderAck, err1 := mc.GetMetaLeader(ctx, pmGetMetaLeaderReq)
		if err1 != nil {
			continue
		}
		if pmGetMetaLeaderAck.Ret != 0 {
			continue
		}
		leader = pmGetMetaLeaderAck.Leader
		flag = true
		break
	}
	if !flag {
		return "", errors.New("Get leader failed")
	}
	return leader, nil

}

// DialMeta ...
func DialMeta(volumeID string) (*grpc.ClientConn, error) {
	var conn *grpc.ClientConn
	var err error

	MetaNodeAddr, _ = GetLeader("Cluster")
	conn, err = grpc.Dial(MetaNodeAddr, grpc.WithInsecure(), grpc.FailOnNonTempDialError(true))
	if err != nil {
		time.Sleep(500 * time.Millisecond)
		MetaNodeAddr, _ = GetLeader(volumeID)
		conn, err = grpc.Dial(MetaNodeAddr, grpc.WithInsecure(), grpc.FailOnNonTempDialError(true))
		if err != nil {
			time.Sleep(500 * time.Millisecond)
			MetaNodeAddr, _ = GetLeader(volumeID)
			conn, err = grpc.Dial(MetaNodeAddr, grpc.WithInsecure(), grpc.FailOnNonTempDialError(true))
		}
	}
	return conn, err
}

func init() {

	var loglevel string
	var port int

	flag.StringVar(&DataNodeServerAddr.IPStr, "host", "127.0.0.1", "ContainerFS DataNode Host")
	flag.IntVar(&port, "port", 8000, "ContainerFS DataNode Port")
	flag.StringVar(&DataNodeServerAddr.Tier, "tier", "sas", "ContainerFS DataNode Storage Medium")
	flag.StringVar(&DataNodeServerAddr.Path, "datapath", "", "ContainerFS DataNode Data Path")
	flag.StringVar(&DataNodeServerAddr.Log, "logpath", "/export/Logs/containerfs/logs/", "ContainerFS Log Path")
	flag.StringVar(&loglevel, "loglevel", "error", "ContainerFS Log Level")
	addr := flag.String("metanode", "127.0.0.1:9903,127.0.0.1:9913,127.0.0.1:9923", "ContainerFS metanode hosts")

	flag.Parse()
	MetaNodePeers = strings.Split(*addr, ",")

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

	_, err := os.Stat(DataNodeServerAddr.Path)
	if err != nil {
		logger.Error("data node statup failed : DataNodeServerAddr.Path not exist !")
		os.Exit(1)
	}

	if ok, _ := utils.LocalPathExists(DataNodeServerAddr.Flag); !ok {
		registryToMeta()
		logger.Debug("registry to metanode success")
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

	startDataService()
}
