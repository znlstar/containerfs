package main

import (
	"bufio"
	"errors"
	"flag"
	"fmt"
	"github.com/tigcode/containerfs/logger"
	"github.com/tigcode/containerfs/proto/dp"
	"github.com/tigcode/containerfs/proto/mp"
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
	"sync/atomic"
	"time"

	"github.com/shirou/gopsutil/cpu"
	"github.com/shirou/gopsutil/disk"
	"github.com/shirou/gopsutil/load"
	"github.com/shirou/gopsutil/mem"
	utilnet "github.com/shirou/gopsutil/net"
)

// Master Struct
type C2MReplServerStream struct {
	stream dp.DataNode_C2MReplServer
	sync.Mutex
	NeedBreak    bool
	BlockGroupID uint64
}
type M2SReplClientStream struct {
	s      *DataNodeServer
	stream dp.DataNode_M2SReplClient
	sync.Mutex
	BlockGroupID uint64
}

// Slave Struct
type M2SReplServerStream struct {
	stream       dp.DataNode_M2SReplServer
	NeedBreak    bool
	BlockGroupID uint64
}
type S2BReplClientStream struct {
	stream       dp.DataNode_S2BReplClient
	BlockGroupID uint64
}

// DataNodeServer ...
type DataNodeServer struct {
	ClientStreamID                 uint64
	C2MReplServerStreamCacheLocker sync.RWMutex
	C2MReplServerStreamCache       map[uint64]*C2MReplServerStream

	M2SReplClientStreamCacheLocker sync.RWMutex
	M2SReplClientStreamCache       map[uint64]*M2SReplClientStream
}

type addr struct {
	Host string
	Path string
	Flag string
	Log  string
	Tier string
}

var MetaNodePeers []string
var MetaNodeAddr string

// DataNodeServerAddr ...
var DataNodeServerAddr addr

func startDataService() {

	lis, err := net.Listen("tcp", DataNodeServerAddr.Host)
	if err != nil {
		panic(fmt.Sprintf("Failed to listen on:%v", DataNodeServerAddr.Host))
	}
	s := grpc.NewServer()
	dp.RegisterDataNodeServer(s, &DataNodeServer{M2SReplClientStreamCache: make(map[uint64]*M2SReplClientStream), C2MReplServerStreamCache: make(map[uint64]*C2MReplServerStream)})
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

	var datanodeRegistryReq mp.Datanode
	datanodeRegistryReq.Host = DataNodeServerAddr.Host
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

func (s *DataNodeServer) writeDisk(blockID uint64, chunkID uint64, databuf []byte) error {

	path := DataNodeServerAddr.Path + "/block-" + strconv.Itoa(int(blockID))
	if ok, err := utils.LocalPathExists(path); !ok && err == nil {
		os.MkdirAll(path, 0777)
	}

	chunkFileName := path + "/chunk-" + strconv.Itoa(int(chunkID))

	f, err := os.OpenFile(chunkFileName, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0660)
	if err != nil {
		logger.Error("Openfile:%v  error:%v ", chunkFileName, err)
		return err
	}
	defer f.Close()

	w := bufio.NewWriter(f)
	_, err = w.Write(databuf)
	if err != nil {
		return err
	}

	err = w.Flush()
	if err != nil {
		return err
	}

	return nil
}

// WriteChunk ...
func (s *DataNodeServer) WriteChunk(ctx context.Context, in *dp.WriteChunkReq) (*dp.WriteChunkAck, error) {

	ack := dp.WriteChunkAck{}
	ack.CommitID = in.CommitID
	if err := s.writeDisk(in.BlockID, in.ChunkID, in.Databuf); err != nil {
		ack.Ret = -1
	}
	return &ack, nil
}

// On Master
// C2MRepl ...
func (s *DataNodeServer) C2MRepl(stream dp.DataNode_C2MReplServer) error {

	clientStreamID := atomic.AddUint64(&s.ClientStreamID, 1)

	logger.Debug("C2MRepl init ,clientStreamID %v", clientStreamID)

	C2MReplServerStream := &C2MReplServerStream{stream: stream}
	s.C2MReplServerStreamCacheLocker.Lock()
	s.C2MReplServerStreamCache[clientStreamID] = C2MReplServerStream
	s.C2MReplServerStreamCacheLocker.Unlock()

	defer func() {
		logger.Debug("C2MRepl out ,clientStreamID %v", clientStreamID)
		s.C2MReplServerStreamCacheLocker.Lock()
		delete(s.C2MReplServerStreamCache, clientStreamID)
		s.C2MReplServerStreamCacheLocker.Unlock()
	}()

	for {
		in, err := C2MReplServerStream.stream.Recv()
		if err == io.EOF {
			logger.Debug("C2MRepl C2MStream Recv EOF")
			return nil
		}
		if err != nil {
			logger.Error("C2MRepl C2MStream Recv err %v", err)
			return err
		}
		if C2MReplServerStream.NeedBreak {
			return errors.New("C2MReplServerStream NeedBreak")
		}

		C2MReplServerStream.BlockGroupID = in.BlockGroupID

		// save to localdisk
		if err := s.writeDisk(in.Master.BlockID, in.ChunkID, in.Databuf); err != nil {
			logger.Error("C2MRepl writeDisk err %v", err)
			return err
		}

		in.StreamID = clientStreamID

		s.M2SReplClientStreamCacheLocker.RLock()
		M2SReplClientStream, ok := s.M2SReplClientStreamCache[in.BlockGroupID]
		s.M2SReplClientStreamCacheLocker.RUnlock()

		if ok && M2SReplClientStream != nil && M2SReplClientStream.stream != nil {

			logger.Debug("C2MRepl Get M2SReplClientStream ok ...")

			M2SReplClientStream.Lock()
			if err := M2SReplClientStream.stream.Send(in); err != nil {
				M2SReplClientStream.Unlock()

				M2SReplClientStream = s.CreateM2SReplClientStream(in.Slave.Host, in.BlockGroupID)
				if M2SReplClientStream == nil {
					return errors.New("CreateM2SRepl err")
				}

				M2SReplClientStream.Lock()
				if err := M2SReplClientStream.stream.Send(in); err != nil {
					M2SReplClientStream.Unlock()
					return err
				}
			}
			M2SReplClientStream.Unlock()

		} else {

			M2SReplClientStream = s.CreateM2SReplClientStream(in.Slave.Host, in.BlockGroupID)
			if M2SReplClientStream == nil {
				return errors.New("CreateM2SRepl err")
			}

			M2SReplClientStream.Lock()
			if err := M2SReplClientStream.stream.Send(in); err != nil {
				M2SReplClientStream.Unlock()
				return err
			}
			M2SReplClientStream.Unlock()

		}

	}
}

func (s *DataNodeServer) CreateM2SReplClientStream(slaveHost string, blockGroupID uint64) *M2SReplClientStream {

	s.M2SReplClientStreamCacheLocker.Lock()
	delete(s.M2SReplClientStreamCache, blockGroupID)
	s.M2SReplClientStreamCacheLocker.Unlock()

	M2Sconn, err := grpc.Dial(slaveHost, grpc.WithInsecure(), grpc.WithBlock(), grpc.FailOnNonTempDialError(true))
	if err != nil {
		logger.Error("CreateM2SStream failed Dial slave %v err %v", slaveHost, err)
		return nil
	}
	M2Sclient := dp.NewDataNodeClient(M2Sconn)
	stream, err := M2Sclient.M2SRepl(context.Background())
	if err != nil {
		logger.Error("CreateM2SStream failed rpc slave %v err %v", slaveHost, err)
		return nil
	}

	M2SReplClientStream := &M2SReplClientStream{stream: stream, s: s, BlockGroupID: blockGroupID}
	s.M2SReplClientStreamCacheLocker.Lock()
	s.M2SReplClientStreamCache[blockGroupID] = M2SReplClientStream
	s.M2SReplClientStreamCacheLocker.Unlock()

	go M2SReplClientStream.BackWard()

	logger.Debug("CreateM2SStream success ...")

	return M2SReplClientStream
}

func (mss *M2SReplClientStream) BackWard() {

	logger.Debug("Starting M2SRecv init ...")

	defer func() {

		mss.s.C2MReplServerStreamCacheLocker.Lock()
		for _, v := range mss.s.C2MReplServerStreamCache {
			if v.BlockGroupID == mss.BlockGroupID {
				v.NeedBreak = true
			}
		}
		mss.s.C2MReplServerStreamCacheLocker.Unlock()

	}()

	for {
		in, err := mss.stream.Recv()
		if err == io.EOF {
			logger.Debug("M2SRecv M2SStream.Recv EOF")
			break
		}
		if err != nil {
			logger.Error("M2SRecv M2SStream.Recv err %v", err)
			break
		}

		mss.s.C2MReplServerStreamCacheLocker.RLock()
		C2MReplServerStream, ok := mss.s.C2MReplServerStreamCache[in.StreamID]
		mss.s.C2MReplServerStreamCacheLocker.RUnlock()

		if ok && C2MReplServerStream != nil {
			logger.Error("M2SRecv get StreamID %v C2MReplServerStream success", in.StreamID)
			C2MReplServerStream.Lock()
			if err := C2MReplServerStream.stream.Send(in); err != nil {
				logger.Error("M2SRecv get StreamID %v C2MReplServerStream success but send err %v", in.StreamID, err)
				C2MReplServerStream.Unlock()
				C2MReplServerStream.NeedBreak = true
				return
			}
			C2MReplServerStream.Unlock()

		} else {
			logger.Error("M2SRecv get StreamID %v C2MReplServerStream err", in.StreamID)
		}

	}

}

func (s *DataNodeServer) M2SRepl(stream dp.DataNode_M2SReplServer) error {

	var S2BReplClientStream *S2BReplClientStream

	defer func() {
		if S2BReplClientStream != nil {
			S2BReplClientStream.stream.CloseSend()
			S2BReplClientStream = nil
		}
	}()

	M2SReplServerStream := &M2SReplServerStream{stream: stream}

	for {
		in, err := M2SReplServerStream.stream.Recv()
		if err == io.EOF {
			logger.Debug("M2SRepl  stream.Recv EOF")
			return nil
		}
		if err != nil {
			logger.Error("M2SRepl  stream.Recv err %v", err)
			return err
		}

		if M2SReplServerStream.NeedBreak {
			return errors.New("M2SReplServerStream NeedBreak")
		}

		if S2BReplClientStream == nil {
			S2BReplClientStream = s.CreateS2BStream(in.Backup.Host, M2SReplServerStream)
			if S2BReplClientStream == nil {
				return err
			}
		}

		// save to localdisk
		if err := s.writeDisk(in.Slave.BlockID, in.ChunkID, in.Databuf); err != nil {
			logger.Error("M2SRepl writeDisk failed %v", err)
			return err
		}

		if err := S2BReplClientStream.stream.Send(in); err != nil {
			logger.Error("M2SRepl S2BReplClientStream.Send failed %v", err)
			ack := dp.StreamWriteAck{CommitID: in.CommitID, DataLen: in.DataLen, ChunkID: in.ChunkID, BlockGroupID: in.BlockGroupID, StreamID: in.StreamID, Ret: -1}
			stream.Send(&ack)
			return err
		}

	}

}

func (s *DataNodeServer) CreateS2BStream(backUpHost string, M2SReplServerStream *M2SReplServerStream) *S2BReplClientStream {

	S2Bconn, err := grpc.Dial(backUpHost, grpc.WithInsecure(), grpc.WithBlock(), grpc.FailOnNonTempDialError(true))
	if err != nil {
		logger.Error("StreamM2SWrite first create S2Bconn S2BReplClientStream Dail to Backup failed %v", err)
		return nil
	}
	S2Bclient := dp.NewDataNodeClient(S2Bconn)

	S2BReplClientStream := &S2BReplClientStream{}
	S2BReplClientStream.stream, err = S2Bclient.S2BRepl(context.Background())
	if err != nil {
		logger.Error("StreamM2SWrite first create S2BReplClientStream S2BRepl rpc to Backup failed %v", err)
		return nil
	}

	go S2BReplClientStream.BackWard(M2SReplServerStream)
	return S2BReplClientStream
}

func (sbs *S2BReplClientStream) BackWard(M2SReplServerStream *M2SReplServerStream) {

	for {
		in, err := sbs.stream.Recv()
		if err == io.EOF {
			logger.Debug("S2BReplClientStream BackWard  S2BReplClientStream.Recv EOF")
			M2SReplServerStream.NeedBreak = true
			break
		}
		if err != nil {
			logger.Error("S2BReplClientStream BackWard  S2BReplClientStream.Recv err %v", err)
			M2SReplServerStream.NeedBreak = true
			break
		}
		if err := M2SReplServerStream.stream.Send(in); err != nil {
			logger.Error("S2BReplClientStream BackWard  M2SReplServerStream.Send err %v", err)
			M2SReplServerStream.NeedBreak = true
			break
		}

	}

}

// On BackUP

// S2BRepl ...

func (s *DataNodeServer) S2BRepl(stream dp.DataNode_S2BReplServer) error {

	for {
		in, err := stream.Recv()
		if err == io.EOF {
			logger.Debug("S2BRepl  stream.Recv EOF")
			return nil
		}
		if err != nil {
			logger.Debug("S2BRepl  stream.Recv err %v", err)
			return err
		}

		ack := dp.StreamWriteAck{CommitID: in.CommitID, DataLen: in.DataLen, ChunkID: in.ChunkID, BlockGroupID: in.BlockGroupID, StreamID: in.StreamID, Ret: 0}
		// save to localdisk
		if err := s.writeDisk(in.Backup.BlockID, in.ChunkID, in.Databuf); err != nil {
			logger.Error("S2BRepl  writeDisk err %v", err)
			ack.Ret = -1
		}

		if err := stream.Send(&ack); err != nil {
			logger.Error("S2BRepl  stream.Send err %v", err)
			return err
		}

	}

}

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
	dhost := in.DstHost
	dmount := in.DstMount

	conn, err := grpc.Dial(dhost, grpc.WithInsecure())
	if err != nil {
		logger.Error("Migrate failed : Dial to DestDataNode:%v failed:%v !", dhost, err)
		ack.Ret = -1
		return &ack, err
	}
	defer conn.Close()
	dc := dp.NewDataNodeClient(conn)

	ctxtmp, _ := context.WithTimeout(context.Background(), 5*time.Minute)

	stream, err := dc.SendMigrateData(ctxtmp)
	if err != nil {
		logger.Error("SendMigrate to DestDataNode:%v err:%v", dhost, err)
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

	flag.StringVar(&DataNodeServerAddr.Host, "host", "127.0.0.1:8001", "ContainerFS DataNode Host")
	flag.IntVar(&port, "port", 8000, "ContainerFS DataNode Port")
	flag.StringVar(&DataNodeServerAddr.Tier, "tier", "sas", "ContainerFS DataNode Storage Medium")
	flag.StringVar(&DataNodeServerAddr.Path, "datapath", "", "ContainerFS DataNode Data Path")
	flag.StringVar(&DataNodeServerAddr.Log, "logpath", "/export/Logs/containerfs/logs/", "ContainerFS Log Path")
	flag.StringVar(&loglevel, "loglevel", "error", "ContainerFS Log Level")
	addr := flag.String("metanode", "127.0.0.1:9903,127.0.0.1:9913,127.0.0.1:9923", "ContainerFS metanode hosts")

	flag.Parse()
	MetaNodePeers = strings.Split(*addr, ",")

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
