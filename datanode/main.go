package main

import (
	"bufio"
	"errors"
	"flag"
	"fmt"
	"github.com/tiglabs/containerfs/logger"
	"github.com/tiglabs/containerfs/proto/dp"
	"github.com/tiglabs/containerfs/proto/mp"
	"github.com/tiglabs/containerfs/utils"
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
	NeedBreak     bool
	ReplicaStream *M2SReplClientStream
}
type M2SReplClientStream struct {
	s      *DataNodeServer
	conn   *grpc.ClientConn
	stream dp.DataNode_M2SReplClient
	sync.Mutex
	BlockGroupID uint64
	refCnt       int32
}

// Slave Struct
type M2SReplServerStream struct {
	stream              dp.DataNode_M2SReplServer
	NeedBreak           bool
	BlockGroupID        uint64
	BackupHost          string
	S2BReplClientStream *S2BReplClientStream
}
type S2BReplClientStream struct {
	conn         *grpc.ClientConn
	stream       dp.DataNode_S2BReplClient
	BlockGroupID uint64
}

// DataNodeServer ...
type DataNodeServer struct {
	ClientStreamID        uint64
	ClientStreamMapLocker sync.RWMutex
	ClientStreamMap       map[uint64]*C2MReplServerStream

	ReplicaStreamCacheLocker sync.RWMutex
	ReplicaStreamCache       map[uint64]*M2SReplClientStream
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
	dp.RegisterDataNodeServer(s, &DataNodeServer{ReplicaStreamCache: make(map[uint64]*M2SReplClientStream), ClientStreamMap: make(map[uint64]*C2MReplServerStream)})
	reflection.Register(s)
	if err := s.Serve(lis); err != nil {
		panic(fmt.Sprintf("Failed to start Serve on:%v", DataNodeServerAddr.Host))
	}
}

func registryToMeta() {
	conn, err := DialMeta("Cluster")
	if err != nil {
		logger.Error("DataNode[%v]: registryToMeta: Dail Meta Failed err:%v", DataNodeServerAddr.Host, err)
		os.Exit(1)
	}
	defer conn.Close()
	mc := mp.NewMetaNodeClient(conn)

	var datanodeRegistryReq mp.DataNode
	datanodeRegistryReq.Host = DataNodeServerAddr.Host
	diskInfo := utils.DiskUsage(DataNodeServerAddr.Path)
	datanodeRegistryReq.Capacity = int32(float64(diskInfo.All) / float64(1024*1024*1024))
	datanodeRegistryReq.Free = int32(float64(diskInfo.Free) / float64(1024*1024*1024))
	datanodeRegistryReq.Used = int32(float64(diskInfo.Used) / float64(1024*1024*1024))
	datanodeRegistryReq.MountPoint = DataNodeServerAddr.Path
	datanodeRegistryReq.Tier = DataNodeServerAddr.Tier
	datanodeRegistryReq.Status = 0

	ack, err := mc.DataNodeRegistry(context.Background(), &datanodeRegistryReq)
	if err != nil {
		logger.Error("DataNode[%v]: register to MetaNode failed! err %v", DataNodeServerAddr.Host, err)
		os.Exit(1)
	}
	if ack.Ret == 0 {
		logger.Debug("DataNode[%v]: register to MetaNode success!", DataNodeServerAddr.Host)
		os.Create(DataNodeServerAddr.Flag)
	} else {
		logger.Error("DataNode[%v]: register to MetaNode failed! ret %v", DataNodeServerAddr.Host, ack.Ret)
		os.Exit(1)
	}

	return
}

// DatanodeHealthCheck rpc GetChunks(GetChunksReq) returns (GetChunksAck){};
func (s *DataNodeServer) DataNodeHealthCheck(ctx context.Context, in *dp.DataNodeHealthCheckReq) (*dp.DataNodeHealthCheckAck, error) {
	ack := dp.DataNodeHealthCheckAck{}
	f, err := os.OpenFile(DataNodeServerAddr.Path+"/health", os.O_RDWR|os.O_TRUNC|os.O_CREATE, 0666)
	if err != nil {
		logger.Error("DataNode[%v]: Open datanode check health file error:%v", DataNodeServerAddr.Host, err)
		ack.Status = 2
	} else {
		_, err = f.WriteString("ok")
		if err != nil {
			logger.Error("DataNode[%v]: Write datanode check health file error:%v", DataNodeServerAddr.Host, err)
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
		logger.Error("DataNode[%v]: Open file %v error:%v ", DataNodeServerAddr.Host, chunkFileName, err)
		return err
	}
	defer f.Close()

	w := bufio.NewWriter(f)
	_, err = w.Write(databuf)
	if err != nil {
		logger.Error("DataNode[%v]: Write to file %v error:%v ", DataNodeServerAddr.Host, chunkFileName, err)
		return err
	}

	err = w.Flush()
	if err != nil {
		logger.Error("DataNode[%v]: Flush file %v error:%v ", DataNodeServerAddr.Host, chunkFileName, err)
		return err
	}

	return nil
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

	logger.Debug("DataNode[%v]: write file %v with offset %v and len %v", DataNodeServerAddr.Host, chunkFileName, chunkOffset, len(in.Databuf))

	f, err = os.OpenFile(chunkFileName, os.O_RDWR|os.O_CREATE, 0660)
	defer f.Close()
	if err != nil {
		logger.Error("DataNode[%v]: Open file %v error:%v ", DataNodeServerAddr.Host, chunkFileName, err)
		ack.Ret = -1
		return &ack, nil
	}

	sret, err = f.Seek(chunkOffset, 0)
	if sret != chunkOffset || err != nil {
		logger.Error("DataNode[%v]: %v Seek to:%v ret:%v error:%v ", DataNodeServerAddr.Host, chunkFileName, chunkOffset, sret, err)
		ack.Ret = -1
		return &ack, nil
	}

	ret, err = f.Write(in.Databuf)
	if ret != len(in.Databuf) || err != nil {
		logger.Error("DataNode[%v]: %v Write len:%v ret:%v error:%v ", DataNodeServerAddr.Host, chunkFileName, len(in.Databuf), ret, err)
		ack.Ret = -1
		return &ack, nil
	}

	ack.Ret = 0
	return &ack, nil
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

func (s *DataNodeServer) getReplicaStream(blockGroupID uint64) *M2SReplClientStream {
	s.ReplicaStreamCacheLocker.RLock()
	defer s.ReplicaStreamCacheLocker.RUnlock()

	ReplicaStream, ok := s.ReplicaStreamCache[blockGroupID]
	if !ok || ReplicaStream == nil {
		return nil
	}

	if ok && ReplicaStream != nil && ReplicaStream.stream == nil {
		logger.Error("DataNode[%v]: get ReplicaStream by BGID-%v but without stream!! delete it from Cache", DataNodeServerAddr.Host, blockGroupID)
		return nil
	}

	atomic.AddInt32(&ReplicaStream.refCnt, 1)
	return ReplicaStream
}

func (s *DataNodeServer) putReplicaStream(ReplicaStream *M2SReplClientStream) {
	refCnt := atomic.AddInt32(&ReplicaStream.refCnt, -1)
	if refCnt > 0 {
		return
	}

	s.ReplicaStreamCacheLocker.RLock()
	tmpStream, ok := s.ReplicaStreamCache[ReplicaStream.BlockGroupID]
	s.ReplicaStreamCacheLocker.RUnlock()

	if ok && tmpStream == ReplicaStream {
		return
	}

	//the last user come to free ReplicaStream
	s.closeReplicaStream(ReplicaStream)
}

func (s *DataNodeServer) closeReplicaStream(ReplicaStream *M2SReplClientStream) {
	ReplicaStream.stream.CloseSend()
	ReplicaStream.conn.Close()
}

func (s *DataNodeServer) addReplicaStream(ReplicaStream *M2SReplClientStream) {
	s.ReplicaStreamCacheLocker.Lock()
	defer s.ReplicaStreamCacheLocker.Unlock()

	tmpStream, ok := s.ReplicaStreamCache[ReplicaStream.BlockGroupID]
	if ok && tmpStream != nil {
		delete(s.ReplicaStreamCache, ReplicaStream.BlockGroupID)
		refCnt := atomic.LoadInt32(&ReplicaStream.refCnt)
		if refCnt <= 0 {
			s.closeReplicaStream(ReplicaStream)
		}
	}

	s.ReplicaStreamCache[ReplicaStream.BlockGroupID] = ReplicaStream
	atomic.StoreInt32(&ReplicaStream.refCnt, 1)
}

func (s *DataNodeServer) C2MReplExit(clientStreamID uint64) {
	logger.Debug("DataNode[%v]: ClientID-%d: C2MRepl exit", DataNodeServerAddr.Host, clientStreamID)
	s.ClientStreamMapLocker.Lock()
	C2MReplServerStream, _ := s.ClientStreamMap[clientStreamID]
	delete(s.ClientStreamMap, clientStreamID)
	s.ClientStreamMapLocker.Unlock()

	if C2MReplServerStream != nil && C2MReplServerStream.ReplicaStream != nil {
		s.putReplicaStream(C2MReplServerStream.ReplicaStream)
	}
}

// C2MRepl ...
func (s *DataNodeServer) C2MRepl(stream dp.DataNode_C2MReplServer) error {

	clientStreamID := atomic.AddUint64(&s.ClientStreamID, 1)

	logger.Debug("DataNode[%v]: ClientID-%d: C2MRepl init for new client", DataNodeServerAddr.Host, clientStreamID)

	C2MReplServerStream := &C2MReplServerStream{stream: stream}
	s.ClientStreamMapLocker.Lock()
	s.ClientStreamMap[clientStreamID] = C2MReplServerStream
	s.ClientStreamMapLocker.Unlock()

	defer s.C2MReplExit(clientStreamID)

	for {
		in, err := C2MReplServerStream.stream.Recv()
		if err == io.EOF {
			logger.Debug("DataNode[%v]: ClientID-%d: C2MRepl Recv EOF", DataNodeServerAddr.Host, clientStreamID)
			return nil
		}
		if err != nil {
			logger.Debug("DataNode[%v]: ClientID-%d: C2MRepl Recv err %v", DataNodeServerAddr.Host, clientStreamID, err)
			return err
		}
		if C2MReplServerStream.NeedBreak {
			logger.Error("DataNode[%v]: ClientID-%d: C2MRepl NeedBreak", DataNodeServerAddr.Host, clientStreamID)
			return errors.New("C2MReplServerStream NeedBreak")
		}

		// save to localdisk
		if err := s.writeDisk(in.Master.BlockID, in.ChunkID, in.Databuf); err != nil {
			logger.Error("DataNode[%v]: ClientID-%d: C2MRepl writeDisk err %v", DataNodeServerAddr.Host, clientStreamID, err)
			return err
		}

		in.StreamID = clientStreamID

		if C2MReplServerStream.ReplicaStream != nil && C2MReplServerStream.ReplicaStream.BlockGroupID != in.BlockGroupID {
			//change block group...should not have pending request on last block group
			s.putReplicaStream(C2MReplServerStream.ReplicaStream)
			C2MReplServerStream.ReplicaStream = nil
		}
		M2SReplClientStream := C2MReplServerStream.ReplicaStream

		if M2SReplClientStream == nil {
			M2SReplClientStream = s.getReplicaStream(in.BlockGroupID)
		}

		if M2SReplClientStream != nil {

			M2SReplClientStream.Lock()
			err := M2SReplClientStream.stream.Send(in)
			M2SReplClientStream.Unlock()

			if err == nil {
				logger.Debug("DataNode[%v]: ClientID-%d: C2MRepl Get M2SRepl Stream by BGID-%v and Send ok", DataNodeServerAddr.Host, clientStreamID, in.BlockGroupID)
				continue
			}

			//should we wait for other pending requests?

			s.putReplicaStream(M2SReplClientStream)
			C2MReplServerStream.ReplicaStream = nil
			logger.Error("DataNode[%v]: ClientID-%d: C2MRepl Get M2SRepl Stream by BGID-%v but Send err %v, go create new Stream", DataNodeServerAddr.Host, clientStreamID, in.BlockGroupID, err)
		}

		M2SReplClientStream = s.CreateM2SReplClientStream(in.Slave.Host, in.BlockGroupID)
		if M2SReplClientStream == nil {
			logger.Error("DataNode[%v]: ClientID-%d:  Create M2SRepl err", DataNodeServerAddr.Host, clientStreamID)
			return errors.New("Create M2SRepl err")
		}
		C2MReplServerStream.ReplicaStream = M2SReplClientStream

		M2SReplClientStream.Lock()
		err = M2SReplClientStream.stream.Send(in)
		M2SReplClientStream.Unlock()

		if err != nil {
			logger.Error("DataNode[%v]: ClientID-%d:  M2SRepl Stream Send err %v", DataNodeServerAddr.Host, clientStreamID, err)
			return err
		}
	}
}

func (s *DataNodeServer) CreateM2SReplClientStream(slaveHost string, blockGroupID uint64) *M2SReplClientStream {

	M2Sconn, err := grpc.Dial(slaveHost, grpc.WithInsecure(), grpc.WithBlock(), grpc.WithTimeout(time.Millisecond*300), grpc.FailOnNonTempDialError(true))
	if err != nil {
		logger.Error("DataNode[%v]: Create M2SStream: Dial to slave %v err %v", DataNodeServerAddr.Host, slaveHost, err)
		return nil
	}
	M2Sclient := dp.NewDataNodeClient(M2Sconn)
	stream, err := M2Sclient.M2SRepl(context.Background())
	if err != nil {
		logger.Error("DataNode[%v]: Create M2SStream to slave %v err %v", DataNodeServerAddr.Host, slaveHost, err)
		return nil
	}

	M2SReplClientStream := &M2SReplClientStream{conn: M2Sconn, stream: stream, s: s, BlockGroupID: blockGroupID}
	s.addReplicaStream(M2SReplClientStream)

	go M2SReplClientStream.BackWard()

	logger.Debug("DataNode[%v]: Create M2SStream success ...", DataNodeServerAddr.Host)

	return M2SReplClientStream
}

func (mss *M2SReplClientStream) BackWardExit() {
	mss.s.ClientStreamMapLocker.Lock()
	for _, v := range mss.s.ClientStreamMap {
		if v.ReplicaStream == mss {
			//maybe we shold send err to Client here.
			v.NeedBreak = true
		}
	}
	mss.s.ClientStreamMapLocker.Unlock()

	logger.Debug("DataNode[%v]: M2Sstream-%p: BGID-%v M2SRecv exit", DataNodeServerAddr.Host, mss, mss.BlockGroupID)
}

func (mss *M2SReplClientStream) BackWard() {

	logger.Debug("DataNode[%v]: BackWard M2Sstream-%p: BGID-%v M2SRecv init", DataNodeServerAddr.Host, mss, mss.BlockGroupID)

	defer mss.BackWardExit()

	for {
		in, err := mss.stream.Recv()
		if err == io.EOF {
			logger.Error("DataNode[%v]: BackWard M2Sstream-%p: BGID-%v M2SRecv EOF", DataNodeServerAddr.Host, mss, mss.BlockGroupID)
			break
		}
		if err != nil {
			logger.Error("DataNode[%v]: BackWard M2Sstream-%p: BGID-%v M2SRecv err %v", DataNodeServerAddr.Host, mss, mss.BlockGroupID, err)
			break
		}

		mss.s.ClientStreamMapLocker.RLock()
		C2MReplServerStream, ok := mss.s.ClientStreamMap[in.StreamID]
		mss.s.ClientStreamMapLocker.RUnlock()

		if ok && C2MReplServerStream != nil {
			logger.Debug("DataNode[%v]: BackWard M2Sstream-%p: BGID-%v M2SRecv get ClientStream success with ClientID-%v", DataNodeServerAddr.Host, mss, mss.BlockGroupID, in.StreamID)

			C2MReplServerStream.Lock()
			if err := C2MReplServerStream.stream.Send(in); err != nil {
				logger.Error("DataNode[%v]: BackWard M2Sstream-%p: BGID-%v M2SRecv send for ClientID-%v err %v", DataNodeServerAddr.Host, mss, mss.BlockGroupID, in.StreamID, err)
				C2MReplServerStream.NeedBreak = true
			}
			C2MReplServerStream.Unlock()
		} else {
			logger.Error("DataNode[%v]: BackWard M2Sstream-%p: BGID-%v M2SRecv get ClientStream failed with ClientID-%v!", DataNodeServerAddr.Host, mss, mss.BlockGroupID, in.StreamID)
		}

	}

}

func (s *DataNodeServer) M2SRepl(stream dp.DataNode_M2SReplServer) error {

	M2SReplServerStream := &M2SReplServerStream{stream: stream}
	logger.Debug("DataNode[%v]: M2SRepl init for stream %p", DataNodeServerAddr.Host, M2SReplServerStream)

	defer func() {
		if M2SReplServerStream.S2BReplClientStream != nil {
			M2SReplServerStream.S2BReplClientStream.stream.CloseSend()
			M2SReplServerStream.S2BReplClientStream.conn.Close()
			M2SReplServerStream.S2BReplClientStream = nil
		}
		logger.Debug("DataNode[%v]: M2SRepl exit for stream %p BackupHost %v BGID-%v",
			DataNodeServerAddr.Host, M2SReplServerStream, M2SReplServerStream.BackupHost, M2SReplServerStream.BlockGroupID)
	}()

	for {
		in, err := M2SReplServerStream.stream.Recv()
		if err == io.EOF {
			logger.Debug("DataNode[%v]: M2SRepl BGID-%v stream.Recv EOF", DataNodeServerAddr.Host, M2SReplServerStream.BlockGroupID)
			return nil
		}
		if err != nil {
			logger.Error("DataNode[%v]: M2SRepl BGID-%v  stream.Recv err %v", DataNodeServerAddr.Host, M2SReplServerStream.BlockGroupID, err)
			return err
		}

		if M2SReplServerStream.NeedBreak {
			logger.Error("DataNode[%v]: M2SRepl BGID-%v NeedBreak", DataNodeServerAddr.Host, M2SReplServerStream.BlockGroupID)
			return errors.New("M2SReplServerStream NeedBreak")
		}

		if M2SReplServerStream.S2BReplClientStream == nil {
			M2SReplServerStream.BlockGroupID = in.BlockGroupID
			M2SReplServerStream.BackupHost = in.Backup.Host

			M2SReplServerStream.S2BReplClientStream = s.CreateS2BStream(in.Backup.Host, M2SReplServerStream)
			if M2SReplServerStream.S2BReplClientStream == nil {
				logger.Error("DataNode[%v]: M2SRepl BGID-%v CreateS2BStream err", DataNodeServerAddr.Host, M2SReplServerStream.BlockGroupID)
				return errors.New("CreateS2BStream err")
			}
		}

		if M2SReplServerStream.BackupHost != in.Backup.Host || M2SReplServerStream.BlockGroupID != in.BlockGroupID {
			logger.Error("DataNode[%v]: M2SRepl  BlockGroup error: stream(BHOST-%v, BGID-%v) != in(BHOST-%v, BGID-%v)",
				DataNodeServerAddr.Host, M2SReplServerStream.BackupHost, M2SReplServerStream.BlockGroupID, in.Backup.Host, in.BlockGroupID)
			return errors.New("BlockGroup err")
		}

		// save to localdisk
		if err := s.writeDisk(in.Slave.BlockID, in.ChunkID, in.Databuf); err != nil {
			logger.Error("DataNode[%v]: BGID-%v M2SRepl writeDisk failed %v", DataNodeServerAddr.Host, M2SReplServerStream.BlockGroupID, err)
			return err
		}

		if err := M2SReplServerStream.S2BReplClientStream.stream.Send(in); err != nil {
			logger.Error("DataNode[%v]: BGID-%v M2SRepl S2BReplClientStream.Send failed %v", DataNodeServerAddr.Host, M2SReplServerStream.BlockGroupID, err)
			ack := dp.StreamWriteAck{CommitID: in.CommitID, DataLen: in.DataLen, ChunkID: in.ChunkID, BlockGroupID: in.BlockGroupID, StreamID: in.StreamID, Ret: -1}
			stream.Send(&ack)
			return err
		}

	}

}

func (s *DataNodeServer) CreateS2BStream(backUpHost string, M2SReplServerStream *M2SReplServerStream) *S2BReplClientStream {

	S2Bconn, err := grpc.Dial(backUpHost, grpc.WithInsecure(), grpc.WithBlock(), grpc.WithTimeout(time.Millisecond*300), grpc.FailOnNonTempDialError(true))
	if err != nil {
		logger.Error("DataNode[%v]: CreateS2BStream Dail to Backup %v failed %v", DataNodeServerAddr.Host, backUpHost, err)
		return nil
	}
	S2Bclient := dp.NewDataNodeClient(S2Bconn)

	stream, err := S2Bclient.S2BRepl(context.Background())
	if err != nil {
		logger.Error("DataNode[%v]: CreateS2BStream rpc to Backup failed %v", DataNodeServerAddr.Host, err)
		return nil
	}
	sbs := &S2BReplClientStream{stream: stream, conn: S2Bconn, BlockGroupID: M2SReplServerStream.BlockGroupID}

	go sbs.BackWard(M2SReplServerStream)
	return sbs
}

func (sbs *S2BReplClientStream) BackWard(M2SReplServerStream *M2SReplServerStream) {

	for {
		in, err := sbs.stream.Recv()
		if err == io.EOF {
			logger.Debug("DataNode[%v]: BGID-%v BackWard  S2BReplClientStream.Recv EOF", DataNodeServerAddr.Host, sbs.BlockGroupID)
			M2SReplServerStream.NeedBreak = true
			break
		}
		if err != nil {
			logger.Error("DataNode[%v]: BGID-%v BackWard  S2BReplClientStream.Recv err %v", DataNodeServerAddr.Host, sbs.BlockGroupID, err)
			M2SReplServerStream.NeedBreak = true
			break
		}
		if err := M2SReplServerStream.stream.Send(in); err != nil {
			logger.Error("DataNode[%v]: BGID-%v BackWard  M2SReplServerStream.Send err %v", DataNodeServerAddr.Host, sbs.BlockGroupID, err)
			M2SReplServerStream.NeedBreak = true
			break
		}

	}

}

// On BackUP

// S2BRepl ...
func (s *DataNodeServer) S2BRepl(stream dp.DataNode_S2BReplServer) error {

	BlockGroupID := uint64(0)

	for {
		in, err := stream.Recv()
		if err == io.EOF {
			logger.Debug("DataNode[%v]: S2BRepl BGID-%v stream.Recv EOF", DataNodeServerAddr.Host, BlockGroupID)
			return nil
		}
		if err != nil {
			logger.Debug("DataNode[%v]: S2BRepl BGID-%v stream.Recv err %v", DataNodeServerAddr.Host, BlockGroupID, err)
			return err
		}

		ack := dp.StreamWriteAck{CommitID: in.CommitID, DataLen: in.DataLen, ChunkID: in.ChunkID, BlockGroupID: in.BlockGroupID, StreamID: in.StreamID, Ret: 0}
		// save to localdisk
		if err := s.writeDisk(in.Backup.BlockID, in.ChunkID, in.Databuf); err != nil {
			logger.Error("DataNode[%v]: S2BRepl BGID-%v  writeDisk err %v", DataNodeServerAddr.Host, BlockGroupID, err)
			ack.Ret = -1
		}

		if err := stream.Send(&ack); err != nil {
			logger.Error("DataNode[%v]: S2BRepl BGID-%v stream.Send err %v", DataNodeServerAddr.Host, BlockGroupID, err)
			return err
		}

	}

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
		logger.Error("DataNode[%v]: %v Seek to:%v ret:%v error:%v ", DataNodeServerAddr.Host, chunkFileName, offset, sret, err)
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
			logger.Error("DataNode[%v]: read chunkfile:%v error:%v", DataNodeServerAddr.Host, chunkFileName, err)
			return err
		}

		if n == 0 {
			logger.Debug("DataNode[%v]: read chunkfile:%v endsize:%v", DataNodeServerAddr.Host, chunkFileName, totalsize)
			break
		}

		totalsize += int64(n)

		if totalsize >= readsize {

			n = n - int(totalsize-readsize)
			ack.Databuf = buf[:n]
			if err := stream.Send(&ack); err != nil {
				logger.Error("DataNode[%v]: Send stream data to fuse error:%v", DataNodeServerAddr.Host, err)
				return err
			}
			break

		} else {
			ack.Databuf = buf[:n]
			if err := stream.Send(&ack); err != nil {
				logger.Error("DataNode[%v]: Send stream data to fuse error:%v", DataNodeServerAddr.Host, err)
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
		logger.Error("DataNode[%v]: Migrate failed : Dial to DestDataNode:%v failed:%v !", DataNodeServerAddr.Host, dhost, err)
		ack.Ret = -1
		return &ack, err
	}
	defer conn.Close()
	dc := dp.NewDataNodeClient(conn)

	ctxtmp, _ := context.WithTimeout(context.Background(), 5*time.Minute)

	stream, err := dc.SendMigrateData(ctxtmp)
	if err != nil {
		logger.Error("DataNode[%v]: SendMigrate to DestDataNode:%v err:%v", DataNodeServerAddr.Host, dhost, err)
		ack.Ret = -1
		return &ack, err
	}

	blkpath := smount + fmt.Sprintf("/block-%d", sid)
	if ok, err := utils.LocalPathExists(blkpath); !ok && err == nil {
		logger.Debug("DataNode[%v]: The Block:%v no chunkdata, so dont need copydata for Migrate", DataNodeServerAddr.Host, sid)
		ack.Ret = 0
		return &ack, nil
	}
	dirs, err := ioutil.ReadDir(blkpath)
	if err != nil {
		logger.Error("DataNode[%v]: List SrcBlk:%v failed:%v for Migrate", DataNodeServerAddr.Host, blkpath, err)
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
			logger.Error("DataNode[%v]: Open SrcBlkFile:%v failed:%v for Migrate", DataNodeServerAddr.Host, fPath, err)
			ack.Ret = -1
			return &ack, err
		}
		buf := make([]byte, 2*1024*1024)
		r := bufio.NewReader(fd)

		for {
			n, err := r.Read(buf)
			if err != nil && err != io.EOF {
				logger.Error("DataNode[%v]: Read SrcBlkFile:%v failed:%v for Migrate", DataNodeServerAddr.Host, fPath, err)
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
				logger.Debug("DataNode[%v]: Send SrcBlkFile:%v to DstBlk success for Migrate because chunkdata IoEOF", DataNodeServerAddr.Host, fPath)
				continue
			}
			if err != nil {
				logger.Error("DataNode[%v]: Send SrcBlkFile:%v to DstBlk failed:%v for Migrate", DataNodeServerAddr.Host, fPath, err)
				ack.Ret = -1
				fd.Close()
				return &ack, err
			}
		}
		fd.Close()
	}

	_, err = stream.CloseAndRecv()
	if err != nil {
		logger.Error("DataNode[%v]: CloseAndRecv SrcBlkPath:%v to DstBlk failed:%v for Migrate", blkpath, err)
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
			logger.Error("DataNode[%v]: Recv from SrcBlk for Migrate Blk err:%v", DataNodeServerAddr.Host, err)
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
		logger.Debug("DataNode[%v]: Write Blk:%v One ChunkFile:%v for Migrate BLK Success!", DataNodeServerAddr.Host, finfo.DstBlkID, chunkFileName)
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
		logger.Error("DataNode[%v]: NodeMonitor get cpu usage failed !", DataNodeServerAddr.Host)
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
		diskio.ReadBytes = v.ReadBytes
		diskio.ReadCount = v.ReadCount
		diskio.WeightedIO = v.WeightedIO
		diskio.WriteBytes = v.WriteBytes
		diskio.WriteCount = v.WriteCount
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

	logger.Debug("DataNode[%v]: NodeMonitor: %v", DataNodeServerAddr.Host, ack.NodeInfo)

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
