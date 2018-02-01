package main

import (
	"bufio"
	"errors"
	"flag"
	"fmt"
	"github.com/tiglabs/containerfs/logger"
	"github.com/tiglabs/containerfs/proto/dp"
	"github.com/tiglabs/containerfs/proto/vp"
	"github.com/tiglabs/containerfs/utils"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	//"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/reflection"
	"io"
	"io/ioutil"
	"net"
	"os"
	"path"
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

var VolMgrHosts []string

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
	isErr        bool
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
		panic(fmt.Sprintf("Failed to start Serve on:%v", DataNodeServerAddr.Host))
	}
}

func registryToVolMgr() {
	_, conn, err := utils.DialVolMgr(VolMgrHosts)
	if err != nil {
		logger.Error("DataNode[%v]: registryToVolMgr: Dail VolMgr Failed err:%v", DataNodeServerAddr.Host, err)
		os.Exit(1)
	}
	defer conn.Close()
	vc := vp.NewVolMgrClient(conn)

	var datanodeRegistryReq vp.DataNode
	datanodeRegistryReq.Host = DataNodeServerAddr.Host
	diskInfo := utils.DiskUsage(DataNodeServerAddr.Path)
	datanodeRegistryReq.Capacity = int32(float64(diskInfo.All) / float64(1024*1024*1024))
	datanodeRegistryReq.Free = int32(float64(diskInfo.Free) / float64(1024*1024*1024))
	datanodeRegistryReq.Used = int32(float64(diskInfo.Used) / float64(1024*1024*1024))
	datanodeRegistryReq.MountPoint = DataNodeServerAddr.Path
	datanodeRegistryReq.Tier = DataNodeServerAddr.Tier
	datanodeRegistryReq.Status = 0

	ack, err := vc.DataNodeRegistry(context.Background(), &datanodeRegistryReq)
	if err != nil {
		logger.Error("DataNode[%v]: register to VolMgr failed! err %v", DataNodeServerAddr.Host, err)
		os.Exit(1)
	}
	if ack.Ret == 0 {
		logger.Debug("DataNode[%v]: register to VolMgr success!", DataNodeServerAddr.Host)
	} else if ack.Ret == 3 {
		logger.Debug("DataNode[%v]: register to VolMgr success, already register!", DataNodeServerAddr.Host)
	} else {
		logger.Error("DataNode[%v]: register to VolMgr failed! ret %v", DataNodeServerAddr.Host, ack.Ret)
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

func (s *DataNodeServer) writeDisk(blockGroupID uint64, chunkID uint64, databuf []byte) error {

	bpath := path.Join(DataNodeServerAddr.Path, "blockgroup-"+strconv.FormatUint(blockGroupID, 10))
	if ok, err := utils.LocalPathExists(bpath); !ok && err == nil {
		os.MkdirAll(bpath, 0777)
	}

	chunkFileName := path.Join(bpath, "chunk-"+strconv.FormatUint(chunkID, 10))

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

	bpath := path.Join(DataNodeServerAddr.Path, "blockgroup-"+strconv.FormatUint(in.BlockGroupID, 10))
	if ok, err := utils.LocalPathExists(bpath); !ok && err == nil {
		os.MkdirAll(bpath, 0777)
	}

	chunkFileName := path.Join(bpath, "chunk-"+strconv.FormatUint(in.ChunkID, 10))

	logger.Debug("DataNode[%v]: write file %v with offset %v and len %v", DataNodeServerAddr.Host, chunkFileName, in.ChunkOffset, len(in.Databuf))

	f, err = os.OpenFile(chunkFileName, os.O_RDWR|os.O_CREATE, 0660)
	defer f.Close()
	if err != nil {
		logger.Error("DataNode[%v]: Open file %v error:%v ", DataNodeServerAddr.Host, chunkFileName, err)
		ack.Ret = -1
		return &ack, nil
	}

	sret, err = f.Seek(in.ChunkOffset, 0)
	if sret != in.ChunkOffset || err != nil {
		logger.Error("DataNode[%v]: %v Seek to:%v ret:%v error:%v ", DataNodeServerAddr.Host, chunkFileName, in.ChunkOffset, sret, err)
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
	if err := s.writeDisk(in.BlockGroupID, in.ChunkID, in.Databuf); err != nil {
		ack.Ret = -1
	}
	return &ack, nil
}

// On Master

func (s *DataNodeServer) getReplicaStream(blockGroupID uint64) *M2SReplClientStream {
	s.M2SReplClientStreamCacheLocker.RLock()
	defer s.M2SReplClientStreamCacheLocker.RUnlock()

	ReplicaStream, ok := s.M2SReplClientStreamCache[blockGroupID]
	if !ok || ReplicaStream == nil {
		return nil
	}

	if ReplicaStream.stream == nil || ReplicaStream.isErr {
		logger.Error("DataNode[%v]: get ReplicaStream by BGID-%v but without stream or isErr %v!!", DataNodeServerAddr.Host, blockGroupID, ReplicaStream.isErr)
		return nil
	}

	atomic.AddInt32(&ReplicaStream.refCnt, 1)
	return ReplicaStream
}

func (s *DataNodeServer) putReplicaStream(ReplicaStream *M2SReplClientStream) {
	s.M2SReplClientStreamCacheLocker.Lock()
	defer s.M2SReplClientStreamCacheLocker.Unlock()

	tmpStream, ok := s.M2SReplClientStreamCache[ReplicaStream.BlockGroupID]
	if ok && tmpStream.isErr {
		delete(s.M2SReplClientStreamCache, ReplicaStream.BlockGroupID)
	}

	refCnt := atomic.AddInt32(&ReplicaStream.refCnt, -1)
	if refCnt > 0 {
		return
	}

	tmpStream, ok = s.M2SReplClientStreamCache[ReplicaStream.BlockGroupID]
	if ok && tmpStream == ReplicaStream {
		return
	}

	//the last user
	s.closeReplicaStream(ReplicaStream)
}

func (s *DataNodeServer) closeReplicaStream(ReplicaStream *M2SReplClientStream) {
	ReplicaStream.stream.CloseSend()
	ReplicaStream.conn.Close()
}

func (s *DataNodeServer) addReplicaStream(ReplicaStream *M2SReplClientStream) {
	s.M2SReplClientStreamCacheLocker.Lock()
	defer s.M2SReplClientStreamCacheLocker.Unlock()

	tmpStream, ok := s.M2SReplClientStreamCache[ReplicaStream.BlockGroupID]
	if ok && tmpStream != nil {
		delete(s.M2SReplClientStreamCache, ReplicaStream.BlockGroupID)
		refCnt := atomic.LoadInt32(&tmpStream.refCnt)
		if refCnt <= 0 {
			s.closeReplicaStream(tmpStream)
		}
	}

	s.M2SReplClientStreamCache[ReplicaStream.BlockGroupID] = ReplicaStream
	atomic.StoreInt32(&ReplicaStream.refCnt, 1)
}

func (s *DataNodeServer) C2MReplExit(clientStreamID uint64) {
	logger.Debug("DataNode[%v]: ClientID-%d: C2MRepl exit", DataNodeServerAddr.Host, clientStreamID)
	s.C2MReplServerStreamCacheLocker.Lock()
	C2MReplServerStream, _ := s.C2MReplServerStreamCache[clientStreamID]
	delete(s.C2MReplServerStreamCache, clientStreamID)
	s.C2MReplServerStreamCacheLocker.Unlock()

	if C2MReplServerStream != nil && C2MReplServerStream.ReplicaStream != nil {
		s.putReplicaStream(C2MReplServerStream.ReplicaStream)
	}
}

// C2MRepl ...
func (s *DataNodeServer) C2MRepl(stream dp.DataNode_C2MReplServer) error {

	clientStreamID := atomic.AddUint64(&s.ClientStreamID, 1)

	logger.Debug("DataNode[%v]: ClientID-%d: C2MRepl init for new client", DataNodeServerAddr.Host, clientStreamID)

	C2MReplServerStream := &C2MReplServerStream{stream: stream}
	s.C2MReplServerStreamCacheLocker.Lock()
	s.C2MReplServerStreamCache[clientStreamID] = C2MReplServerStream
	s.C2MReplServerStreamCacheLocker.Unlock()

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
		if err := s.writeDisk(in.BlockGroupID, in.ChunkID, in.Databuf); err != nil {
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

		if M2SReplClientStream == nil || M2SReplClientStream.isErr {
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
			M2SReplClientStream.isErr = true
			s.putReplicaStream(M2SReplClientStream)
			C2MReplServerStream.ReplicaStream = nil
			logger.Error("DataNode[%v]: ClientID-%d: C2MRepl Get M2SRepl Stream by BGID-%v but Send err %v, go create new Stream", DataNodeServerAddr.Host, clientStreamID, in.BlockGroupID, err)
		}

		M2SReplClientStream = s.CreateM2SReplClientStream(in.Slave, in.BlockGroupID)
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
			M2SReplClientStream.isErr = true
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
	mss.s.C2MReplServerStreamCacheLocker.Lock()
	for _, v := range mss.s.C2MReplServerStreamCache {
		if v.ReplicaStream == mss {
			//maybe we shold send err to Client here.
			v.NeedBreak = true
		}
	}
	mss.s.C2MReplServerStreamCacheLocker.Unlock()

	logger.Debug("DataNode[%v]: M2Sstream-%p: BGID-%v M2SRecv exit", DataNodeServerAddr.Host, mss, mss.BlockGroupID)
}

func (mss *M2SReplClientStream) BackWard() {

	logger.Debug("DataNode[%v]: BackWard M2Sstream-%p: BGID-%v M2SRecv init", DataNodeServerAddr.Host, mss, mss.BlockGroupID)

	defer mss.BackWardExit()

	for {
		in, err := mss.stream.Recv()
		if err == io.EOF {
			logger.Error("DataNode[%v]: BackWard M2Sstream-%p: BGID-%v M2SRecv EOF", DataNodeServerAddr.Host, mss, mss.BlockGroupID)
			mss.isErr = true
			break
		}
		if err != nil {
			logger.Error("DataNode[%v]: BackWard M2Sstream-%p: BGID-%v M2SRecv err %v", DataNodeServerAddr.Host, mss, mss.BlockGroupID, err)
			mss.isErr = true
			break
		}
		if in.Ret == -1000 {
			logger.Error("DataNode[%v]: BackWard M2Sstream-%p: BGID-%v M2SRecv receive Slave BackWard break", DataNodeServerAddr.Host, mss, mss.BlockGroupID)
			mss.isErr = true
			break
		}

		mss.s.C2MReplServerStreamCacheLocker.RLock()
		C2MReplServerStream, ok := mss.s.C2MReplServerStreamCache[in.StreamID]
		mss.s.C2MReplServerStreamCacheLocker.RUnlock()

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
			M2SReplServerStream.BackupHost = in.Backup

			M2SReplServerStream.S2BReplClientStream = s.CreateS2BStream(in.Backup, M2SReplServerStream)
			if M2SReplServerStream.S2BReplClientStream == nil {
				logger.Error("DataNode[%v]: M2SRepl BGID-%v CreateS2BStream err", DataNodeServerAddr.Host, M2SReplServerStream.BlockGroupID)
				return errors.New("CreateS2BStream err")
			}
		}

		if M2SReplServerStream.BackupHost != in.Backup || M2SReplServerStream.BlockGroupID != in.BlockGroupID {
			logger.Error("DataNode[%v]: M2SRepl  BlockGroup error: stream(BHOST-%v, BGID-%v) != in(BHOST-%v, BGID-%v)",
				DataNodeServerAddr.Host, M2SReplServerStream.BackupHost, M2SReplServerStream.BlockGroupID, in.Backup, in.BlockGroupID)
			return errors.New("BlockGroup err")
		}

		// save to localdisk
		if err := s.writeDisk(in.BlockGroupID, in.ChunkID, in.Databuf); err != nil {
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
		}
		if err != nil {
			logger.Error("DataNode[%v]: BGID-%v BackWard  S2BReplClientStream.Recv err %v", DataNodeServerAddr.Host, sbs.BlockGroupID, err)
			M2SReplServerStream.NeedBreak = true
		}
		if M2SReplServerStream.NeedBreak {
			ack := dp.StreamWriteAck{Ret: -1000}
			M2SReplServerStream.stream.Send(&ack)
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
		BlockGroupID = in.BlockGroupID

		ack := dp.StreamWriteAck{CommitID: in.CommitID, DataLen: in.DataLen, ChunkID: in.ChunkID, BlockGroupID: in.BlockGroupID, StreamID: in.StreamID, Ret: 0}
		// save to localdisk
		if err := s.writeDisk(in.BlockGroupID, in.ChunkID, in.Databuf); err != nil {
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

	bpath := path.Join(DataNodeServerAddr.Path, "blockgroup-"+strconv.FormatUint(in.BlockGroupID, 10))
	if ok, err := utils.LocalPathExists(bpath); !ok && err == nil {
		return err
	}

	chunkFileName := path.Join(bpath, "chunk-"+strconv.FormatUint(in.ChunkID, 10))

	f, err := os.Open(chunkFileName)
	defer f.Close()
	if err != nil {
		return err
	}

	sret, err := f.Seek(in.Offset, 0)
	if sret != in.Offset || err != nil {
		logger.Error("DataNode[%v]: %v Seek to:%v ret:%v error:%v ", DataNodeServerAddr.Host, chunkFileName, in.Offset, sret, err)
		return err
	}

	var ack dp.StreamReadChunkAck
	var totalsize int64
	bufsize := in.Readsize
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

		if totalsize >= in.Readsize {

			n = n - int(totalsize-in.Readsize)
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
	sid := in.BlockGroupID
	dhost := in.DstHost

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

	bgpath := path.Join(DataNodeServerAddr.Path, fmt.Sprintf("/blockgroup-%d", sid))
	if ok, err := utils.LocalPathExists(bgpath); !ok && err == nil {
		logger.Debug("DataNode[%v]: The Block:%v no chunkdata, so dont need copydata for Migrate", DataNodeServerAddr.Host, sid)
		ack.Ret = 0
		return &ack, nil
	}
	dirs, err := ioutil.ReadDir(bgpath)
	if err != nil {
		logger.Error("DataNode[%v]: List SrcBlk:%v failed:%v for Migrate", DataNodeServerAddr.Host, bgpath, err)
		ack.Ret = -1
		return &ack, err
	}

	for _, v := range dirs {
		if v.IsDir() {
			continue
		}
		fInfo := dp.FInfo{}
		fInfo.FName = v.Name()
		fInfo.BlockGroupID = sid
		fPath := bgpath + "/" + v.Name()

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
		logger.Error("DataNode[%v]: CloseAndRecv SrcBlkPath:%v to DstBlk failed:%v for Migrate", bgpath, err)
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

		bgpath := path.Join(DataNodeServerAddr.Path, fmt.Sprintf("/blockgroup-%d", finfo.BlockGroupID))
		if ok, err := utils.LocalPathExists(bgpath); !ok && err == nil {
			os.MkdirAll(bgpath, 0777)
		}

		chunkFileName := bgpath + "/" + finfo.FName
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
		logger.Debug("DataNode[%v]: Write Blk:%v One ChunkFile:%v for Migrate BLK Success!", DataNodeServerAddr.Host, finfo.BlockGroupID, chunkFileName)
	}
}

//DeleteChunk rpc DeleteChunks(eleteChunksReq) returns (eleteChunksAck){};
func (s *DataNodeServer) DeleteChunk(ctx context.Context, in *dp.DeleteChunkReq) (*dp.DeleteChunkAck, error) {
	var err error

	ack := dp.DeleteChunkAck{}

	chunkFileName := path.Join(DataNodeServerAddr.Path, "blockgroup-"+strconv.FormatUint(in.BlockGroupID, 10), "chunk-"+strconv.FormatUint(in.ChunkID, 10))

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

func init() {

	var loglevel string
	var volMgrHosts string

	flag.StringVar(&DataNodeServerAddr.Host, "host", "127.0.0.1:8801", "ContainerFS DataNode Host")
	flag.StringVar(&DataNodeServerAddr.Tier, "tier", "sas", "ContainerFS DataNode Storage Medium")
	flag.StringVar(&DataNodeServerAddr.Path, "datapath", "", "ContainerFS DataNode Data Path")
	flag.StringVar(&DataNodeServerAddr.Log, "logpath", "/export/Logs/containerfs/logs/", "ContainerFS Log Path")
	flag.StringVar(&loglevel, "loglevel", "error", "ContainerFS Log Level")
	flag.StringVar(&volMgrHosts, "volmgr", "10.8.64.216,10.8.64.217,10.8.64.218", "ContainerFS VolMgr hosts")

	flag.Parse()
        if len(os.Args) >= 2 && (os.Args[1] == "version") {
                fmt.Println(utils.Version())
                os.Exit(0)
        }

	tmp := strings.Split(volMgrHosts, ",")

	VolMgrHosts = make([]string, 3)
	VolMgrHosts[0] = tmp[0] + ":7703"
	VolMgrHosts[1] = tmp[1] + ":7713"
	VolMgrHosts[2] = tmp[2] + ":7723"

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

	registryToVolMgr()

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
