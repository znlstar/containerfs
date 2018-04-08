package datanode

import (
	"bufio"
	"errors"
	"os"
	"io"
	"path"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/tiglabs/containerfs/logger"
	"github.com/tiglabs/containerfs/proto/dp"
	"github.com/tiglabs/containerfs/utils"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
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

func (s *DataNodeServer) writeDisk(blockGroupID uint64, chunkID uint64, databuf []byte) error {

	bpath := path.Join(DtAddr.Path, "blockgroup-"+strconv.FormatUint(blockGroupID, 10))
	if ok, err := utils.LocalPathExists(bpath); !ok && err == nil {
		os.MkdirAll(bpath, 0777)
	}

	chunkFileName := path.Join(bpath, "chunk-"+strconv.FormatUint(chunkID, 10))

	f, err := os.OpenFile(chunkFileName, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0660)
	if err != nil {
		logger.Error("DataNode[%v]: Open file %v error:%v ", DtAddr.Host, chunkFileName, err)
		return err
	}
	defer f.Close()

	w := bufio.NewWriter(f)
	_, err = w.Write(databuf)
	if err != nil {
		logger.Error("DataNode[%v]: Write to file %v error:%v ", DtAddr.Host, chunkFileName, err)
		return err
	}

	err = w.Flush()
	if err != nil {
		logger.Error("DataNode[%v]: Flush file %v error:%v ", DtAddr.Host, chunkFileName, err)
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

	bpath := path.Join(DtAddr.Path, "blockgroup-"+strconv.FormatUint(in.BlockGroupID, 10))
	if ok, err := utils.LocalPathExists(bpath); !ok && err == nil {
		os.MkdirAll(bpath, 0777)
	}

	chunkFileName := path.Join(bpath, "chunk-"+strconv.FormatUint(in.ChunkID, 10))

	logger.Debug("DataNode[%v]: write file %v with offset %v and len %v", DtAddr.Host, chunkFileName, in.ChunkOffset, len(in.Databuf))

	f, err = os.OpenFile(chunkFileName, os.O_RDWR|os.O_CREATE, 0660)
	defer f.Close()
	if err != nil {
		logger.Error("DataNode[%v]: Open file %v error:%v ", DtAddr.Host, chunkFileName, err)
		ack.Ret = -1
		return &ack, nil
	}

	sret, err = f.Seek(in.ChunkOffset, 0)
	if sret != in.ChunkOffset || err != nil {
		logger.Error("DataNode[%v]: %v Seek to:%v ret:%v error:%v ", DtAddr.Host, chunkFileName, in.ChunkOffset, sret, err)
		ack.Ret = -1
		return &ack, nil
	}

	ret, err = f.Write(in.Databuf)
	if ret != len(in.Databuf) || err != nil {
		logger.Error("DataNode[%v]: %v Write len:%v ret:%v error:%v ", DtAddr.Host, chunkFileName, len(in.Databuf), ret, err)
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
		logger.Error("DataNode[%v]: get ReplicaStream by BGID-%v but without stream or isErr %v!!", DtAddr.Host, blockGroupID, ReplicaStream.isErr)
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
	logger.Debug("DataNode[%v]: ClientID-%d: C2MRepl exit", DtAddr.Host, clientStreamID)
	s.C2MReplServerStreamCacheLocker.Lock()
	C2MReplServerStream, _ := s.C2MReplServerStreamCache[clientStreamID]
	delete(s.C2MReplServerStreamCache, clientStreamID)
	s.C2MReplServerStreamCacheLocker.Unlock()

	if C2MReplServerStream != nil && C2MReplServerStream.ReplicaStream != nil {
		s.putReplicaStream(C2MReplServerStream.ReplicaStream)
	}
}

// On Master: C2MRepl ...
func (s *DataNodeServer) C2MRepl(stream dp.DataNode_C2MReplServer) error {

	clientStreamID := atomic.AddUint64(&s.ClientStreamID, 1)

	logger.Debug("DataNode[%v]: ClientID-%d: C2MRepl init for new client", DtAddr.Host, clientStreamID)

	C2MReplServerStream := &C2MReplServerStream{stream: stream}
	s.C2MReplServerStreamCacheLocker.Lock()
	s.C2MReplServerStreamCache[clientStreamID] = C2MReplServerStream
	s.C2MReplServerStreamCacheLocker.Unlock()

	defer s.C2MReplExit(clientStreamID)

	for {
		in, err := C2MReplServerStream.stream.Recv()
		if err == io.EOF {
			logger.Debug("DataNode[%v]: ClientID-%d: C2MRepl Recv EOF", DtAddr.Host, clientStreamID)
			return nil
		}
		if err != nil {
			logger.Debug("DataNode[%v]: ClientID-%d: C2MRepl Recv err %v", DtAddr.Host, clientStreamID, err)
			return err
		}
		if C2MReplServerStream.NeedBreak {
			logger.Error("DataNode[%v]: ClientID-%d: C2MRepl NeedBreak", DtAddr.Host, clientStreamID)
			return errors.New("C2MReplServerStream NeedBreak")
		}

		// save to localdisk
		if err := s.writeDisk(in.BlockGroupID, in.ChunkID, in.Databuf); err != nil {
			logger.Error("DataNode[%v]: ClientID-%d: C2MRepl writeDisk err %v", DtAddr.Host, clientStreamID, err)
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
				logger.Debug("DataNode[%v]: ClientID-%d: C2MRepl Get M2SRepl Stream by BGID-%v and Send ok", DtAddr.Host, clientStreamID, in.BlockGroupID)
				continue
			}

			//should we wait for other pending requests?
			M2SReplClientStream.isErr = true
			s.putReplicaStream(M2SReplClientStream)
			C2MReplServerStream.ReplicaStream = nil
			logger.Error("DataNode[%v]: ClientID-%d: C2MRepl Get M2SRepl Stream by BGID-%v but Send err %v, go create new Stream", DtAddr.Host, clientStreamID, in.BlockGroupID, err)
		}

		M2SReplClientStream = s.CreateM2SReplClientStream(in.Slave, in.BlockGroupID)
		if M2SReplClientStream == nil {
			logger.Error("DataNode[%v]: ClientID-%d:  Create M2SRepl err", DtAddr.Host, clientStreamID)
			return errors.New("Create M2SRepl err")
		}
		C2MReplServerStream.ReplicaStream = M2SReplClientStream

		M2SReplClientStream.Lock()
		err = M2SReplClientStream.stream.Send(in)
		M2SReplClientStream.Unlock()

		if err != nil {
			logger.Error("DataNode[%v]: ClientID-%d:  M2SRepl Stream Send err %v", DtAddr.Host, clientStreamID, err)
			M2SReplClientStream.isErr = true
			return err
		}
	}
}

func (s *DataNodeServer) CreateM2SReplClientStream(slaveHost string, blockGroupID uint64) *M2SReplClientStream {

	M2Sconn, err := grpc.Dial(slaveHost, grpc.WithInsecure(), grpc.WithBlock(), grpc.WithTimeout(time.Millisecond*300), grpc.FailOnNonTempDialError(true))
	if err != nil {
		logger.Error("DataNode[%v]: Create M2SStream: Dial to slave %v err %v", DtAddr.Host, slaveHost, err)
		return nil
	}
	M2Sclient := dp.NewDataNodeClient(M2Sconn)
	stream, err := M2Sclient.M2SRepl(context.Background())
	if err != nil {
		logger.Error("DataNode[%v]: Create M2SStream to slave %v err %v", DtAddr.Host, slaveHost, err)
		return nil
	}

	M2SReplClientStream := &M2SReplClientStream{conn: M2Sconn, stream: stream, s: s, BlockGroupID: blockGroupID}
	s.addReplicaStream(M2SReplClientStream)

	go M2SReplClientStream.BackWard()

	logger.Debug("DataNode[%v]: Create M2SStream success ...", DtAddr.Host)

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

	logger.Debug("DataNode[%v]: M2Sstream-%p: BGID-%v M2SRecv exit", DtAddr.Host, mss, mss.BlockGroupID)
}

func (mss *M2SReplClientStream) BackWard() {

	logger.Debug("DataNode[%v]: BackWard M2Sstream-%p: BGID-%v M2SRecv init", DtAddr.Host, mss, mss.BlockGroupID)

	defer mss.BackWardExit()

	for {
		in, err := mss.stream.Recv()
		if err == io.EOF {
			logger.Error("DataNode[%v]: BackWard M2Sstream-%p: BGID-%v M2SRecv EOF", DtAddr.Host, mss, mss.BlockGroupID)
			mss.isErr = true
			break
		}
		if err != nil {
			logger.Error("DataNode[%v]: BackWard M2Sstream-%p: BGID-%v M2SRecv err %v", DtAddr.Host, mss, mss.BlockGroupID, err)
			mss.isErr = true
			break
		}
		if in.Ret == -1000 {
			logger.Error("DataNode[%v]: BackWard M2Sstream-%p: BGID-%v M2SRecv receive Slave BackWard break", DtAddr.Host, mss, mss.BlockGroupID)
			mss.isErr = true
			break
		}

		mss.s.C2MReplServerStreamCacheLocker.RLock()
		C2MReplServerStream, ok := mss.s.C2MReplServerStreamCache[in.StreamID]
		mss.s.C2MReplServerStreamCacheLocker.RUnlock()

		if ok && C2MReplServerStream != nil {
			logger.Debug("DataNode[%v]: BackWard M2Sstream-%p: BGID-%v M2SRecv get ClientStream success with ClientID-%v", DtAddr.Host, mss, mss.BlockGroupID, in.StreamID)

			C2MReplServerStream.Lock()
			if err := C2MReplServerStream.stream.Send(in); err != nil {
				logger.Error("DataNode[%v]: BackWard M2Sstream-%p: BGID-%v M2SRecv send for ClientID-%v err %v", DtAddr.Host, mss, mss.BlockGroupID, in.StreamID, err)
				C2MReplServerStream.NeedBreak = true
			}
			C2MReplServerStream.Unlock()
		} else {
			logger.Error("DataNode[%v]: BackWard M2Sstream-%p: BGID-%v M2SRecv get ClientStream failed with ClientID-%v!", DtAddr.Host, mss, mss.BlockGroupID, in.StreamID)
		}

	}

}

// On Slave: M2SRepl...
func (s *DataNodeServer) M2SRepl(stream dp.DataNode_M2SReplServer) error {

	M2SReplServerStream := &M2SReplServerStream{stream: stream}
	logger.Debug("DataNode[%v]: M2SRepl init for stream %p", DtAddr.Host, M2SReplServerStream)

	defer func() {
		if M2SReplServerStream.S2BReplClientStream != nil {
			M2SReplServerStream.S2BReplClientStream.stream.CloseSend()
			M2SReplServerStream.S2BReplClientStream.conn.Close()
			M2SReplServerStream.S2BReplClientStream = nil
		}
		logger.Debug("DataNode[%v]: M2SRepl exit for stream %p BackupHost %v BGID-%v",
			DtAddr.Host, M2SReplServerStream, M2SReplServerStream.BackupHost, M2SReplServerStream.BlockGroupID)
	}()

	for {
		in, err := M2SReplServerStream.stream.Recv()
		if err == io.EOF {
			logger.Debug("DataNode[%v]: M2SRepl BGID-%v stream.Recv EOF", DtAddr.Host, M2SReplServerStream.BlockGroupID)
			return nil
		}
		if err != nil {
			logger.Error("DataNode[%v]: M2SRepl BGID-%v  stream.Recv err %v", DtAddr.Host, M2SReplServerStream.BlockGroupID, err)
			return err
		}

		if M2SReplServerStream.NeedBreak {
			logger.Error("DataNode[%v]: M2SRepl BGID-%v NeedBreak", DtAddr.Host, M2SReplServerStream.BlockGroupID)
			return errors.New("M2SReplServerStream NeedBreak")
		}

		ack := dp.StreamWriteAck{CommitID: in.CommitID, DataLen: in.DataLen, ChunkID: in.ChunkID, BlockGroupID: in.BlockGroupID, StreamID: in.StreamID, Ret: 0}

		if in.Backup != "" {
			if M2SReplServerStream.S2BReplClientStream == nil {
				M2SReplServerStream.BlockGroupID = in.BlockGroupID
				M2SReplServerStream.BackupHost = in.Backup

				M2SReplServerStream.S2BReplClientStream = s.CreateS2BStream(in.Backup, M2SReplServerStream)
				if M2SReplServerStream.S2BReplClientStream == nil {
					logger.Error("DataNode[%v]: M2SRepl BGID-%v CreateS2BStream err", DtAddr.Host, M2SReplServerStream.BlockGroupID)
					return errors.New("CreateS2BStream err")
				}
			}

			if M2SReplServerStream.BackupHost != in.Backup || M2SReplServerStream.BlockGroupID != in.BlockGroupID {
				logger.Error("DataNode[%v]: M2SRepl  BlockGroup error: stream(BHOST-%v, BGID-%v) != in(BHOST-%v, BGID-%v)",
					DtAddr.Host, M2SReplServerStream.BackupHost, M2SReplServerStream.BlockGroupID, in.Backup, in.BlockGroupID)
				return errors.New("BlockGroup err")
			}

			// save to localdisk
			if err := s.writeDisk(in.BlockGroupID, in.ChunkID, in.Databuf); err != nil {
				logger.Error("DataNode[%v]: BGID-%v M2SRepl writeDisk failed %v", DtAddr.Host, M2SReplServerStream.BlockGroupID, err)
				return err
			}

			if err := M2SReplServerStream.S2BReplClientStream.stream.Send(in); err != nil {
				logger.Error("DataNode[%v]: BGID-%v M2SRepl S2BReplClientStream.Send failed %v", DtAddr.Host, M2SReplServerStream.BlockGroupID, err)
				ack.Ret = -1
				stream.Send(&ack)
				return err
			}
		} else {
			if err := s.writeDisk(in.BlockGroupID, in.ChunkID, in.Databuf); err != nil {
				logger.Error("DataNode[%v]: M2SRepl BGID-%v  writeDisk err %v", DtAddr.Host, in.BlockGroupID, err)
				ack.Ret = -1
			}

			if err := stream.Send(&ack); err != nil {
				logger.Error("DataNode[%v]: M2SRepl BGID-%v stream.Send err %v", DtAddr.Host, in.BlockGroupID, err)
				return err
			}
		}

	}

}

func (s *DataNodeServer) CreateS2BStream(backUpHost string, M2SReplServerStream *M2SReplServerStream) *S2BReplClientStream {

	S2Bconn, err := grpc.Dial(backUpHost, grpc.WithInsecure(), grpc.WithBlock(), grpc.WithTimeout(time.Millisecond*300), grpc.FailOnNonTempDialError(true))
	if err != nil {
		logger.Error("DataNode[%v]: CreateS2BStream Dail to Backup %v failed %v", DtAddr.Host, backUpHost, err)
		return nil
	}
	S2Bclient := dp.NewDataNodeClient(S2Bconn)

	stream, err := S2Bclient.S2BRepl(context.Background())
	if err != nil {
		logger.Error("DataNode[%v]: CreateS2BStream rpc to Backup failed %v", DtAddr.Host, err)
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
			logger.Debug("DataNode[%v]: BGID-%v BackWard  S2BReplClientStream.Recv EOF", DtAddr.Host, sbs.BlockGroupID)
			M2SReplServerStream.NeedBreak = true
		}
		if err != nil {
			logger.Error("DataNode[%v]: BGID-%v BackWard  S2BReplClientStream.Recv err %v", DtAddr.Host, sbs.BlockGroupID, err)
			M2SReplServerStream.NeedBreak = true
		}
		if M2SReplServerStream.NeedBreak {
			ack := dp.StreamWriteAck{Ret: -1000}
			M2SReplServerStream.stream.Send(&ack)
			break
		}

		if err := M2SReplServerStream.stream.Send(in); err != nil {
			logger.Error("DataNode[%v]: BGID-%v BackWard  M2SReplServerStream.Send err %v", DtAddr.Host, sbs.BlockGroupID, err)
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
			logger.Debug("DataNode[%v]: S2BRepl BGID-%v stream.Recv EOF", DtAddr.Host, BlockGroupID)
			return nil
		}
		if err != nil {
			logger.Debug("DataNode[%v]: S2BRepl BGID-%v stream.Recv err %v", DtAddr.Host, BlockGroupID, err)
			return err
		}
		BlockGroupID = in.BlockGroupID

		ack := dp.StreamWriteAck{CommitID: in.CommitID, DataLen: in.DataLen, ChunkID: in.ChunkID, BlockGroupID: in.BlockGroupID, StreamID: in.StreamID, Ret: 0}
		// save to localdisk
		if err := s.writeDisk(in.BlockGroupID, in.ChunkID, in.Databuf); err != nil {
			logger.Error("DataNode[%v]: S2BRepl BGID-%v  writeDisk err %v", DtAddr.Host, BlockGroupID, err)
			ack.Ret = -1
		}

		if err := stream.Send(&ack); err != nil {
			logger.Error("DataNode[%v]: S2BRepl BGID-%v stream.Send err %v", DtAddr.Host, BlockGroupID, err)
			return err
		}

	}

}
