package datanode

import (
	"bufio"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path"
	"strconv"
	"time"

	"github.com/tiglabs/containerfs/logger"
	"github.com/tiglabs/containerfs/proto/dp"
	"github.com/tiglabs/containerfs/utils"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
)

// StreamReadChunk: use stream mode for read chunk data
func (s *DataNodeServer) StreamReadChunk(in *dp.StreamReadChunkReq, stream dp.DataNode_StreamReadChunkServer) error {

	bpath := path.Join(DtAddr.Path, "blockgroup-"+strconv.FormatUint(in.BlockGroupID, 10))
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
		logger.Error("DataNode[%v]: %v Seek to:%v ret:%v error:%v ", DtAddr.Host, chunkFileName, in.Offset, sret, err)
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
			logger.Error("DataNode[%v]: read chunkfile:%v error:%v", DtAddr.Host, chunkFileName, err)
			return err
		}

		if n == 0 {
			logger.Debug("DataNode[%v]: read chunkfile:%v endsize:%v", DtAddr.Host, chunkFileName, totalsize)
			break
		}

		totalsize += int64(n)

		if totalsize >= in.Readsize {

			n = n - int(totalsize-in.Readsize)
			ack.Databuf = buf[:n]
			if err := stream.Send(&ack); err != nil {
				logger.Error("DataNode[%v]: Send stream data to fuse error:%v", DtAddr.Host, err)
				return err
			}
			break

		} else {
			ack.Databuf = buf[:n]
			if err := stream.Send(&ack); err != nil {
				logger.Error("DataNode[%v]: Send stream data to fuse error:%v", DtAddr.Host, err)
				return err
			}
		}

	}

	return nil
}

// RecvMigrateMsg: receive DataNode migrate message from VolMgr
func (s *DataNodeServer) RecvMigrateMsg(ctx context.Context, in *dp.RecvMigrateReq) (*dp.RecvMigrateAck, error) {
	ack := dp.RecvMigrateAck{}
	sid := in.BlockGroupID
	dhost := in.DstHost

	conn, err := grpc.Dial(dhost, grpc.WithInsecure())
	if err != nil {
		logger.Error("DataNode[%v]: Migrate failed : Dial to DestDataNode:%v failed:%v !", DtAddr.Host, dhost, err)
		ack.Ret = -1
		return &ack, err
	}
	defer conn.Close()
	dc := dp.NewDataNodeClient(conn)

	ctxtmp, _ := context.WithTimeout(context.Background(), 5*time.Minute)

	stream, err := dc.SendMigrateData(ctxtmp)
	if err != nil {
		logger.Error("DataNode[%v]: SendMigrate to DestDataNode:%v err:%v", DtAddr.Host, dhost, err)
		ack.Ret = -1
		return &ack, err
	}

	bgpath := path.Join(DtAddr.Path, fmt.Sprintf("/blockgroup-%d", sid))
	if ok, err := utils.LocalPathExists(bgpath); !ok && err == nil {
		logger.Debug("DataNode[%v]: The Block:%v no chunkdata, so dont need copydata for Migrate", DtAddr.Host, sid)
		ack.Ret = 0
		return &ack, nil
	}
	dirs, err := ioutil.ReadDir(bgpath)
	if err != nil {
		logger.Error("DataNode[%v]: List SrcBlk:%v failed:%v for Migrate", DtAddr.Host, bgpath, err)
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
			logger.Error("DataNode[%v]: Open SrcBlkFile:%v failed:%v for Migrate", DtAddr.Host, fPath, err)
			ack.Ret = -1
			return &ack, err
		}
		buf := make([]byte, 2*1024*1024)
		r := bufio.NewReader(fd)

		for {
			n, err := r.Read(buf)
			if err != nil && err != io.EOF {
				logger.Error("DataNode[%v]: Read SrcBlkFile:%v failed:%v for Migrate", DtAddr.Host, fPath, err)
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
				logger.Debug("DataNode[%v]: Send SrcBlkFile:%v to DstBlk success for Migrate because chunkdata IoEOF", DtAddr.Host, fPath)
				continue
			}
			if err != nil {
				logger.Error("DataNode[%v]: Send SrcBlkFile:%v to DstBlk failed:%v for Migrate", DtAddr.Host, fPath, err)
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

// SendMigrateData: send chunk data to the new DataNode
func (s *DataNodeServer) SendMigrateData(stream dp.DataNode_SendMigrateDataServer) error {
	for {
		finfo, err := stream.Recv()
		if err == io.EOF {
			return stream.SendAndClose(&dp.SendAck{Ret: 0})
		}
		if err != nil {
			logger.Error("DataNode[%v]: Recv from SrcBlk for Migrate Blk err:%v", DtAddr.Host, err)
			return err
		}

		bgpath := path.Join(DtAddr.Path, fmt.Sprintf("/blockgroup-%d", finfo.BlockGroupID))
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
		logger.Debug("DataNode[%v]: Write Blk:%v One ChunkFile:%v for Migrate BLK Success!", DtAddr.Host, finfo.BlockGroupID, chunkFileName)
	}
}

//DeleteChunk: delete chunk file
func (s *DataNodeServer) DeleteChunk(ctx context.Context, in *dp.DeleteChunkReq) (*dp.DeleteChunkAck, error) {
	var err error

	ack := dp.DeleteChunkAck{}

	chunkFileName := path.Join(DtAddr.Path, "blockgroup-"+strconv.FormatUint(in.BlockGroupID, 10), "chunk-"+strconv.FormatUint(in.ChunkID, 10))

	err = os.Remove(chunkFileName)
	if err != nil {
		ack.Ret = 0
	} else {
		ack.Ret = 0
	}
	ack.Ret = 0
	return &ack, nil
}
