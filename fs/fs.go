package cfs

import (
	"bufio"
	"bytes"
	"github.com/ipdcode/containerfs/logger"
	dp "github.com/ipdcode/containerfs/proto/dp"
	mp "github.com/ipdcode/containerfs/proto/mp"
	vp "github.com/ipdcode/containerfs/proto/vp"
	"github.com/ipdcode/containerfs/utils"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"io"
	"fmt"
	"os"
	"strconv"
	"sync"
	"syscall"
)

var VolMgrAddr string
var MetaNodeAddr string

const (
	O_RDONLY = os.O_RDONLY // 0    00000000000000000000000000000000
	O_WRONLY = os.O_WRONLY // 1    00000000000000000000000000000001
	O_RDWR   = os.O_RDWR   // 2    00000000000000000000000000000010
	O_APPEND = os.O_APPEND // 1024 00000000000000000000010000000000
	O_CREATE = os.O_CREATE // 64   00000000000000000000000001000000
	O_TRUNC  = os.O_TRUNC  // 512  00000000000000000000001000000000
	O_DIRECT = 0x4000
	O_MVOPT  = O_RDONLY | 0x20000
	O_TAROPT = O_MVOPT | syscall.O_NONBLOCK
)

const (
	chunkSize  = 64 * 1024 * 1024
	bufferSize = 128 * 1024
)

type CFS struct {
	VolID  string
	Status int // 0 ok , 1 readonly 2 invaild
}

func CreateVol(name string, capacity string) int32 {
	conn, err := grpc.Dial(VolMgrAddr, grpc.WithInsecure())
	if err != nil {
		logger.Error("CreateVol failed,Dial to volmgr fail :%v\n", err)
		return -1

	}
	defer conn.Close()
	vc := vp.NewVolMgrClient(conn)
	spaceQuota, _ := strconv.Atoi(capacity)
	pCreateVolReq := &vp.CreateVolReq{
		VolName:    name,
		SpaceQuota: int32(spaceQuota),
		MetaDomain: MetaNodeAddr,
	}
	pCreateVolAck, _ := vc.CreateVol(context.Background(), pCreateVolReq)

	// send to metadata to registry a new map
	fmt.Println("-------")
	fmt.Println(MetaNodeAddr)
	conn2, err2 := grpc.Dial(MetaNodeAddr, grpc.WithInsecure())
	if err2 != nil {
		logger.Error("CreateVol failed,Dial to metanode fail :%v\n", err2)
		return -1
	}
	defer conn2.Close()
	mc := mp.NewMetaNodeClient(conn2)
	pmCreateNameSpaceReq := &mp.CreateNameSpaceReq{
		VolID: pCreateVolAck.UUID,
	}
	pmCreateNameSpaceAck, _ := mc.CreateNameSpace(context.Background(), pmCreateNameSpaceReq)
	if pmCreateNameSpaceAck.Ret != 0 {
		logger.Error("CreateNameSpace failed :%v\n", pmCreateNameSpaceAck.Ret)
		return -1
	}

	return 0
}

func GetVolInfo(name string) (int32, *vp.GetVolInfoAck) {

	conn, err := grpc.Dial(VolMgrAddr, grpc.WithInsecure())
	if err != nil {
		logger.Error("GetVolInfo failed,Dial to volmgr fail :%v\n", err)
		return -1, nil
	}
	defer conn.Close()
	vc := vp.NewVolMgrClient(conn)
	pGetVolInfoReq := &vp.GetVolInfoReq{
		UUID: name,
	}
	pGetVolInfoAck, _ := vc.GetVolInfo(context.Background(), pGetVolInfoReq)
	if pGetVolInfoAck.Ret != 0 {
		return 1, nil
	}
	return 0, pGetVolInfoAck
}

func GetFSInfo(name string) (int32, *mp.GetFSInfoAck) {

	conn, err := grpc.Dial(MetaNodeAddr, grpc.WithInsecure())
	if err != nil {
		logger.Error("GetFSInfo failed,Dial to metanode fail :%v\n", err)
		return -1, nil
	}
	defer conn.Close()
	mc := mp.NewMetaNodeClient(conn)
	pGetFSInfoReq := &mp.GetFSInfoReq{
		VolID: name,
	}
	pGetFSInfoAck, _ := mc.GetFSInfo(context.Background(), pGetFSInfoReq)
	if pGetFSInfoAck.Ret != 0 {
		return 1, nil
	}
	return 0, pGetFSInfoAck
}

func OpenFileSystem(UUID string) *CFS {
	cfs := CFS{VolID: UUID, Status: 0}
	return &cfs
}

func (cfs *CFS) CreateDir(path string) int32 {
	//fmt.Println("CreateDir...")
	conn, err := grpc.Dial(MetaNodeAddr, grpc.WithInsecure())
	if err != nil {
		logger.Error("CreateDir failed,Dial to metanode fail :%v\n", err)
		return -1
	}
	defer conn.Close()
	mc := mp.NewMetaNodeClient(conn)
	pCreateDirReq := &mp.CreateDirReq{
		FullPathName: path,
		VolID:        cfs.VolID,
	}
	pCreateDirAck, _ := mc.CreateDir(context.Background(), pCreateDirReq)

	return pCreateDirAck.Ret

}
func (cfs *CFS) Stat(path string) (int32, *mp.InodeInfo) {
	conn, err := grpc.Dial(MetaNodeAddr, grpc.WithInsecure())
	if err != nil {
		logger.Error("Stat failed,Dial to metanode fail :%v\n", err)
		return -1, nil
	}
	defer conn.Close()
	mc := mp.NewMetaNodeClient(conn)
	pStatReq := &mp.StatReq{
		FullPathName: path,
		VolID:        cfs.VolID,
	}
	pStatAck, _ := mc.Stat(context.Background(), pStatReq)

	return pStatAck.Ret, pStatAck.InodeInfo

}
func (cfs *CFS) List(path string) (int32, []*mp.InodeInfo) {
	conn, err := grpc.Dial(MetaNodeAddr, grpc.WithInsecure())
	if err != nil {
		logger.Error("List failed,Dial to metanode fail :%v\n", err)
		return -1, nil
	}
	defer conn.Close()
	mc := mp.NewMetaNodeClient(conn)
	pListReq := &mp.ListReq{
		FullPathName: path,
		VolID:        cfs.VolID,
	}
	pListAck, _ := mc.List(context.Background(), pListReq)

	return pListAck.Ret, pListAck.InodeInfos

}
func (cfs *CFS) DeleteDir(path string) int32 {
	conn, err := grpc.Dial(MetaNodeAddr, grpc.WithInsecure())
	if err != nil {
		logger.Error("DeleteDir failed,Dial to metanode fail :%v\n", err)
		return -1
	}
	defer conn.Close()
	mc := mp.NewMetaNodeClient(conn)
	pDeleteDirReq := &mp.DeleteDirReq{
		FullPathName: path,
		VolID:        cfs.VolID,
	}
	pDeleteDirAck, _ := mc.DeleteDir(context.Background(), pDeleteDirReq)

	return pDeleteDirAck.Ret
}

func (cfs *CFS) Rename(path1 string, path2 string) int32 {
	conn, err := grpc.Dial(MetaNodeAddr, grpc.WithInsecure())
	if err != nil {
		logger.Error("Rename failed,Dial to metanode fail :%v\n", err)
		return -1
	}
	defer conn.Close()
	mc := mp.NewMetaNodeClient(conn)
	pRenameReq := &mp.RenameReq{
		FullPathName1: path1,
		FullPathName2: path2,
		VolID:         cfs.VolID,
	}
	pRenameAck, _ := mc.Rename(context.Background(), pRenameReq)

	return pRenameAck.Ret
}

func (cfs *CFS) OpenFile(path string, flags int) (int32, *CFile) {
	var ret int32
	var writer int32 = 0
	var tmpFileSize int64 = 0

	cfile := CFile{}

	if flags == O_RDONLY || flags == O_MVOPT || flags == O_TAROPT {
		chunkInfos := make([]*mp.ChunkInfo, 0)
		if ret, chunkInfos = cfs.GetFileChunks(path); ret != 0 {
			return ret, nil
		}

		for i := range chunkInfos {
			tmpFileSize += int64(chunkInfos[i].ChunkSize)
		}
		cfile = CFile{
			Path:       path,
			OpenFlag:   flags,
			cfs:        cfs,
			Writer:     writer,
			FileSize:   tmpFileSize,
			chunks:     chunkInfos,
			lastoffset: 0,
			readBuf:    []byte{},
		}
	} else if (flags&O_APPEND) != 0 && (flags&O_CREATE) == 0 {

		chunkInfos := make([]*mp.ChunkInfo, 0)

		if ret, chunkInfos = cfs.GetFileChunks(path); ret != 0 {
			return ret, nil
		}

		for i := range chunkInfos {
			tmpFileSize += int64(chunkInfos[i].ChunkSize)
		}
		lastChunk := chunkInfos[len(chunkInfos)-1]
		tmpBuffer := wBuffer{
			buffer:    new(bytes.Buffer),
			freeSize:  bufferSize,
			chunkInfo: lastChunk,
		}

		wChannel := make(chan *wBuffer, 128)
		cfile = CFile{
			Path:     path,
			OpenFlag: flags,
			cfs:      cfs,
			Writer:   writer,
			FileSize: tmpFileSize,
			chunks:   chunkInfos,
			wChannel: wChannel,
			wBuffer:  tmpBuffer,
		}
		go cfile.flushChannel()

	} else {

		writer = 1
		cfs.DeleteFile(path)
		if ret = cfs.CreateFile(path); ret != 0 {
			return ret, nil
		}

		tmpBuffer := wBuffer{
			buffer:   new(bytes.Buffer),
			freeSize: bufferSize,
		}

		wChannel := make(chan *wBuffer, 128)
		cfile = CFile{
			Path:     path,
			OpenFlag: flags,
			cfs:      cfs,
			Writer:   writer,
			FileSize: tmpFileSize,
			wChannel: wChannel,
			wBuffer:  tmpBuffer,
		}
		go cfile.flushChannel()
	}
	return 0, &cfile
}

func (cfs *CFS) CreateFile(path string) int32 {
	conn, err := grpc.Dial(MetaNodeAddr, grpc.WithInsecure())
	if err != nil {
		logger.Error("CreateFile failed,Dial to metanode fail :%v\n", err)
		return -1
	}
	defer conn.Close()
	mc := mp.NewMetaNodeClient(conn)
	pCreateFileReq := &mp.CreateFileReq{
		FullPathName: path,
		VolID:        cfs.VolID,
	}
	pCreateFileAck, _ := mc.CreateFile(context.Background(), pCreateFileReq)
	if pCreateFileAck.Ret == 1 {
		return 1
	}
	if pCreateFileAck.Ret == 2 {
		return 2
	}
	if pCreateFileAck.Ret == 17 {
		return 17
	}
	return 0
}

func (cfs *CFS) DeleteFile(path string) int32 {

	ret, chunkInfos := cfs.GetFileChunks(path)
	if ret != 0 {
		return ret
	}

	for _, v1 := range chunkInfos {
		for _, v2 := range v1.BlockGroup.BlockInfos {

			addr := utils.Inet_ntoa(v2.DataNodeIP).String() + ":" + strconv.Itoa(int(v2.DataNodePort))
			conn, err := grpc.Dial(addr, grpc.WithInsecure())
			if err != nil {
				logger.Error("DeleteFile failed,Dial to datanode fail :%v\n", err)
				return -1
			}

			dc := dp.NewDataNodeClient(conn)

			dpDeleteChunkReq := &dp.DeleteChunkReq{
				ChunkID: v1.ChunkID,
				BlockID: v2.BlockID,
			}
			dpDeleteChunkAck, _ := dc.DeleteChunk(context.Background(), dpDeleteChunkReq)
			if dpDeleteChunkAck.Ret != 0 {
				//return dpDeleteChunkAck.Ret
			}
			conn.Close()
		}
	}

	conn, err := grpc.Dial(MetaNodeAddr, grpc.WithInsecure())
	if err != nil {
		logger.Error("DeleteFile failed,Dial to metanode fail :%v\n", err)
		return -1
	}
	defer conn.Close()
	mc := mp.NewMetaNodeClient(conn)
	mpDeleteFileReq := &mp.DeleteFileReq{
		FullPathName: path,
		VolID:        cfs.VolID,
	}
	mpDeleteFileAck, _ := mc.DeleteFile(context.Background(), mpDeleteFileReq)

	return mpDeleteFileAck.Ret

}
func (cfs *CFS) AllocateChunk(path string) (int32, *mp.ChunkInfo) {
	conn, err := grpc.Dial(MetaNodeAddr, grpc.WithInsecure())
	if err != nil {
		logger.Error("AllocateChunk failed,Dial to metanode fail :%v\n", err)
		return -1, nil
	}
	defer conn.Close()
	mc := mp.NewMetaNodeClient(conn)
	pAllocateChunkReq := &mp.AllocateChunkReq{
		FileName: path,
		VolID:    cfs.VolID,
	}
	pAllocateChunkAck, _ := mc.AllocateChunk(context.Background(), pAllocateChunkReq)
	if pAllocateChunkAck.Ret != 0 {
		return pAllocateChunkAck.Ret, nil
	}

	return pAllocateChunkAck.Ret, pAllocateChunkAck.ChunkInfo
}

func (cfs *CFS) GetFileChunks(path string) (int32, []*mp.ChunkInfo) {
	conn, err := grpc.Dial(MetaNodeAddr, grpc.WithInsecure())
	if err != nil {
		logger.Error("GetFileChunks failed,Dial to metanode fail :%v\n", err)
		return -1, nil
	}
	defer conn.Close()
	mc := mp.NewMetaNodeClient(conn)
	pGetFileChunksReq := &mp.GetFileChunksReq{
		FileName: path,
		VolID:    cfs.VolID,
	}
	pGetFileChunksAck, _ := mc.GetFileChunks(context.Background(), pGetFileChunksReq)
	if pGetFileChunksAck.Ret != 0 {
		return pGetFileChunksAck.Ret, nil
	}
	return pGetFileChunksAck.Ret, pGetFileChunksAck.ChunkInfos
}

type wBuffer struct {
	freeSize  int32         // chunk size
	chunkInfo *mp.ChunkInfo // chunk info
	buffer    *bytes.Buffer // chunk data
}
type CFile struct {
	cfs      *CFS
	Path     string
	OpenFlag int
	FileSize int64

	// for write
	WMutex sync.Mutex
	Writer int32
	//FirstW bool
	wBuffer       wBuffer
	wChannel      chan *wBuffer // write channel
	wg            sync.WaitGroup
	wLastDataNode [3]string

	// for read
	lastoffset int64
	RMutex     sync.Mutex
	chunks     []*mp.ChunkInfo // chunkinfo
	readBuf    []byte
}

func (cfile *CFile) streamread(chunkidx int, ch chan *bytes.Buffer, offset int64, size int64) {
	for i, _ := range cfile.chunks[chunkidx].BlockGroup.BlockInfos {
		conn, err := grpc.Dial(utils.Inet_ntoa(cfile.chunks[chunkidx].BlockGroup.BlockInfos[i].DataNodeIP).String()+":"+strconv.Itoa(int(cfile.chunks[chunkidx].BlockGroup.BlockInfos[i].DataNodePort)), grpc.WithInsecure())
		if err != nil {
			logger.Error("streamread failed,Dial to datanode fail :%v\n", err)
			continue
		}

		dc := dp.NewDataNodeClient(conn)
		streamreadChunkReq := &dp.StreamReadChunkReq{
			ChunkID:  cfile.chunks[chunkidx].ChunkID,
			BlockID:  cfile.chunks[chunkidx].BlockGroup.BlockInfos[i].BlockID,
			Offset:   offset,
			Readsize: size,
		}
		stream, err := dc.StreamReadChunk(context.Background(), streamreadChunkReq)
		if err != nil {
			close(ch)
		}
		buffer := new(bytes.Buffer)
		for {
			ack, err := stream.Recv()
			if err == io.EOF {
				conn.Close()
				break
			}
			if err != nil {
				close(ch)
			}
			buffer.Write(ack.Databuf)
		}
		ch <- buffer
		conn.Close()
	}
}

func (cfile *CFile) Read(data *[]byte, offset int64, readsize int64) int64 {

	var buffer *bytes.Buffer

	cfile.lastoffset = offset
	if offset+readsize > cfile.FileSize {
		readsize = cfile.FileSize - offset
	} else {
		readsize = readsize
	}

	var length int64 = 0
	var free_offset int64
	var free_size int64
	var begin_chunk_num int = 0
	var end_chunk_num int = 0
	cur_offset := offset
	for i := range cfile.chunks {
		free_offset = cur_offset - int64(cfile.chunks[i].ChunkSize)
		if free_offset < 0 {
			begin_chunk_num = i
			break
		} else {
			cur_offset = free_offset
		}
	}

	cur_size := offset + readsize

	for i := range cfile.chunks {
		free_size = cur_size - int64(cfile.chunks[i].ChunkSize)
		if free_size <= 0 {
			end_chunk_num = i
			break
		} else {
			cur_size = free_size
		}
	}

	var each_read_len int64
	freesize := readsize

	for i := range cfile.chunks[begin_chunk_num : end_chunk_num+1] {
		index := i + begin_chunk_num
		if cur_offset+freesize < int64(cfile.chunks[index].ChunkSize) {
			each_read_len = freesize
		} else {
			each_read_len = int64(cfile.chunks[index].ChunkSize) - cur_offset
		}
		if len(cfile.readBuf) == 0 {
			if buffer == nil {
				buffer = new(bytes.Buffer)
			}
			var ch chan *bytes.Buffer
			ch = make(chan *bytes.Buffer)
			go cfile.streamread(index, ch, 0, int64(cfile.chunks[index].ChunkSize))
			buffer = <-ch
			cfile.readBuf = buffer.Next(buffer.Len())
			buffer.Reset()
		}
		*data = cfile.readBuf[cur_offset : cur_offset+each_read_len]
		cur_offset += each_read_len
		if cur_offset == int64(len(cfile.readBuf)) {
			cur_offset = 0
			cfile.readBuf = []byte{}
		}
		freesize = freesize - each_read_len
		length += each_read_len
	}
	return length
}

func (cfile *CFile) Seek(offset int64, whence int32) int64 {
	if whence <= 1 {
		cfile.lastoffset = offset
	} else if whence == 2 {
		cfile.lastoffset = cfile.FileSize - offset
	} else {
		logger.Error("Seek file for read/write invalid whence:%v\n", whence)
		return -1
	}
	return 0
}

func (cfile *CFile) Write(buf []byte, len int32) int32 {
	cfile.WMutex.Lock()
	defer cfile.WMutex.Unlock()
	var w int32
	w = 0
	for w < len {
		if (cfile.FileSize % chunkSize) == 0 {
			var ret int32
			ret, cfile.wBuffer.chunkInfo = cfile.cfs.AllocateChunk(cfile.Path)
			if ret != 0 {
				if ret == 28 /*ENOSPC*/ {
					return -1
				} else {
					return -2
				}
			}

		}
		if cfile.wBuffer.freeSize == 0 {
			cfile.wBuffer.buffer = new(bytes.Buffer)
			cfile.wBuffer.freeSize = bufferSize
		}
		if len-w < cfile.wBuffer.freeSize {
			cfile.wBuffer.buffer.Write(buf[w:len])
			cfile.wBuffer.freeSize = cfile.wBuffer.freeSize - (len - w)
			cfile.FileSize = cfile.FileSize + int64(len-w)
			cfile.wBuffer.chunkInfo.ChunkSize = cfile.wBuffer.chunkInfo.ChunkSize + int32(len-w)
			w = len
			break
		} else {
			cfile.wBuffer.buffer.Write(buf[w : w+cfile.wBuffer.freeSize])
			w = w + cfile.wBuffer.freeSize
			cfile.FileSize = cfile.FileSize + int64(cfile.wBuffer.freeSize)
			cfile.wBuffer.chunkInfo.ChunkSize = cfile.wBuffer.chunkInfo.ChunkSize + int32(cfile.wBuffer.freeSize)
			cfile.wBuffer.freeSize = 0
		}

		if cfile.wBuffer.freeSize == 0 {
			cfile.push2Channel()
		}
	}

	return w
}

func (cfile *CFile) push2Channel() int32 {
	if cfile.wBuffer.chunkInfo == nil {
		return 0
	}
	wBuffer := cfile.wBuffer   // record cur buffer
	cfile.wChannel <- &wBuffer // push to channel
	cfile.wg.Add(1)
	return 0
}

func (cfile *CFile) close2Channel() int32 {
	/*
	   1. avoid repeat push for integer file ETC. 64MB , the last push has already done in Write func
	*/
	if cfile.wBuffer.freeSize != 0 && cfile.wBuffer.chunkInfo != nil {
		wBuffer := cfile.wBuffer   // record cur buffer
		cfile.wChannel <- &wBuffer // push to channel
		cfile.wg.Add(1)
	}

	var wEndBuffer wBuffer
	wEndBuffer.buffer = nil
	wEndBuffer.freeSize = 0
	wEndBuffer.chunkInfo = nil

	cfile.wChannel <- &wEndBuffer // send the end flag to channel
	cfile.wg.Add(1)

	return 0
}

func (cfile *CFile) flushChannel() {
	var addr [3]string
	var connD [3]*grpc.ClientConn
	var dc [3]dp.DataNodeClient

	// update chunkinfo to metanode
	connM, err := grpc.Dial(MetaNodeAddr, grpc.WithInsecure())
	if err != nil {
		logger.Error("flushChannel,Dial failed:%v\n", err)
	}
	defer connM.Close()

	for {
		v := <-cfile.wChannel

		if v.buffer == nil && v.chunkInfo == nil && v.freeSize == 0 {
			if connD[0] != nil {
				connD[0].Close()
			}
			if connD[1] != nil {
				connD[1].Close()
			}
			if connD[2] != nil {
				connD[2].Close()
			}
			cfile.wg.Add(-1)
			break
		}

		dataBuf := v.buffer.Next(v.buffer.Len())

		for i := range v.chunkInfo.BlockGroup.BlockInfos {
			addr[i] = utils.Inet_ntoa(v.chunkInfo.BlockGroup.BlockInfos[i].DataNodeIP).String() + ":" + strconv.Itoa(int(v.chunkInfo.BlockGroup.BlockInfos[i].DataNodePort))
			if addr[i] != cfile.wLastDataNode[i] {
				if connD[i] != nil {
					connD[i].Close()
				}
				var err error
				connD[i], err = grpc.Dial(addr[i], grpc.WithInsecure())
				if err != nil {
					logger.Error("flushChannel to datanode failed,Dial failed:%v\n", err)
					return
				}
				dc[i] = dp.NewDataNodeClient(connD[i])
				cfile.wLastDataNode[i] = addr[i]
			}

			blockID := v.chunkInfo.BlockGroup.BlockInfos[i].BlockID
			chunkID := v.chunkInfo.ChunkID

			pWriteChunkReq := &dp.WriteChunkReq{
				ChunkID: chunkID,
				BlockID: blockID,
				Databuf: dataBuf,
			}
			pWriteChunkAck, _ := dc[i].WriteChunk(context.Background(), pWriteChunkReq)
			if pWriteChunkAck.Ret != 0 {
				logger.Error("flushChannel WriteChunk Failed :%v\n", pWriteChunkAck.Ret)
				cfile.cfs.Status = 1
				return
			}

		}
		mc := mp.NewMetaNodeClient(connM)
		pSyncChunkReq := &mp.SyncChunkReq{
			FileName:  cfile.Path,
			VolID:     cfile.cfs.VolID,
			ChunkInfo: v.chunkInfo,
		}
		pSyncChunkAck, _ := mc.SyncChunk(context.Background(), pSyncChunkReq)
		if pSyncChunkAck.Ret != 0 {
			cfile.cfs.Status = 1
			return
		}
		cfile.wg.Add(-1)
	}
}

func (cfile *CFile) Flush() int32 {
	return 0
}

func (cfile *CFile) Sync() int32 {
	return 0
}

func closeP(cfile *CFile) {
	cfile = nil
}
func (cfile *CFile) Close() int32 {
	if cfile.OpenFlag != O_RDONLY && cfile.OpenFlag != O_MVOPT && cfile.OpenFlag != O_TAROPT {
		cfile.close2Channel()
		cfile.wg.Wait()
	}
	defer closeP(cfile)
	return 0
}

func ProcessLocalBuffer(buffer []byte, cfile *CFile) {
	cfile.Write(buffer, int32(len(buffer)))
}

func ReadLocalAndWriteCFS(filePth string, bufSize int, hookfn func([]byte, *CFile), cfile *CFile) error {
	f, err := os.Open(filePth)
	if err != nil {
		return err
	}
	defer f.Close()
	buf := make([]byte, bufSize)
	bfRd := bufio.NewReader(f)
	for {
		n, err := bfRd.Read(buf)
		hookfn(buf[:n], cfile)
		if err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		}
	}
	return nil
}
