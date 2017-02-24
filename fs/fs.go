package cfs

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"io"
	dp "ipd.org/containerfs/proto/dp"
	mp "ipd.org/containerfs/proto/mp"
	vp "ipd.org/containerfs/proto/vp"
	"ipd.org/containerfs/utils"
	"os"
	"strconv"
	"sync"
)

const (
	volMgrAddress   = "10.8.65.94:10001"
	metaDataAddress = "10.8.65.94:10002"
)

const (
	O_RDONLY = 0
	O_WRONLY = 1
	O_RDWR   = 2
)

const (
	chunkSize = 64 * 1024 * 1024
)

type CFS struct {
	volID string
}

func CreateVol(name string, capacity string) {
	//fmt.Println("createVol...")
	// send to volmgr to allcate a new vol
	conn, err := grpc.Dial(volMgrAddress, grpc.WithInsecure())
	if err != nil {
		fmt.Printf("did not connect: %v", err)
	}
	defer conn.Close()
	vc := vp.NewVolMgrClient(conn)
	spaceQuota, _ := strconv.Atoi(capacity)
	pCreateVolReq := &vp.CreateVolReq{VolName: name, SpaceQuota: int32(spaceQuota)}
	pCreateVolAck, _ := vc.CreateVol(context.Background(), pCreateVolReq)

	fmt.Println(pCreateVolAck)

	// send to metadata to registry a new map
	conn2, err2 := grpc.Dial(metaDataAddress, grpc.WithInsecure())
	if err2 != nil {
		fmt.Printf("did not connect: %v", err2)
	}
	defer conn2.Close()
	mc := mp.NewMetaNodeClient(conn2)
	pmCreateNameSpaceReq := &mp.CreateNameSpaceReq{VolID: pCreateVolAck.UUID}
	pmCreateNameSpaceAck, _ := mc.CreateNameSpace(context.Background(), pmCreateNameSpaceReq)
	if pmCreateNameSpaceAck.Ret != 0 {
		fmt.Println("CreateNameSpace failed ...")
	}
}

func GetVolInfo(name string) {
	//fmt.Println("GetVolInfo...")
	conn, err := grpc.Dial(volMgrAddress, grpc.WithInsecure())
	if err != nil {
		fmt.Printf("did not connect: %v", err)
	}
	defer conn.Close()
	vc := vp.NewVolMgrClient(conn)
	pGetVolInfoReq := &vp.GetVolInfoReq{UUID: name}
	pGetVolInfoAck, _ := vc.GetVolInfo(context.Background(), pGetVolInfoReq)

	b, _ := json.Marshal(pGetVolInfoAck)
	fmt.Println(string(b))
}

func OpenFileSystem(UUID string) *CFS {
	cfs := CFS{volID: UUID}
	return &cfs
}

func (cfs *CFS) CreateDir(path string) {
	//fmt.Println("CreateDir...")
	conn, err := grpc.Dial(metaDataAddress, grpc.WithInsecure())
	if err != nil {
		fmt.Printf("did not connect: %v", err)
	}
	defer conn.Close()
	mc := mp.NewMetaNodeClient(conn)
	pCreateDirReq := &mp.CreateDirReq{FullPathName: path, VolID: cfs.volID}
	pCreateDirAck, _ := mc.CreateDir(context.Background(), pCreateDirReq)
	if pCreateDirAck.Ret == -1 {
		fmt.Print("failed\n")
		return
	}
	if pCreateDirAck.Ret == 1 {
		fmt.Print("not allowed\n")
		return
	}
	if pCreateDirAck.Ret == 2 {
		fmt.Print("no parent path\n")
		return
	}
	if pCreateDirAck.Ret == 17 {
		fmt.Print("already exist\n")
		return
	}

}
func (cfs *CFS) Stat(path string) int32 {
	conn, err := grpc.Dial(metaDataAddress, grpc.WithInsecure())
	if err != nil {
		fmt.Printf("did not connect: %v", err)
	}
	defer conn.Close()
	mc := mp.NewMetaNodeClient(conn)
	pStatReq := &mp.StatReq{FullPathName: path, VolID: cfs.volID}
	pStatAck, _ := mc.Stat(context.Background(), pStatReq)
	if pStatAck.Ret == 2 {
		fmt.Print("not existed\n")
		return pStatAck.Ret
	}
	b, _ := json.Marshal(pStatAck)
	fmt.Println(string(b))
	return 0
}
func (cfs *CFS) List(path string) {
	conn, err := grpc.Dial(metaDataAddress, grpc.WithInsecure())
	if err != nil {
		fmt.Printf("did not connect: %v", err)
	}
	defer conn.Close()
	mc := mp.NewMetaNodeClient(conn)
	pListReq := &mp.ListReq{FullPathName: path, VolID: cfs.volID}
	pListAck, _ := mc.List(context.Background(), pListReq)
	if pListAck.Ret == 2 {
		fmt.Print("not existed\n")
		return
	}
	for _, value := range pListAck.InodeInfos {
		fmt.Println(value.Name)
	}
}
func (cfs *CFS) ListAll(path string) {
	conn, err := grpc.Dial(metaDataAddress, grpc.WithInsecure())
	if err != nil {
		fmt.Printf("did not connect: %v", err)
	}
	defer conn.Close()
	mc := mp.NewMetaNodeClient(conn)
	pListReq := &mp.ListReq{FullPathName: path, VolID: cfs.volID}
	pListAck, _ := mc.List(context.Background(), pListReq)
	if pListAck.Ret == 2 {
		fmt.Print("not existed\n")
		return
	}
	for _, value := range pListAck.InodeInfos {
		fmt.Println(value)
	}

}
func (cfs *CFS) DeleteDir(path string) {
	conn, err := grpc.Dial(metaDataAddress, grpc.WithInsecure())
	if err != nil {
		fmt.Printf("did not connect: %v", err)
	}
	defer conn.Close()
	mc := mp.NewMetaNodeClient(conn)
	pDeleteDirReq := &mp.DeleteDirReq{FullPathName: path, VolID: cfs.volID}
	pDeleteDirAck, _ := mc.DeleteDir(context.Background(), pDeleteDirReq)
	if pDeleteDirAck.Ret == 1 {
		fmt.Println("not allowed")
	}
}

func (cfs *CFS) Rename(path1 string, path2 string) {
	conn, err := grpc.Dial(metaDataAddress, grpc.WithInsecure())
	if err != nil {
		fmt.Printf("did not connect: %v", err)
	}
	defer conn.Close()
	mc := mp.NewMetaNodeClient(conn)
	pRenameReq := &mp.RenameReq{FullPathName1: path1, FullPathName2: path2, VolID: cfs.volID}
	pRenameAck, _ := mc.Rename(context.Background(), pRenameReq)
	if pRenameAck.Ret == 2 {
		fmt.Println("not existed")
	}
	if pRenameAck.Ret == 1 {
		fmt.Println("not allowed")
	}
}

func (cfs *CFS) OpenFile(path string, flags int32) (int32, *CFile) {
	var ret int32
	var writer int32 = 0

	chunkInfos := make([]*mp.ChunkInfo, 0)

	if flags == O_RDONLY {
		if ret, chunkInfos = cfs.GetFileChunks(path); ret != 0 {
			return ret, nil
		}
	}
	if flags == O_WRONLY {
		writer = 1
		if ret = cfs.CreateFile(path); ret != 0 {
			return ret, nil
		}
	}

	wChunk := Chunk{freeSize: 0}
	wChannel := make(chan *Chunk, 16)
	cfile := CFile{Path: path, OpenFlag: flags, cfs: cfs, Writer: writer, chunks: chunkInfos, wChannel: wChannel, wChunk: wChunk, lastoffset: 0}
	return 0, &cfile
}

func (cfs *CFS) CreateFile(path string) int32 {
	conn, err := grpc.Dial(metaDataAddress, grpc.WithInsecure())
	if err != nil {
		fmt.Printf("did not connect: %v", err)
	}
	defer conn.Close()
	mc := mp.NewMetaNodeClient(conn)
	pCreateFileReq := &mp.CreateFileReq{FullPathName: path, VolID: cfs.volID}
	pCreateFileAck, _ := mc.CreateFile(context.Background(), pCreateFileReq)
	if pCreateFileAck.Ret == 1 {
		fmt.Print("not allowed\n")
		return 1
	}
	if pCreateFileAck.Ret == 2 {
		fmt.Print("no parent path\n")
		return 2
	}
	if pCreateFileAck.Ret == 17 {
		fmt.Print("already exist\n")
		return 17
	}
	return 0
}

func (cfs *CFS) AllocateChunk(path string) (int32, *mp.ChunkInfo) {
	conn, err := grpc.Dial(metaDataAddress, grpc.WithInsecure())
	if err != nil {
		fmt.Printf("did not connect: %v", err)
	}
	defer conn.Close()
	mc := mp.NewMetaNodeClient(conn)
	pAllocateChunkReq := &mp.AllocateChunkReq{FileName: path, VolID: cfs.volID}
	fmt.Println("pAllocateChunkReq : ")
	fmt.Println(pAllocateChunkReq)
	pAllocateChunkAck, _ := mc.AllocateChunk(context.Background(), pAllocateChunkReq)
	if pAllocateChunkAck.Ret != 0 {
		fmt.Print("AllocateChunk Failed!\n")
		return pAllocateChunkAck.Ret, nil
	}

	return pAllocateChunkAck.Ret, pAllocateChunkAck.ChunkInfo
}

func (cfs *CFS) GetFileChunks(path string) (int32, []*mp.ChunkInfo) {
	conn, err := grpc.Dial(metaDataAddress, grpc.WithInsecure())
	if err != nil {
		fmt.Printf("did not connect: %v", err)
	}
	defer conn.Close()
	mc := mp.NewMetaNodeClient(conn)
	pGetFileChunksReq := &mp.GetFileChunksReq{FileName: path, VolID: cfs.volID}
	pGetFileChunksAck, _ := mc.GetFileChunks(context.Background(), pGetFileChunksReq)
	if pGetFileChunksAck.Ret != 0 {
		fmt.Print("GetFileChunks Failed!\n")
		return pGetFileChunksAck.Ret, nil
	}
	return pGetFileChunksAck.Ret, pGetFileChunksAck.ChunkInfos
}

type Chunk struct {
	freeSize  int32         // chunk size
	chunkInfo *mp.ChunkInfo // chunk info
	buffer    *bytes.Buffer // chunk data
}
type CFile struct {
	cfs      *CFS
	Path     string
	OpenFlag int32

	// for write
	WMutex sync.Mutex
	Writer int32
	//FirstW bool
	//lastSeq  int32       // last sequence number
	wChunk   Chunk
	wChannel chan *Chunk // write channel

	// for read
	lastoffset int64
	RMutex     sync.Mutex
	//rChannel    chan []byte
	chunks []*mp.ChunkInfo // chunkinfo
}

func (cfile *CFile) Read(buf *[]byte, read_size int64) int64 {
	if cfile.OpenFlag != O_RDONLY {
		fmt.Println("Openflag bad parameter!\n")
		return -1
	}

	cfile.RMutex.Lock()
	defer cfile.RMutex.Unlock()

	var ret int64 = 0
	var filesize int64 = 0

        for i := range cfile.chunks {
                filesize += int64(cfile.chunks[i].ChunkSize)
        }

	if cfile.lastoffset == filesize {
                return 0
        }	

	ret = cfile.Pread(buf, read_size, cfile.lastoffset)
	if ret >= 0 {
		cfile.lastoffset += ret
	}
	return ret
}

func (cfile *CFile) Pread(buf *[]byte, read_size int64, offset int64) int64 {
	if buf == nil || read_size < 0 || offset < 0 {
		fmt.Println("Pread bad parameter!\n")
		return -1
	}

	if cfile.lastoffset == -1 || cfile.lastoffset != offset {
		//cfile.sequential_ratio /= 2
		fmt.Println("Pread miss last read offset!\n")
		return -1
	}

	var free_offset int64
	var cur_chunk_num int = 0
	cur_offset := cfile.lastoffset
	for i := range cfile.chunks {
		free_offset = cur_offset - int64(cfile.chunks[i].ChunkSize)
		if free_offset < 0 {
			cur_chunk_num = i
			break
		} else {
			cur_offset = free_offset
		}
	}

	var ret_len int64
	var each_read_len int64
	var total_buf bytes.Buffer
	read_len := read_size

	
	for i,_ := range cfile.chunks[cur_chunk_num:] {
		cur_chunk_index := i + cur_chunk_num
		if cur_offset+read_len < int64(cfile.chunks[cur_chunk_index].ChunkSize) {
			each_read_len = read_len
		} else {
			each_read_len = int64(cfile.chunks[cur_chunk_index].ChunkSize) - cur_offset
		}
		read_len = read_len - each_read_len

		for j,_ := range cfile.chunks[cur_chunk_index].BlockGroup.BlockInfos {
			conn, err := grpc.Dial(utils.Inet_ntoa(cfile.chunks[cur_chunk_index].BlockGroup.BlockInfos[j].DataNodeIP).String()+":"+strconv.Itoa(int(cfile.chunks[cur_chunk_index].BlockGroup.BlockInfos[j].DataNodePort)), grpc.WithInsecure())
			if err != nil {
				fmt.Printf("Connect Datanode error: %v\n", err)
				continue
			}
			dc := dp.NewDataNodeClient(conn)
			readChunkReq := &dp.ReadChunkReq{ChunkID: cfile.chunks[cur_chunk_index].ChunkID, BlockID: cfile.chunks[cur_chunk_index].BlockGroup.BlockInfos[j].BlockID, Readlen: each_read_len, Offset: cur_offset}
			readChunkAck, _ := dc.ReadChunk(context.Background(), readChunkReq)
			if readChunkAck.Ret < 0 {
				fmt.Println("Read data from chunkserver return error, retry the replic chunk!\n")
				conn.Close()
			} else {
				total_buf.WriteString(readChunkAck.Databuf)
				ret_len += readChunkAck.Readsize
				//fmt.Printf("###totalchunk:%v ### curchunk:%v ### chunkid:%v ## chunksize:%v ## n_size:%v ## ret_size:%v ## offset:%v ### this time bufsize:%v ### have need read size:%v ####\n",len(cfile.chunks),cur_chunk_num,cfile.chunks[cur_chunk_index].ChunkID, cfile.chunks[cur_chunk_index].ChunkSize, each_read_len, readChunkAck.Readsize, cur_offset,read_size,read_len)
				conn.Close()
				break
			}
		}
		cur_offset += each_read_len
		if cur_offset == int64(cfile.chunks[cur_chunk_index].ChunkSize) {
			cur_offset = 0
		}
		str := total_buf.String()
		*buf = utils.S2B(&str)
		if read_len == 0 && ret_len == read_size {
			return ret_len
		}
	}
	
	return ret_len
}

func (cfile *CFile) Seek(offset int64, whence int32) int64 {
	return 0
}

func (cfile *CFile) Write(buf []byte, len int32) int32 {
	cfile.WMutex.Lock()
	defer cfile.WMutex.Unlock()
	var w int32
	w = 0
	for w < len {
		if cfile.wChunk.freeSize == 0 {
			fmt.Println("need a new chunk !")
			cfile.wChunk = Chunk{buffer: new(bytes.Buffer), freeSize: chunkSize}
			var ret int32
			ret, cfile.wChunk.chunkInfo = cfile.cfs.AllocateChunk(cfile.Path)
			if ret != 0 {
				return ret
			}
		}
		if len-w < cfile.wChunk.freeSize {
			cfile.wChunk.buffer.Write(buf[w:len])
			cfile.wChunk.freeSize = cfile.wChunk.freeSize - (len - w)
			w = len
			break
		} else {
			cfile.wChunk.buffer.Write(buf[w:cfile.wChunk.freeSize])
			w = w + cfile.wChunk.freeSize
			cfile.wChunk.freeSize = 0
		}

		if cfile.wChunk.freeSize == 0 {
			cfile.WriteChunk()
		}
	}
	return w
}
func (cfile *CFile) WriteChunk() int32 {
	cfile.wChunk.chunkInfo.ChunkSize = chunkSize - cfile.wChunk.freeSize
	wChunk := cfile.wChunk    // record cur chunk
	cfile.wChannel <- &wChunk // push to channel
	cfile.wChunk.freeSize = 0 // disable cur chunk
	cfile.flushChannel()
	return 0
}
func (cfile *CFile) flushChannel() {
	//var ret int32
	fmt.Println("flushChannel ... ")

	for v := range cfile.wChannel {
		// write chunk to 3 datanode
		//fmt.Println("v:")
		//fmt.Println(v)
		//fmt.Println(v.chunkInfo.BlockGroup.BlockInfos)
		for i := range v.chunkInfo.BlockGroup.BlockInfos {
			fmt.Println("write chunk to datanode 1...")
			addr := utils.Inet_ntoa(v.chunkInfo.BlockGroup.BlockInfos[i].DataNodeIP).String() + ":" + strconv.Itoa(int(v.chunkInfo.BlockGroup.BlockInfos[i].DataNodePort))
			fmt.Println("addr:")
			fmt.Println(addr)
			conn, err := grpc.Dial(addr, grpc.WithInsecure())
			if err != nil {
				fmt.Printf("did not connect: %v", err)
			}
			defer conn.Close()
			dc := dp.NewDataNodeClient(conn)

			dataBuf := v.buffer.String()
			dataLen := len(dataBuf)
			fmt.Println(dataLen)
			blockID := v.chunkInfo.BlockGroup.BlockInfos[i].BlockID
			chunkID := v.chunkInfo.ChunkID
			var cur int = 0
			for cur < dataLen {
				if dataLen-cur < 1024*1024 {
					pWriteChunkReq := &dp.WriteChunkReq{ChunkID: chunkID, BlockID: blockID, Databuf: dataBuf[cur:dataLen]}
					pWriteChunkAck, _ := dc.WriteChunk(context.Background(), pWriteChunkReq)
					if pWriteChunkAck.Ret != 0 {
						fmt.Print("WriteChunk Failed\n")
						return
					}
					cur = dataLen
				} else {
					pWriteChunkReq := &dp.WriteChunkReq{ChunkID: chunkID, BlockID: blockID, Databuf: dataBuf[cur : cur+1024*1024]}
					pWriteChunkAck, _ := dc.WriteChunk(context.Background(), pWriteChunkReq)
					if pWriteChunkAck.Ret != 0 {
						fmt.Print("WriteChunk Failed\n")
						return
					}
					cur = cur + 1024*1024
				}

			}
		}

		fmt.Println("update chunkinfo to metanode...")
		// update chunkinfo to metanode
		conn, err := grpc.Dial(metaDataAddress, grpc.WithInsecure())
		if err != nil {
			fmt.Printf("did not connect: %v", err)
		}
		defer conn.Close()
		mc := mp.NewMetaNodeClient(conn)
		pSyncChunkReq := &mp.SyncChunkReq{FileName: cfile.Path, VolID: cfile.cfs.volID, ChunkInfo: v.chunkInfo}
		pSyncChunkAck, _ := mc.SyncChunk(context.Background(), pSyncChunkReq)
		if pSyncChunkAck.Ret != 0 {
			fmt.Print("SyncChunk Failed!\n")
			return
		}
		return
	}
	return
}

func (cfile *CFile) Flush() int32 {
	return 0
}

func (cfile *CFile) Sync() int32 {
	return 0
}

func (cfile *CFile) Close() int32 {
	fmt.Println("Close  ... ")

	if cfile.OpenFlag == O_WRONLY {
		fmt.Println("WriteChunk ... ")
		cfile.WriteChunk()
	}
	return 0
}

func ProcessLocalBuffer(buffer []byte, cfile *CFile) {
	//fmt.Println("ProcessLocalBuffer...")
	cfile.Write(buffer, int32(len(buffer)))
}

func ReadLocalAndWriteCFS(filePth string, bufSize int, hookfn func([]byte, *CFile), cfile *CFile) error {
	fmt.Println("ReadLocalAndWriteCFS...")
	f, err := os.Open(filePth)
	if err != nil {
		return err
	}
	defer f.Close()
	buf := make([]byte, bufSize) //一次读取多少个字节
	bfRd := bufio.NewReader(f)
	for {
		n, err := bfRd.Read(buf)
		hookfn(buf[:n], cfile) // n 是成功读取字节数
		if err != nil {        //遇到任何错误立即返回，并忽略 EOF 错误信息
			if err == io.EOF {
				return nil
			}
			return err
		}
	}
	return nil
}
