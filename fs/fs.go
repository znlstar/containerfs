package cfs

import (
	"bufio"
	"bytes"
	"fmt"
	"io"

	"bazil.org/fuse"
	"github.com/ipdcode/containerfs/logger"
	dp "github.com/ipdcode/containerfs/proto/dp"
	mp "github.com/ipdcode/containerfs/proto/mp"
	vp "github.com/ipdcode/containerfs/proto/vp"
	"github.com/ipdcode/containerfs/utils"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	//"math/rand"
	"os"
	"strconv"
	"sync"
	"time"
)

var VolMgrAddr string   //VolMgrAddr
var MetaNodeAddr string //MetaNodeAddr

const (
	O_RDONLY = os.O_RDONLY // 0    00000000000000000000000000000000
	O_WRONLY = os.O_WRONLY // 1    00000000000000000000000000000001
	O_RDWR   = os.O_RDWR   // 2    00000000000000000000000000000010
	O_APPEND = os.O_APPEND // 1024 00000000000000000000010000000000
	O_CREATE = os.O_CREATE // 64   00000000000000000000000001000000
	O_TRUNC  = os.O_TRUNC  // 512  00000000000000000000001000000000
	O_EXCL   = os.O_EXCL   // 0x4000
)

// chunksize and buffersize for write
const (
	chunkSize  = 64 * 1024 * 1024
	bufferSize = 256 * 1024
)

// fs
type CFS struct {
	VolID string
	//Status int // 0 ok , 1 readonly 2 invaild
}

// create volume function
func CreateVol(name string, capacity string) int32 {
	conn, err := DialVolmgr(VolMgrAddr)
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
	ctx, _ := context.WithTimeout(context.Background(), 5*time.Second)
	pCreateVolAck, err2 := vc.CreateVol(ctx, pCreateVolReq)
	if err2 != nil {
		return -1
	}
	if pCreateVolAck.Ret != 0 {
		return -1
	}

	// send to metadata to registry a new map
	conn2, err3 := DialMeta()
	if err3 != nil {
		logger.Error("CreateVol failed,Dial to metanode fail :%v\n", err3)
		return -1
	}
	defer conn2.Close()
	mc := mp.NewMetaNodeClient(conn2)
	pmCreateNameSpaceReq := &mp.CreateNameSpaceReq{
		VolID: pCreateVolAck.UUID,
		Type:  0,
	}
	ctx2, _ := context.WithTimeout(context.Background(), 5*time.Second)
	pmCreateNameSpaceAck, err4 := mc.CreateNameSpace(ctx2, pmCreateNameSpaceReq)
	if err4 != nil {
		return -1
	}
	if pmCreateNameSpaceAck.Ret != 0 {
		logger.Error("CreateNameSpace failed :%v\n", pmCreateNameSpaceAck.Ret)
		return -1
	}

	fmt.Println(pCreateVolAck.UUID)

	return 0
}

// get volume info
func GetVolInfo(name string) (int32, *vp.GetVolInfoAck) {

	conn, err := DialVolmgr(VolMgrAddr)
	if err != nil {
		logger.Error("GetVolInfo failed,Dial to volmgr fail :%v\n", err)
		return -1, nil
	}
	defer conn.Close()
	vc := vp.NewVolMgrClient(conn)
	pGetVolInfoReq := &vp.GetVolInfoReq{
		UUID: name,
	}
	ctx, _ := context.WithTimeout(context.Background(), 5*time.Second)
	pGetVolInfoAck, err2 := vc.GetVolInfo(ctx, pGetVolInfoReq)
	if err2 != nil {
		return 1, nil
	}
	if pGetVolInfoAck.Ret != 0 {
		return 1, nil
	}
	return 0, pGetVolInfoAck
}

// create volume function
func DeleteVol(uuid string) int32 {

	cfs := OpenFileSystem(uuid)
	ret, inode := cfs.Stat("/")
	if ret == 0 {
		if len(inode.ChildrenInodeIDs) > 0 {
			logger.Error("DeleteVol failed,volume has files or dirs")
			fmt.Println("DeleteVol failed,volume has files or dirs")

			return -1
		}
	} else {
		logger.Error("DeleteVol failed,stat root dir failed")
		fmt.Println("DeleteVol failed,stat root dir failed")

		return -1
	}

	// send to metadata to delete a  map
	conn2, err3 := DialMeta()
	if err3 != nil {
		logger.Error("DeleteVol failed,Dial to metanode fail :%v\n", err3)
		fmt.Printf("DeleteVol failed,Dial to metanode fail :%v\n", err3)

		return -1
	}
	defer conn2.Close()
	mc := mp.NewMetaNodeClient(conn2)
	pmDeleteNameSpaceReq := &mp.DeleteNameSpaceReq{
		VolID: uuid,
		Type:  0,
	}
	ctx, _ := context.WithTimeout(context.Background(), 5*time.Second)
	pmDeleteNameSpaceAck, err4 := mc.DeleteNameSpace(ctx, pmDeleteNameSpaceReq)
	if err4 != nil {
		return -1
	}

	if pmDeleteNameSpaceAck.Ret != 0 {
		logger.Error("DeleteNameSpace failed :%v\n", pmDeleteNameSpaceAck.Ret)
		fmt.Printf("DeleteNameSpace failed :%v\n", pmDeleteNameSpaceAck.Ret)

		return -1
	}

	conn, err := DialVolmgr(VolMgrAddr)
	if err != nil {
		logger.Error("deleteVol failed,Dial to volmgr fail :%v\n", err)
		fmt.Printf("deleteVol failed,Dial to volmgr fail :%v\n", err)

		return -1

	}
	defer conn.Close()
	vc := vp.NewVolMgrClient(conn)
	pDeleteVolReq := &vp.DeleteVolReq{
		UUID: uuid,
	}
	ctx2, _ := context.WithTimeout(context.Background(), 5*time.Second)
	pDeleteVolAck, err2 := vc.DeleteVol(ctx2, pDeleteVolReq)
	if err2 != nil {
		fmt.Printf("deleteVol failed,grpc func err :%v\n", err2)

		return -1
	}
	if pDeleteVolAck.Ret != 0 {
		fmt.Printf("deleteVol failed,grpc func ret :%v\n", pDeleteVolAck.Ret)

		return -1
	}

	return 0
}

// get filesystem info
func GetFSInfo(name string) (int32, *mp.GetFSInfoAck) {

	conn, err := DialMeta()
	if err != nil {
		logger.Error("GetFSInfo failed,Dial to metanode fail :%v\n", err)
		return -1, nil
	}
	defer conn.Close()
	mc := mp.NewMetaNodeClient(conn)
	pGetFSInfoReq := &mp.GetFSInfoReq{
		VolID: name,
	}
	ctx, _ := context.WithTimeout(context.Background(), 5*time.Second)
	pGetFSInfoAck, err2 := mc.GetFSInfo(ctx, pGetFSInfoReq)
	if err2 != nil {
		return 1, nil
	}
	if pGetFSInfoAck.Ret != 0 {
		return 1, nil
	}
	return 0, pGetFSInfoAck
}

// open a filesystem
func OpenFileSystem(UUID string) *CFS {
	cfs := CFS{VolID: UUID}
	return &cfs
}

// create dir
func (cfs *CFS) CreateDir(path string) int32 {
	conn, err := DialMeta()
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
	ctx, _ := context.WithTimeout(context.Background(), 5*time.Second)
	pCreateDirAck, err2 := mc.CreateDir(ctx, pCreateDirReq)
	if err2 != nil {
		return -1
	}

	return pCreateDirAck.Ret

}

// stat
func (cfs *CFS) Stat(path string) (int32, *mp.InodeInfo) {
	conn, err := DialMeta()
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
	ctx, _ := context.WithTimeout(context.Background(), 5*time.Second)
	pStatAck, err2 := mc.Stat(ctx, pStatReq)
	if err2 != nil {
		time.Sleep(time.Second)
		conn, err = DialMeta()
		if err != nil {
			logger.Error("Stat failed,Dial to metanode fail :%v\n", err)
			return -1, nil
		}
		mc = mp.NewMetaNodeClient(conn)
		ctx, _ := context.WithTimeout(context.Background(), 5*time.Second)
		pStatAck, err2 = mc.Stat(ctx, pStatReq)
		if err2 != nil {
			return -1, nil
		}

	}

	return pStatAck.Ret, pStatAck.InodeInfo

}

// list
func (cfs *CFS) List(path string) (int32, []*mp.InodeInfo) {
	conn, err := DialMeta()
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
	ctx, _ := context.WithTimeout(context.Background(), 5*time.Second)
	pListAck, err2 := mc.List(ctx, pListReq)
	if err2 != nil {
		return -1, nil
	}

	return pListAck.Ret, pListAck.InodeInfos

}

// delete dir
func (cfs *CFS) DeleteDir(path string) int32 {
	conn, err := DialMeta()
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
	ctx, _ := context.WithTimeout(context.Background(), 5*time.Second)
	pDeleteDirAck, err2 := mc.DeleteDir(ctx, pDeleteDirReq)
	if err2 != nil {
		return -1
	}
	return pDeleteDirAck.Ret
}

// rename
func (cfs *CFS) Rename(path1 string, path2 string) int32 {
	conn, err := DialMeta()
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
	ctx, _ := context.WithTimeout(context.Background(), 5*time.Second)
	pRenameAck, err2 := mc.Rename(ctx, pRenameReq)
	if err2 != nil {
		return -1
	}

	return pRenameAck.Ret
}

// create file
func (cfs *CFS) CreateFile(path string, flags int) (int32, *CFile) {

	if flags&O_TRUNC != 0 {
		if ret, _ := cfs.Stat(path); ret == 0 {
			cfs.DeleteFile(path)
		}
	}

	if flags&O_EXCL != 0 {
		if ret, _ := cfs.Stat(path); ret == 0 {
			return 17, nil
		}
	}

	cfile := CFile{}
	if ret := cfs.createFile(path); ret != 0 {
		return ret, nil
	}

	tmpBuffer := wBuffer{
		buffer:   new(bytes.Buffer),
		freeSize: bufferSize,
	}

	wChannel := make(chan *wBuffer, 128)
	cfile = CFile{
		Path:      path,
		OpenFlag:  flags,
		cfs:       cfs,
		FileSize:  0,
		ReaderMap: make(map[fuse.HandleID]*ReaderInfo),

		wChannel: wChannel,
		wBuffer:  tmpBuffer,
	}
	go cfile.flushChannel()

	return 0, &cfile
}

// open file
func (cfs *CFS) OpenFile(path string, flags int) (int32, *CFile) {
	var ret int32
	var writer int32 = 0
	var tmpFileSize int64 = 0

	cfile := CFile{}

	if (flags&O_WRONLY) != 0 || (flags&O_RDWR) != 0 {

		if (flags & O_APPEND) != 0 {
			chunkInfos := make([]*mp.ChunkInfoWithBG, 0)

			if ret, chunkInfos = cfs.GetFileChunks(path); ret != 0 {
				return ret, nil
			}

			if len(chunkInfos) > 0 {

				for i := range chunkInfos {
					tmpFileSize += int64(chunkInfos[i].ChunkSize)
				}
				lastChunk := chunkInfos[len(chunkInfos)-1]

				tmpBuffer := wBuffer{
					buffer:    new(bytes.Buffer),
					freeSize:  bufferSize - (lastChunk.ChunkSize % bufferSize),
					chunkInfo: lastChunk,
				}

				wChannel := make(chan *wBuffer, 128)
				cfile = CFile{
					Path:      path,
					OpenFlag:  flags,
					cfs:       cfs,
					Writer:    writer,
					FileSize:  tmpFileSize,
					wChannel:  wChannel,
					wBuffer:   tmpBuffer,
					chunks:    chunkInfos,
					ReaderMap: make(map[fuse.HandleID]*ReaderInfo),
				}
			} else {

				tmpBuffer := wBuffer{
					buffer:   new(bytes.Buffer),
					freeSize: bufferSize,
				}
				wChannel := make(chan *wBuffer, 128)
				cfile = CFile{
					Path:      path,
					OpenFlag:  flags,
					cfs:       cfs,
					Writer:    writer,
					FileSize:  0,
					wChannel:  wChannel,
					wBuffer:   tmpBuffer,
					ReaderMap: make(map[fuse.HandleID]*ReaderInfo),
				}

			}

			go cfile.flushChannel()
		} else {

			cfs.DeleteFile(path)
			if ret = cfs.createFile(path); ret != 0 {
				return ret, nil
			}

			tmpBuffer := wBuffer{
				buffer:   new(bytes.Buffer),
				freeSize: bufferSize,
			}

			wChannel := make(chan *wBuffer, 32)
			cfile = CFile{
				Path:      path,
				OpenFlag:  flags,
				cfs:       cfs,
				Writer:    writer,
				FileSize:  0,
				wChannel:  wChannel,
				wBuffer:   tmpBuffer,
				chunks:    nil,
				ReaderMap: make(map[fuse.HandleID]*ReaderInfo),
			}
			go cfile.flushChannel()
		}
	} else {
		chunkInfos := make([]*mp.ChunkInfoWithBG, 0)
		if ret, chunkInfos = cfs.GetFileChunks(path); ret != 0 {
			return ret, nil
		}
		for i := range chunkInfos {
			tmpFileSize += int64(chunkInfos[i].ChunkSize)
		}

		tmpBuffer := wBuffer{
			buffer:   new(bytes.Buffer),
			freeSize: bufferSize,
		}
		wChannel := make(chan *wBuffer, 128)

		cfile = CFile{
			Path:      path,
			OpenFlag:  flags,
			cfs:       cfs,
			Writer:    writer,
			FileSize:  tmpFileSize,
			wChannel:  wChannel,
			wBuffer:   tmpBuffer,
			chunks:    chunkInfos,
			ReaderMap: make(map[fuse.HandleID]*ReaderInfo),
		}

		go cfile.flushChannel()
	}
	return 0, &cfile
}

// update open file
func (cfs *CFS) UpdateOpenFile(cfile *CFile, flags int) int32 {

	if (flags&O_WRONLY) != 0 || (flags&O_RDWR) != 0 {

		if (flags & O_APPEND) != 0 {
			chunkInfos := make([]*mp.ChunkInfoWithBG, 0)

			var ret int32
			if ret, chunkInfos = cfs.GetFileChunks(cfile.Path); ret != 0 {
				return ret
			}

			if len(chunkInfos) > 0 {
				lastChunk := chunkInfos[len(chunkInfos)-1]
				tmpBuffer := wBuffer{
					buffer:    new(bytes.Buffer),
					freeSize:  bufferSize - (lastChunk.ChunkSize % bufferSize),
					chunkInfo: lastChunk,
				}
				cfile.wBuffer = tmpBuffer
			}
			//go cfile.flushChannel()

		} else {
			cfs.DeleteFile(cfile.Path)
			if ret := cfs.createFile(cfile.Path); ret != 0 {
				return ret
			}

			tmpBuffer := wBuffer{
				buffer:   new(bytes.Buffer),
				freeSize: bufferSize,
			}

			cfile.wBuffer = tmpBuffer
			cfile.chunks = nil
			cfile.ReaderMap = make(map[fuse.HandleID]*ReaderInfo)

			//go cfile.flushChannel()
		}
	}
	return 0
}

// create file
func (cfs *CFS) createFile(path string) int32 {

	conn, err := DialMeta()
	if err != nil {
		logger.Error("createFile failed,Dial to metanode fail :%v\n", err)
		return -1
	}
	defer conn.Close()
	mc := mp.NewMetaNodeClient(conn)
	pCreateFileReq := &mp.CreateFileReq{
		FullPathName: path,
		VolID:        cfs.VolID,
	}
	ctx, _ := context.WithTimeout(context.Background(), 5*time.Second)
	pCreateFileAck, err1 := mc.CreateFile(ctx, pCreateFileReq)
	if err1 != nil {
		time.Sleep(time.Second)
		conn, err = DialMeta()
		if err != nil {
			logger.Error("AllocateChunk failed,Dial to metanode fail :%v\n", err)
			return -1
		}
		mc = mp.NewMetaNodeClient(conn)
		ctx, _ := context.WithTimeout(context.Background(), 5*time.Second)
		pCreateFileAck, err1 = mc.CreateFile(ctx, pCreateFileReq)
		if err1 != nil {
			logger.Error("CreateFile failed,grpc func failed :%v\n", err1)
			return -1
		}
	}
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

// delete file
func (cfs *CFS) DeleteFile(path string) int32 {

	ret, chunkInfos := cfs.GetFileChunks(path)
	if ret != 0 {
		return ret
	}
	for _, v1 := range chunkInfos {
		for _, v2 := range v1.BlockGroup.BlockInfos {

			addr := utils.Inet_ntoa(v2.DataNodeIP).String() + ":" + strconv.Itoa(int(v2.DataNodePort))
			conn, err := DialData(addr)
			if err != nil {
				logger.Error("DeleteFile failed,Dial to datanode fail :%v\n", err)
				return -1
			}

			dc := dp.NewDataNodeClient(conn)

			dpDeleteChunkReq := &dp.DeleteChunkReq{
				ChunkID: v1.ChunkID,
				BlockID: v2.BlockID,
			}
			ctx, _ := context.WithTimeout(context.Background(), 5*time.Second)
			_, err2 := dc.DeleteChunk(ctx, dpDeleteChunkReq)
			if err2 != nil {
				time.Sleep(time.Second)
				conn, err = DialData(addr)
				if err != nil {
					logger.Error("DeleteChunk failed,Dial to metanode fail :%v\n", err)
				} else {
					dc = dp.NewDataNodeClient(conn)
					ctx, _ := context.WithTimeout(context.Background(), 5*time.Second)
					_, err2 = dc.DeleteChunk(ctx, dpDeleteChunkReq)
					if err2 != nil {
						logger.Error("DeleteChunk failed,grpc func failed :%v\n", err2)
					}
				}
			}

			conn.Close()
		}
	}

	conn, err := DialMeta()
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
	ctx, _ := context.WithTimeout(context.Background(), 5*time.Second)
	mpDeleteFileAck, err2 := mc.DeleteFile(ctx, mpDeleteFileReq)
	if err2 != nil {
		time.Sleep(time.Second)
		conn, err = DialMeta()
		if err != nil {
			logger.Error("DeleteChunk failed,Dial to metanode fail :%v\n", err)
			return -1
		}
		mc = mp.NewMetaNodeClient(conn)
		ctx, _ := context.WithTimeout(context.Background(), 5*time.Second)
		mpDeleteFileAck, err2 = mc.DeleteFile(ctx, mpDeleteFileReq)
		if err2 != nil {
			logger.Error("DeleteFile failed,grpc func failed :%v\n", err2)
			return -1
		}
	}

	return mpDeleteFileAck.Ret

}

// allcoate chunk
func (cfs *CFS) AllocateChunk(path string) (int32, *mp.ChunkInfoWithBG) {

	conn, err := DialMeta()
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
	ctx, _ := context.WithTimeout(context.Background(), 5*time.Second)
	pAllocateChunkAck, err2 := mc.AllocateChunk(ctx, pAllocateChunkReq)
	if err2 != nil {
		time.Sleep(time.Second)
		conn, err = DialMeta()
		if err != nil {
			logger.Error("AllocateChunk failed,Dial to metanode fail :%v\n", err)
			return -1, nil
		}
		mc = mp.NewMetaNodeClient(conn)
		ctx, _ := context.WithTimeout(context.Background(), 5*time.Second)
		pAllocateChunkAck, err2 = mc.AllocateChunk(ctx, pAllocateChunkReq)
		if err2 != nil {
			logger.Error("AllocateChunk failed,grpc func failed :%v\n", err2)
			return -1, nil
		}
	}
	if pAllocateChunkAck.Ret != 0 {
		return pAllocateChunkAck.Ret, nil
	}

	return pAllocateChunkAck.Ret, pAllocateChunkAck.ChunkInfo
}

// get file chunks
func (cfs *CFS) GetFileChunks(path string) (int32, []*mp.ChunkInfoWithBG) {
	conn, err := DialMeta()
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
	ctx, _ := context.WithTimeout(context.Background(), 5*time.Second)
	pGetFileChunksAck, err2 := mc.GetFileChunks(ctx, pGetFileChunksReq)
	if err2 != nil {
		conn, err = DialMeta()
		time.Sleep(time.Second)
		if err != nil {
			logger.Error("GetFileChunks failed,Dial to metanode fail :%v\n", err)
			return -1, nil
		}
		mc = mp.NewMetaNodeClient(conn)
		ctx, _ := context.WithTimeout(context.Background(), 5*time.Second)
		pGetFileChunksAck, err2 = mc.GetFileChunks(ctx, pGetFileChunksReq)
		if err2 != nil {
			logger.Error("GetFileChunks failed,grpc func failed :%v\n", err2)
			return -1, nil
		}
	}
	if pGetFileChunksAck.Ret != 0 {
		return pGetFileChunksAck.Ret, nil
	}
	return pGetFileChunksAck.Ret, pGetFileChunksAck.ChunkInfos
}

type wBuffer struct {
	freeSize  int32               // chunk size
	chunkInfo *mp.ChunkInfoWithBG // chunk info
	buffer    *bytes.Buffer       // chunk data
}

// reader info for read
type ReaderInfo struct {
	LastOffset int64
	readBuf    []byte
	Ch         chan *bytes.Buffer
}

// a file
type CFile struct {
	cfs      *CFS
	Path     string
	OpenFlag int
	FileSize int64
	Status   int // 0 ok

	// for write
	WMutex sync.Mutex
	Writer int32
	//FirstW bool
	wBuffer       wBuffer
	wChannel      chan *wBuffer // write channel
	wg            sync.WaitGroup
	wgWriteReps   sync.WaitGroup
	wLastDataNode [3]string

	// for read
	//lastoffset int64
	RMutex sync.Mutex
	chunks []*mp.ChunkInfoWithBG // chunkinfo
	//readBuf    []byte
	ReaderMap map[fuse.HandleID]*ReaderInfo
}

func (cfile *CFile) streamread(chunkidx int, ch chan *bytes.Buffer, offset int64, size int64) {
	var conn *grpc.ClientConn
	var err error
	var buffer *bytes.Buffer
	outflag := 0
	inflag := 0
	for i := 0; i < len(cfile.chunks[chunkidx].BlockGroup.BlockInfos); i++ {

		/*
			if cfile.chunks[chunkidx].BlockGroup.BlockInfos[i].Status != 0 {
				logger.Error("streamreadChunkReq error:%v, so retry other datanode!", "blk status error")
				outflag += 1
				continue
			}
		*/

		if cfile.chunks[chunkidx].Status[i] != 0 {
			logger.Error("streamreadChunkReq error:%v, so retry other datanode!", "blk status error")
			outflag += 1
			continue
		}

		buffer = new(bytes.Buffer)
		//r := rand.New(rand.NewSource(time.Now().UnixNano()))
		//idx = r.Intn(len(cfile.chunks[chunkidx].BlockGroup.BlockInfos))

		conn, err = DialData(utils.Inet_ntoa(cfile.chunks[chunkidx].BlockGroup.BlockInfos[i].DataNodeIP).String() + ":" + strconv.Itoa(int(cfile.chunks[chunkidx].BlockGroup.BlockInfos[i].DataNodePort)))
		if err != nil {
			logger.Error("streamread failed,Dial to datanode fail :%v\n", err)
			outflag += 1
			continue
		}

		dc := dp.NewDataNodeClient(conn)
		streamreadChunkReq := &dp.StreamReadChunkReq{
			ChunkID:  cfile.chunks[chunkidx].ChunkID,
			BlockID:  cfile.chunks[chunkidx].BlockGroup.BlockInfos[i].BlockID,
			Offset:   offset,
			Readsize: size,
		}
		ctx, _ := context.WithTimeout(context.Background(), 10*time.Second)
		stream, err := dc.StreamReadChunk(ctx, streamreadChunkReq)
		if err != nil {
			logger.Error("streamreadChunkReq error:%v, so retry other datanode!", err)
			outflag += 1
			continue
		}
		for {
			ack, err := stream.Recv()
			/*
				if err == io.EOF {
					conn.Close()
					break
				}
			*/
			if err == io.EOF {
				//logger.Error("=== Recv11 err:%v ===", err)
				break
			}
			if err != nil {
				//if err != nil {
				logger.Error("=== streamreadChunkReq Recv err:%v ===", err)
				inflag += 1
				outflag += 1
				break
			}
			if ack != nil {
				if len(ack.Databuf) == 0 {
					//logger.Error("1== This time Recv from datanode size is 0")
					continue
				} else {
					buffer.Write(ack.Databuf)
					inflag = 0
				}
			} else {
				//logger.Error("2== This time Recv from datanode size is 0")
				continue
			}

		}

		if inflag == 0 {
			ch <- buffer
			conn.Close()
			break
		} else if inflag == 3 {
			buffer = new(bytes.Buffer)
			buffer.Write([]byte{})
			logger.Debug("=== inflag:%v == buflen:%v ==", inflag, buffer.Len())
			ch <- buffer
			conn.Close()
			break
		} else if inflag < 3 {
			logger.Error("=== streamreadChunkReq Recv error, so need retry other datanode!!!")
			continue
		}
	}
	if outflag == 3 {
		buffer = new(bytes.Buffer)
		buffer.Write([]byte{})
		logger.Debug("=== outflag:%v == buflen:%v ==", outflag, buffer.Len())
		ch <- buffer
		conn.Close()
	}
}

// read
func (cfile *CFile) Read(handleId fuse.HandleID, data *[]byte, offset int64, readsize int64) int64 {
	if cfile.chunks == nil || len(cfile.chunks) == 0 {
		return -1
	}

	if offset+readsize > cfile.FileSize {
		readsize = cfile.FileSize - offset
	}

	var length int64 = 0
	var free_offset int64
	var free_size int64
	var begin_chunk_num int = 0
	var end_chunk_num int = 0
	cur_offset := offset
	for i, _ := range cfile.chunks {
		free_offset = cur_offset - int64(cfile.chunks[i].ChunkSize)
		if free_offset < 0 {
			begin_chunk_num = i
			break
		} else {
			cur_offset = free_offset
		}
	}

	cur_size := offset + readsize

	for i, _ := range cfile.chunks {
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
	if end_chunk_num < begin_chunk_num {
		logger.Error("This Read data from beginchunk:%v lager than endchunk:%v\n", begin_chunk_num, end_chunk_num)
		return 0
	}

	if begin_chunk_num > len(cfile.chunks) || end_chunk_num+1 > len(cfile.chunks) || begin_chunk_num > cap(cfile.chunks) || end_chunk_num+1 > cap(cfile.chunks) {
		return 0
	}

	for i, _ := range cfile.chunks[begin_chunk_num : end_chunk_num+1] {
		index := i + begin_chunk_num
		if cur_offset+freesize < int64(cfile.chunks[index].ChunkSize) {
			each_read_len = freesize
		} else {
			each_read_len = int64(cfile.chunks[index].ChunkSize) - cur_offset
		}
		if len(cfile.ReaderMap[handleId].readBuf) == 0 {
			buffer := new(bytes.Buffer)
			cfile.ReaderMap[handleId].Ch = make(chan *bytes.Buffer)
			go cfile.streamread(index, cfile.ReaderMap[handleId].Ch, 0, int64(cfile.chunks[index].ChunkSize))
			buffer = <-cfile.ReaderMap[handleId].Ch
			if buffer.Len() == 0 {
				logger.Error("Recv chunk:%v from datanode size:%v , but retsize is 0", index, cfile.chunks[index].ChunkSize)
				return -1
			}
			cfile.ReaderMap[handleId].readBuf = buffer.Next(buffer.Len())
			buffer.Reset()
			buffer = nil
			logger.Debug("#### Read chunk:%v == bufferlen:%v == curoffset:%v == eachlen:%v ==offset:%v == readsize:%v ####", index, len(cfile.ReaderMap[handleId].readBuf), cur_offset, each_read_len, offset, readsize)
		}

		buflen := int64(len(cfile.ReaderMap[handleId].readBuf))
		bufcap := int64(cap(cfile.ReaderMap[handleId].readBuf))

		if cur_offset > buflen || cur_offset > bufcap {
			logger.Error("== Read chunk:%v from datanode (offset:%v -- needreadsize:%v) lager than exist (buflen:%v -- bufcap:%v)\n", index, cur_offset, each_read_len, buflen, bufcap)
			return 0
		}

		if cur_offset+each_read_len > buflen {
			//logger.Error("== Read chunk:%v from datanode (offset:%v -- needreadsize:%v) lager than exist (buflen:%v -- chunksize:%v)\n", index, cur_offset, each_read_len, buflen, cfile.chunks[index].ChunkSize)
			each_read_len = buflen - cur_offset
			*data = append(*data, cfile.ReaderMap[handleId].readBuf[cur_offset:cur_offset+each_read_len]...)
		} else {
			*data = append(*data, cfile.ReaderMap[handleId].readBuf[cur_offset:cur_offset+each_read_len]...)
		}

		cur_offset += each_read_len
		if cur_offset == int64(len(cfile.ReaderMap[handleId].readBuf)) {
			//if cur_offset == int64(cfile.chunks[index].ChunkSize) {
			cur_offset = 0
			cfile.ReaderMap[handleId].readBuf = []byte{}
		}
		freesize = freesize - each_read_len
		length += each_read_len
	}
	return length
}

/*func (cfile *CFile) Seek(offset int64, whence int32) int64 {
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
*/

// write
func (cfile *CFile) Write(buf []byte, len int32) int32 {

	if cfile.Status != 0 {
		logger.Error("cfile status error , Write func return -2 ")
		return -2
	}

	cfile.WMutex.Lock()
	defer cfile.WMutex.Unlock()
	var w int32
	w = 0

	for w < len {
		if (cfile.FileSize % chunkSize) == 0 {
			logger.Debug("need a new chunk...")
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
	wBuffer := cfile.wBuffer // record cur buffer
	cfile.wg.Add(1)
	cfile.wChannel <- &wBuffer // push to channel

	return 0
}

func (cfile *CFile) close2Channel() int32 {
	/*
	   1. avoid repeat push for integer file ETC. 64MB , the last push has already done in Write func
	*/
	if cfile.wBuffer.freeSize != 0 && cfile.wBuffer.chunkInfo != nil {
		wBuffer := cfile.wBuffer // record cur buffer
		cfile.wg.Add(1)
		cfile.wChannel <- &wBuffer // push to channel
	}
	return 0
}

//DestroyChannel: destroy channel
func (cfile *CFile) DestroyChannel() {
	var wEndBuffer wBuffer
	wEndBuffer.buffer = nil
	wEndBuffer.freeSize = 0
	wEndBuffer.chunkInfo = nil
	cfile.wChannel <- &wEndBuffer // send the end flag to channel
}

func (cfile *CFile) SetChunkStatus(ip string, port int32, blkgrpid int32, blkid int32, chunkid int64, position int32, status int32) int32 {

	/*
		mpUpdateChunkInfoReq := &mp.UpdateChunkInfoReq{}

		mpUpdateChunkInfoReq.VolID = cfile.cfs.VolID
		mpUpdateChunkInfoReq.ChunkID = chunkid
		mpUpdateChunkInfoReq.Status = status

		conn1, err := DialMeta()
		if err != nil {
			logger.Error("Dial to metanode fail :%v for update chunk status\n", err)
			return -1
		}
		defer conn1.Close()
		mc := mp.NewMetaNodeClient(conn1)

		ctx, _ := context.WithTimeout(context.Background(), 5*time.Second)
		_, ret := mc.UpdateChunkInfo(ctx, mpUpdateChunkInfoReq)
		if ret != nil {
			logger.Error("mp UpdateChunkInfo...failed\n")
			return -1
		}
	*/

	vpUpdateChunkInfoReq := &vp.UpdateChunkInfoReq{}
	vpUpdateChunkInfoReq.Ip = ip
	vpUpdateChunkInfoReq.Port = port
	vpUpdateChunkInfoReq.VolID = cfile.cfs.VolID
	vpUpdateChunkInfoReq.BlockGroupID = blkgrpid
	vpUpdateChunkInfoReq.BlockID = blkid
	vpUpdateChunkInfoReq.ChunkID = chunkid
	vpUpdateChunkInfoReq.Position = position
	vpUpdateChunkInfoReq.Status = status

	conn2, err := DialVolmgr(VolMgrAddr)
	if err != nil {
		logger.Error("Dial to volmgr fail :%v for update chunk status\n", err)
		return -1
	}
	defer conn2.Close()
	vc := vp.NewVolMgrClient(conn2)

	ctx, _ := context.WithTimeout(context.Background(), 5*time.Second)
	_, ret := vc.UpdateChunkInfo(ctx, vpUpdateChunkInfoReq)
	if ret != nil {
		logger.Error("vp UpdateChunkInfo...failed\n")
		return -1
	}

	return 0
}
func (cfile *CFile) writeChunk(ip string, port int32, dc dp.DataNodeClient, req *dp.WriteChunkReq, blkgrpid int32, copies *int, position int32, chunkStatus *int32) {
	ctx, _ := context.WithTimeout(context.Background(), 5*time.Second)
	ret, err := dc.WriteChunk(ctx, req)
	if err != nil {
		cfile.SetChunkStatus(ip, port, blkgrpid, req.BlockID, req.ChunkID, position, 1)
		*chunkStatus = 1
	} else {
		if ret.Ret != 0 {
			cfile.SetChunkStatus(ip, port, blkgrpid, req.BlockID, req.ChunkID, position, 1)
			*chunkStatus = 1
		} else {
			*copies = *copies + 1
		}
	}
	cfile.wgWriteReps.Add(-1)

}

func (cfile *CFile) flushChannel() {

	// update chunkinfo to metanode
	connM, err := DialMeta()
	if err != nil {
		logger.Error("Dial failed:%v\n", err)
		logger.Error("process exit!!!")
		os.Exit(1)
	}
	defer connM.Close()

	var addr [3]string
	var connD [3]*grpc.ClientConn
	var dc [3]dp.DataNodeClient
	var curChunkID int64 = -1
	var curChunkStatus [3]int32

	copies := 0

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
			break
		}

		dataBuf := v.buffer.Next(v.buffer.Len())

		copies = 0

		if v.chunkInfo.ChunkID != curChunkID {
			curChunkStatus[0] = 0
			curChunkStatus[1] = 0
			curChunkStatus[2] = 0
			curChunkID = v.chunkInfo.ChunkID
		}

		for i := range v.chunkInfo.BlockGroup.BlockInfos {

			if curChunkStatus[i] != 0 {
				continue
			}
			ip := utils.Inet_ntoa(v.chunkInfo.BlockGroup.BlockInfos[i].DataNodeIP).String()
			port := int(v.chunkInfo.BlockGroup.BlockInfos[i].DataNodePort)

			addr[i] = ip + ":" + strconv.Itoa(port)
			if addr[i] != cfile.wLastDataNode[i] {
				if connD[i] != nil {
					connD[i].Close()
				}
				var err error
				connD[i], err = DialData(addr[i])
				if err != nil {
					logger.Error("flushChannel to datanode failed,Dial failed:%v\n", err)
					//cfile.SetBlkStatus(v.chunkInfo.BlockGroup.BlockGroupID, v.chunkInfo.BlockGroup.BlockInfos[i].BlockID, -1)
					curChunkStatus[i] = 1
					continue
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
			/*
				pWriteChunkAck, _ := dc[i].WriteChunk(context.Background(), pWriteChunkReq)
				if pWriteChunkAck.Ret != 0 {
					logger.Error("flushChannel WriteChunk Failed :%v\n", pWriteChunkAck.Ret)
					continue
				}
			*/
			cfile.wgWriteReps.Add(1)
			//go dc[i].WriteChunk(context.Background(), pWriteChunkReq)
			go cfile.writeChunk(ip, v.chunkInfo.BlockGroup.BlockInfos[i].DataNodePort, dc[i], pWriteChunkReq, v.chunkInfo.BlockGroup.BlockGroupID, &copies, int32(i), &curChunkStatus[i])

		}

		cfile.wgWriteReps.Wait()

		if copies < 2 {
			logger.Error("WriteChunk copies < 2")
			cfile.Status = 1
			break
		}

		mc := mp.NewMetaNodeClient(connM)
		pSyncChunkReq := &mp.SyncChunkReq{
			FileName: cfile.Path,
			VolID:    cfile.cfs.VolID,
		}

		var tmpChunkInfo mp.ChunkInfo
		tmpChunkInfo.ChunkSize = v.chunkInfo.ChunkSize
		tmpChunkInfo.ChunkID = v.chunkInfo.ChunkID
		tmpChunkInfo.BlockGroupID = v.chunkInfo.BlockGroup.BlockGroupID

		for i := 0; i < 3; i++ {
			tmpChunkInfo.Status = append(tmpChunkInfo.Status, curChunkStatus[i])

		}

		pSyncChunkReq.ChunkInfo = &tmpChunkInfo

		ctx, _ := context.WithTimeout(context.Background(), 5*time.Second)
		pSyncChunkAck, ret := mc.SyncChunk(ctx, pSyncChunkReq)
		if ret != nil {
			logger.Error("flushChannel SyncChunk Failed :%v\n", pSyncChunkReq.ChunkInfo)
			connM.Close()
			var err error
			time.Sleep(time.Second)
			connM, err = DialMeta()
			if err != nil {
				logger.Error("Dial failed:%v\n", err)
				cfile.Status = 1
				break
			}
			mc := mp.NewMetaNodeClient(connM)
			ctx, _ := context.WithTimeout(context.Background(), 5*time.Second)
			pSyncChunkAck, ret = mc.SyncChunk(ctx, pSyncChunkReq)
			if ret != nil {
				logger.Error("flushChannel SyncChunk Failed again:%v\n", pSyncChunkReq.ChunkInfo)
				cfile.Status = 1
				break
			}
		}
		if pSyncChunkAck.Ret != 0 {
			cfile.Status = 1
			logger.Error("flushChannel SyncChunk Failed :%v\n", pSyncChunkReq.ChunkInfo)
			break
		}

		chunkNum := len(cfile.chunks)

		v.chunkInfo.Status = tmpChunkInfo.Status

		if chunkNum == 0 {
			cfile.chunks = append(cfile.chunks, v.chunkInfo)
		} else {
			if cfile.chunks[chunkNum-1].ChunkID == v.chunkInfo.ChunkID {
				cfile.chunks[chunkNum-1].ChunkSize = v.chunkInfo.ChunkSize
				cfile.chunks[chunkNum-1].Status = v.chunkInfo.Status
			} else {
				cfile.chunks = append(cfile.chunks, v.chunkInfo)
			}
		}

		cfile.wg.Add(-1)
	}
}

// flush
func (cfile *CFile) Flush() int32 {
	return 0
}

// sync
func (cfile *CFile) Sync() int32 {
	return 0
}

// close
func (cfile *CFile) Close(flags int) int32 {
	if cfile.Status != 0 {
		logger.Error("cfile status error , Close func just return")
		return 0
	}

	if (flags&O_WRONLY) != 0 || (flags&O_RDWR) != 0 {
		cfile.close2Channel()
		cfile.wg.Wait()
	}

	return 0
}

// process local buffer
func ProcessLocalBuffer(buffer []byte, cfile *CFile) {
	cfile.Write(buffer, int32(len(buffer)))
}

// read local and write to cfs
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
}
