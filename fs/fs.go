package cfs

import (
	"bazil.org/fuse"
	"bufio"
	"bytes"
	"fmt"
	"github.com/ipdcode/containerfs/logger"
	dp "github.com/ipdcode/containerfs/proto/dp"
	mp "github.com/ipdcode/containerfs/proto/mp"
	vp "github.com/ipdcode/containerfs/proto/vp"
	"github.com/ipdcode/containerfs/utils"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"io"
	"math/rand"
	"os"
	"strconv"
	"sync"
	"time"
)

// MetaNodePeers ...
var MetaNodePeers []string

// VolMgrAddr ...
var VolMgrAddr string

//MetaNodeAddr ...
var MetaNodeAddr string

// chunksize and buffersize for write
const (
	chunkSize  = 64 * 1024 * 1024
	bufferSize = 1024 * 1024
)

// CFS ...
type CFS struct {
	VolID string
	//Status int // 0 ok , 1 readonly 2 invaild
}

// CreateVol volume function
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
	ctx, _ := context.WithTimeout(context.Background(), 20*time.Second)
	pCreateVolAck, err2 := vc.CreateVol(ctx, pCreateVolReq)
	if err2 != nil {
		return -1
	}
	if pCreateVolAck.Ret != 0 {
		return -1
	}

	// send to metadata to registry a new map

	conn2, err3 := grpc.Dial(MetaNodeAddr, grpc.WithInsecure(), grpc.FailOnNonTempDialError(true))
	if err3 != nil {
		logger.Error("CreateVol failed,Dial to metanode fail :%v\n", err3)
		return -1
	}
	defer conn2.Close()
	mc := mp.NewMetaNodeClient(conn2)
	pmCreateNameSpaceReq := &mp.CreateNameSpaceReq{
		VolID:       pCreateVolAck.UUID,
		RaftGroupID: pCreateVolAck.RaftGroupID,
		Type:        0,
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

// GetVolInfo volume info
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

// SnapShootVol ...
func SnapShootVol(uuid string) int32 {
	// send to metadata to delete a  map
	conn, err := DialMeta(uuid)
	if err != nil {
		logger.Error("SnapShootVol failed,Dial to metanode fail :%v\n", err)
		fmt.Printf("SnapShootVol failed,Dial to metanode fail :%v\n", err)

		return -1
	}
	defer conn.Close()
	mc := mp.NewMetaNodeClient(conn)
	pmSnapShootNameSpaceReq := &mp.SnapShootNameSpaceReq{
		VolID: uuid,
		Type:  0,
	}
	ctx, _ := context.WithTimeout(context.Background(), 5*time.Second)
	pmSnapShootNameSpaceAck, err4 := mc.SnapShootNameSpace(ctx, pmSnapShootNameSpaceReq)
	if err4 != nil {
		return -1
	}

	if pmSnapShootNameSpaceAck.Ret != 0 {
		logger.Error("DeleteNameSpace failed :%v\n", pmSnapShootNameSpaceAck.Ret)
		fmt.Printf("DeleteNameSpace failed :%v\n", pmSnapShootNameSpaceAck.Ret)
		return -1
	}
	return 0
}

// DeleteVol function
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
	conn2, err3 := DialMeta(uuid)
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

// GetFSInfo ...
func GetFSInfo(name string) (int32, *mp.GetFSInfoAck) {

	conn, err := DialMeta(name)
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

// OpenFileSystem ...
func OpenFileSystem(UUID string) *CFS {
	cfs := CFS{VolID: UUID}
	return &cfs
}

// CreateDir ...
func (cfs *CFS) CreateDir(path string) int32 {
	conn, err := DialMeta(cfs.VolID)
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

// Stat ...
func (cfs *CFS) Stat(path string) (int32, *mp.InodeInfo) {
	conn, err := DialMeta(cfs.VolID)
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
		conn, err = DialMeta(cfs.VolID)
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

// List ...
func (cfs *CFS) List(path string) (int32, []*mp.InodeInfo) {
	conn, err := DialMeta(cfs.VolID)
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

// DeleteDir ...
func (cfs *CFS) DeleteDir(path string) int32 {
	conn, err := DialMeta(cfs.VolID)
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

// Rename ...
func (cfs *CFS) Rename(path1 string, path2 string) int32 {
	conn, err := DialMeta(cfs.VolID)
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

// CreateFile ...
func (cfs *CFS) CreateFile(path string, flags int) (int32, *CFile) {

	if flags&os.O_TRUNC != 0 {
		if ret, _ := cfs.Stat(path); ret == 0 {
			cfs.DeleteFile(path)
		}
	}

	if flags&os.O_EXCL != 0 {
		if ret, _ := cfs.Stat(path); ret == 0 {
			return 17, nil
		}
	}

	cfile := CFile{}
	if ret := cfs.createFile(path); ret != 0 {
		return ret, nil
	}

	conn, err := DialMeta(cfs.VolID)
	if err != nil {
		return -1, nil
	}

	tmpBuffer := wBuffer{
		buffer:   new(bytes.Buffer),
		freeSize: bufferSize,
	}

	cfile = CFile{
		Path:      path,
		OpenFlag:  flags,
		cfs:       cfs,
		FileSize:  0,
		ReaderMap: make(map[fuse.HandleID]*ReaderInfo),

		wBuffer: tmpBuffer,
		ConnM:   conn,
	}
	//go cfile.send()

	return 0, &cfile
}

// OpenFile ...
func (cfs *CFS) OpenFile(path string, flags int) (int32, *CFile) {
	var ret int32
	var writer int32
	var tmpFileSize int64

	cfile := CFile{}

	if (flags&os.O_WRONLY) != 0 || (flags&os.O_RDWR) != 0 {

		conn, err := DialMeta(cfs.VolID)
		if err != nil {
			return -1, nil
		}
		if (flags & os.O_APPEND) != 0 {
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

				cfile = CFile{
					Path:      path,
					OpenFlag:  flags,
					cfs:       cfs,
					Writer:    writer,
					FileSize:  tmpFileSize,
					wBuffer:   tmpBuffer,
					chunks:    chunkInfos,
					ReaderMap: make(map[fuse.HandleID]*ReaderInfo),
					ConnM:     conn,
				}

			} else {

				tmpBuffer := wBuffer{
					buffer:   new(bytes.Buffer),
					freeSize: bufferSize,
				}
				cfile = CFile{
					Path:      path,
					OpenFlag:  flags,
					cfs:       cfs,
					Writer:    writer,
					FileSize:  0,
					wBuffer:   tmpBuffer,
					ReaderMap: make(map[fuse.HandleID]*ReaderInfo),
					ConnM:     conn,
				}

			}

			//go cfile.send()
		} else {

			cfs.DeleteFile(path)
			if ret = cfs.createFile(path); ret != 0 {
				return ret, nil
			}

			tmpBuffer := wBuffer{
				buffer:   new(bytes.Buffer),
				freeSize: bufferSize,
			}

			cfile = CFile{
				Path:      path,
				OpenFlag:  flags,
				cfs:       cfs,
				Writer:    writer,
				FileSize:  0,
				wBuffer:   tmpBuffer,
				chunks:    nil,
				ReaderMap: make(map[fuse.HandleID]*ReaderInfo),
				ConnM:     conn,
			}
			//go cfile.send()
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

		cfile = CFile{
			Path:      path,
			OpenFlag:  flags,
			cfs:       cfs,
			Writer:    writer,
			FileSize:  tmpFileSize,
			wBuffer:   tmpBuffer,
			chunks:    chunkInfos,
			ReaderMap: make(map[fuse.HandleID]*ReaderInfo),
		}

		//go cfile.send()
	}
	return 0, &cfile
}

// UpdateOpenFile ...
func (cfs *CFS) UpdateOpenFile(cfile *CFile, flags int) int32 {

	if (flags&os.O_WRONLY) != 0 || (flags&os.O_RDWR) != 0 {

		conn, err := DialMeta(cfs.VolID)
		if err != nil {
			return -1
		}

		cfile.ConnM = conn

		if (flags & os.O_APPEND) != 0 {

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
			//go cfile.send()

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

			//go cfile.send()
		}
	}
	return 0
}

// createFile ...
func (cfs *CFS) createFile(path string) int32 {

	conn, err := DialMeta(cfs.VolID)
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
		conn, err = DialMeta(cfs.VolID)
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

// DeleteFile ...
func (cfs *CFS) DeleteFile(path string) int32 {

	ret, chunkInfos := cfs.GetFileChunks(path)
	if ret != 0 {
		return ret
	}
	for _, v1 := range chunkInfos {
		for _, v2 := range v1.BlockGroup.BlockInfos {

			addr := utils.InetNtoa(v2.DataNodeIP).String() + ":" + strconv.Itoa(int(v2.DataNodePort))
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

	conn, err := DialMeta(cfs.VolID)
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
		conn, err = DialMeta(cfs.VolID)
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

// AllocateChunk ...
func (cfs *CFS) AllocateChunk(path string) (int32, *mp.ChunkInfoWithBG) {

	conn, err := DialMeta(cfs.VolID)
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
		conn, err = DialMeta(cfs.VolID)
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

// GetFileChunks ...
func (cfs *CFS) GetFileChunks(path string) (int32, []*mp.ChunkInfoWithBG) {
	conn, err := DialMeta(cfs.VolID)
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
		conn, err = DialMeta(cfs.VolID)
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
	freeSize    int32               // chunk size
	chunkInfo   *mp.ChunkInfoWithBG // chunk info
	buffer      *bytes.Buffer       // chunk data
	startOffset int64
	endOffset   int64
}

// ReaderInfo ...
type ReaderInfo struct {
	LastOffset int64
	readBuf    []byte
	Ch         chan *bytes.Buffer
}

// CFile ...
type CFile struct {
	cfs      *CFS
	Path     string
	OpenFlag int
	FileSize int64
	Status   int32 // 0 ok

	// for write
	//WMutex sync.Mutex
	Writer int32
	//FirstW bool
	wBuffer        wBuffer
	wgWriteReps    sync.WaitGroup
	ConnM          *grpc.ClientConn
	wLastDataNode  [3]string
	ConnD          [3]*grpc.ClientConn
	Dc             [3]dp.DataNodeClient
	CurChunkID     int64
	CurChunkStatus [3]int32

	// for read
	//lastoffset int64
	RMutex sync.Mutex
	chunks []*mp.ChunkInfoWithBG // chunkinfo
	//readBuf    []byte
	ReaderMap      map[fuse.HandleID]*ReaderInfo
}

func generateRandomNumber(start int, end int, count int) []int {
	if end < start || (end-start) < count {
		return nil
	}

	nums := make([]int, 0)
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	for len(nums) < count {
		num := r.Intn((end - start)) + start

		exist := false
		for _, v := range nums {
			if v == num {
				exist = true
				break
			}
		}

		if !exist {
			nums = append(nums, num)
		}
	}

	return nums
}

func (cfile *CFile) streamread(chunkidx int, ch chan *bytes.Buffer, offset int64, size int64) {
	var conn *grpc.ClientConn
	var err error
	var buffer *bytes.Buffer
	outflag := 0
	inflag := 0
	idxs := generateRandomNumber(0, 3, 3)

	for n := 0; n < len(cfile.chunks[chunkidx].BlockGroup.BlockInfos); n++ {
		i := idxs[n]
		if cfile.chunks[chunkidx].Status[i] != 0 {
			logger.Error("streamreadChunkReq chunk status:%v error, so retry other datanode!", cfile.chunks[chunkidx].Status[i])
			outflag++
			continue
		}

		buffer = new(bytes.Buffer)
		//r := rand.New(rand.NewSource(time.Now().UnixNano()))
		//idx = r.Intn(len(cfile.chunks[chunkidx].BlockGroup.BlockInfos))

		conn, err = DialData(utils.InetNtoa(cfile.chunks[chunkidx].BlockGroup.BlockInfos[i].DataNodeIP).String() + ":" + strconv.Itoa(int(cfile.chunks[chunkidx].BlockGroup.BlockInfos[i].DataNodePort)))
		if err != nil {
			logger.Error("streamread failed,Dial to datanode fail :%v", err)
			outflag++
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
			outflag++
			continue
		}
		for {
			ack, err := stream.Recv()

			if err == io.EOF {
				break
			}
			if err != nil {
				logger.Error("=== streamreadChunkReq Recv err:%v ===", err)
				inflag++
				outflag++
				break
			}
			if ack != nil {
				if len(ack.Databuf) == 0 {
					continue
				} else {
					buffer.Write(ack.Databuf)
					inflag = 0
				}
			} else {
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
			logger.Error("Stream Read the chunk three copy Recv error")
			ch <- buffer
			conn.Close()
			break
		} else if inflag < 3 {
			logger.Error("Stream Read the chunk %v copy Recv error, so need retry other datanode!!!", inflag)
			continue
		}
	}
	if outflag == 3 {
		buffer = new(bytes.Buffer)
		buffer.Write([]byte{})
		logger.Error("Stream Read the chunk three copy Datanode error")
		ch <- buffer
		conn.Close()
	}
}

// Read ...
func (cfile *CFile) Read(handleID fuse.HandleID, data *[]byte, offset int64, readsize int64) int64 {
	// read data from write buffer

	cache := cfile.wBuffer
	n := cache.buffer.Len()
	if n != 0 && offset >= cache.startOffset {
		cfile.ReaderMap[handleID].readBuf = cache.buffer.Bytes()
		if offset+readsize < cache.endOffset {
			*data = append(*data, cfile.ReaderMap[handleID].readBuf[offset:offset+readsize]...)
			return readsize
		} else {
			*data = append(*data, cfile.ReaderMap[handleID].readBuf[offset:cache.endOffset]...)
			return cache.endOffset - offset
		}
	}

	if cfile.chunks == nil || len(cfile.chunks) == 0 {
		logger.Error("Read File but Chunks not exist")
		return -1
	}

	if offset+readsize > cfile.FileSize {
		readsize = cfile.FileSize - offset
	}

	var length int64
	var freeOffset int64
	var freeSize int64
	var beginChunkNum int
	var endChunkNum int
	curOffset := offset
	for i, v := range cfile.chunks {
		freeOffset = curOffset - int64(v.ChunkSize)
		if freeOffset < 0 {
			beginChunkNum = i
			break
		} else {
			curOffset = freeOffset
		}
	}

	curSize := offset + readsize

	for i, v := range cfile.chunks {
		freeSize = curSize - int64(v.ChunkSize)
		if freeSize <= 0 {
			endChunkNum = i
			break
		} else {
			curSize = freeSize
		}
	}

	var eachReadLen int64
	freesize := readsize
	if endChunkNum < beginChunkNum {
		logger.Error("This Read data from beginchunk:%v lager than endchunk:%v", beginChunkNum, endChunkNum)
		return -1
	}

	if beginChunkNum > len(cfile.chunks) || endChunkNum+1 > len(cfile.chunks) || beginChunkNum > cap(cfile.chunks) || endChunkNum+1 > cap(cfile.chunks) {
		logger.Error("Read begin or end chunk num not right")
		return -1
	}

	//for i, _ := range cfile.chunks[beginChunkNum : endChunkNum+1] {
	for i := 0; i < len(cfile.chunks[beginChunkNum:endChunkNum+1]); i++ {
		index := i + beginChunkNum
		if curOffset+freesize < int64(cfile.chunks[index].ChunkSize) {
			eachReadLen = freesize
		} else {
			eachReadLen = int64(cfile.chunks[index].ChunkSize) - curOffset
		}
		if len(cfile.ReaderMap[handleID].readBuf) == 0 {
			buffer := new(bytes.Buffer)
			cfile.ReaderMap[handleID].Ch = make(chan *bytes.Buffer)
			go cfile.streamread(index, cfile.ReaderMap[handleID].Ch, 0, int64(cfile.chunks[index].ChunkSize))
			buffer = <-cfile.ReaderMap[handleID].Ch
			if buffer.Len() == 0 {
				logger.Error("Recv chunk:%v from datanode size:%v , but retsize is 0", index, cfile.chunks[index].ChunkSize)
				return -1
			}
			cfile.ReaderMap[handleID].readBuf = buffer.Next(buffer.Len())
			buffer.Reset()
			buffer = nil
			//logger.Debug("#### Read chunk:%v == bufferlen:%v == curoffset:%v == eachlen:%v ==offset:%v == readsize:%v ####", index, len(cfile.ReaderMap[handleID].readBuf), curOffset, eachReadLen, offset, readsize)
		}

		buflen := int64(len(cfile.ReaderMap[handleID].readBuf))
		bufcap := int64(cap(cfile.ReaderMap[handleID].readBuf))

		if curOffset > buflen || curOffset > bufcap {
			logger.Error("== Read chunk:%v from datanode (offset:%v -- needreadsize:%v) lager than exist (buflen:%v -- bufcap:%v)\n", index, curOffset, eachReadLen, buflen, bufcap)
			return -1
		}

		if curOffset+eachReadLen > buflen {
			eachReadLen = buflen - curOffset
			*data = append(*data, cfile.ReaderMap[handleID].readBuf[curOffset:curOffset+eachReadLen]...)
		} else {
			*data = append(*data, cfile.ReaderMap[handleID].readBuf[curOffset:curOffset+eachReadLen]...)
		}

		curOffset += eachReadLen
		if curOffset == int64(len(cfile.ReaderMap[handleID].readBuf)) {
			curOffset = 0
			cfile.ReaderMap[handleID].readBuf = []byte{}
		}
		freesize = freesize - eachReadLen
		length += eachReadLen
	}
	return length
}

// Write ...
func (cfile *CFile) Write(buf []byte, len int32) int32 {

	if cfile.Status != 0 {
		logger.Error("cfile status error , Write func return -2 ")
		return -2
	}

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
				}
				return -2
			}

		}
		if cfile.wBuffer.freeSize == 0 {
			cfile.wBuffer.buffer = new(bytes.Buffer)
			cfile.wBuffer.freeSize = bufferSize
		}
		if len-w < cfile.wBuffer.freeSize {
			if len != w {
				cfile.wBuffer.buffer.Write(buf[w:len])
				cfile.wBuffer.freeSize = cfile.wBuffer.freeSize - (len - w)
				cfile.wBuffer.startOffset = cfile.FileSize
				cfile.FileSize = cfile.FileSize + int64(len-w)
				cfile.wBuffer.endOffset = cfile.FileSize
				cfile.wBuffer.chunkInfo.ChunkSize = cfile.wBuffer.chunkInfo.ChunkSize + int32(len-w)
				w = len
			}
			break
		} else {
			cfile.wBuffer.buffer.Write(buf[w : w+cfile.wBuffer.freeSize])
			w = w + cfile.wBuffer.freeSize
			cfile.wBuffer.startOffset = cfile.FileSize
			cfile.FileSize = cfile.FileSize + int64(cfile.wBuffer.freeSize)
			cfile.wBuffer.endOffset = cfile.FileSize
			cfile.wBuffer.chunkInfo.ChunkSize = cfile.wBuffer.chunkInfo.ChunkSize + int32(cfile.wBuffer.freeSize)
			cfile.wBuffer.freeSize = 0
		}

		if cfile.wBuffer.freeSize == 0 {
			ret := cfile.push()
			if ret != 0 {
				return -1
			}
		}
	}

	return w
}

func (cfile *CFile) push() int32 {

	if cfile.Status != 0 {
		logger.Error("cfile status error , push func return err ")
		return 0
	}

	if cfile.wBuffer.chunkInfo == nil {
		return 0
	}
	wBuffer := cfile.wBuffer // record cur buffer

	return cfile.send(&wBuffer)
}

// Flush ...
func (cfile *CFile) Flush() int32 {

	if cfile.Status != 0 {
		logger.Error("cfile status error , Flush func return err ")
		return cfile.Status
	}
	//avoid repeat push for integer file ETC. 64MB , the last push has already done in Write func
	if cfile.wBuffer.freeSize != 0 && cfile.wBuffer.chunkInfo != nil {
		wBuffer := cfile.wBuffer
		cfile.wBuffer.freeSize = 0
		return cfile.send(&wBuffer)
	}
	return 0
}

// SetChunkStatus ...
func (cfile *CFile) SetChunkStatus(ip string, port int32, blkgrpid int32, blkid int32, chunkid int64, position int32, status int32) int32 {

	vpUpdateChunkInfoReq := &vp.UpdateChunkInfoReq{}
	vpUpdateChunkInfoReq.Ip = ip
	vpUpdateChunkInfoReq.Port = port
	vpUpdateChunkInfoReq.VolID = cfile.cfs.VolID
	vpUpdateChunkInfoReq.BlockGroupID = blkgrpid
	vpUpdateChunkInfoReq.BlockID = blkid
	vpUpdateChunkInfoReq.ChunkID = chunkid
	vpUpdateChunkInfoReq.Position = position
	vpUpdateChunkInfoReq.Status = status
	vpUpdateChunkInfoReq.Path = cfile.Path

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
func (cfile *CFile) writeChunk(ip string, port int32, dc dp.DataNodeClient, req *dp.WriteChunkReq, blkgrpid int32, copies *int, position int32) {
	ctx, _ := context.WithTimeout(context.Background(), 5*time.Second)
	ret, err := dc.WriteChunk(ctx, req)
	if err != nil {
		cfile.SetChunkStatus(ip, port, blkgrpid, req.BlockID, req.ChunkID, position, 1)
		cfile.CurChunkStatus[position] = 1
	} else {
		if ret.Ret != 0 {
			cfile.SetChunkStatus(ip, port, blkgrpid, req.BlockID, req.ChunkID, position, 1)
			cfile.CurChunkStatus[position] = 1
		} else {
			*copies = *copies + 1
		}
	}
	cfile.wgWriteReps.Add(-1)
}

func (cfile *CFile) send(v *wBuffer) int32 {

	dataBuf := v.buffer.Next(v.buffer.Len())
	copies := 0

	if v.chunkInfo.ChunkID != cfile.CurChunkID {
		cfile.CurChunkID = v.chunkInfo.ChunkID
		cfile.CurChunkStatus[0] = 0
		cfile.CurChunkStatus[1] = 0
		cfile.CurChunkStatus[2] = 0
	}

	for i := range v.chunkInfo.BlockGroup.BlockInfos {

		if cfile.CurChunkStatus[i] != 0 {
			continue
		}

		ip := utils.InetNtoa(v.chunkInfo.BlockGroup.BlockInfos[i].DataNodeIP).String()
		port := int(v.chunkInfo.BlockGroup.BlockInfos[i].DataNodePort)
		addr := ip + ":" + strconv.Itoa(port)

		if addr != cfile.wLastDataNode[i] {
			if cfile.ConnD[i] != nil {
				cfile.ConnD[i].Close()
			}
			var err error
			cfile.ConnD[i], err = DialData(addr)
			if err != nil {
				logger.Error("send to datanode failed,Dial failed:%v\n", err)
				continue
			}
			cfile.Dc[i] = dp.NewDataNodeClient(cfile.ConnD[i])
			cfile.wLastDataNode[i] = addr
		}

		blockID := v.chunkInfo.BlockGroup.BlockInfos[i].BlockID
		chunkID := v.chunkInfo.ChunkID

		pWriteChunkReq := &dp.WriteChunkReq{
			ChunkID: chunkID,
			BlockID: blockID,
			Databuf: dataBuf,
		}

		cfile.wgWriteReps.Add(1)
		go cfile.writeChunk(ip, v.chunkInfo.BlockGroup.BlockInfos[i].DataNodePort, cfile.Dc[i], pWriteChunkReq, v.chunkInfo.BlockGroup.BlockGroupID, &copies, int32(i))

	}

	cfile.wgWriteReps.Wait()

	if copies < 1 {
		logger.Error("WriteChunk copies < 2")
		cfile.Status = 1
		return cfile.Status
	}

	mc := mp.NewMetaNodeClient(cfile.ConnM)
	pSyncChunkReq := &mp.SyncChunkReq{
		FileName: cfile.Path,
		VolID:    cfile.cfs.VolID,
	}

	var tmpChunkInfo mp.ChunkInfo
	tmpChunkInfo.ChunkSize = v.chunkInfo.ChunkSize
	tmpChunkInfo.ChunkID = v.chunkInfo.ChunkID
	tmpChunkInfo.BlockGroupID = v.chunkInfo.BlockGroup.BlockGroupID

	for i := 0; i < 3; i++ {
		tmpChunkInfo.Status = append(tmpChunkInfo.Status, cfile.CurChunkStatus[i])
	}

	pSyncChunkReq.ChunkInfo = &tmpChunkInfo

	ctx, _ := context.WithTimeout(context.Background(), 5*time.Second)
	pSyncChunkAck, ret := mc.SyncChunk(ctx, pSyncChunkReq)
	if ret != nil {
		logger.Error("send SyncChunk Failed :%v\n", pSyncChunkReq.ChunkInfo)
		cfile.ConnM.Close()
		var err error
		time.Sleep(2 * time.Second)
		cfile.ConnM, err = DialMeta(cfile.cfs.VolID)
		if err != nil {
			logger.Error("Dial failed:%v\n", err)
			cfile.Status = 1
			return cfile.Status
		}
		mc := mp.NewMetaNodeClient(cfile.ConnM)
		ctx, _ := context.WithTimeout(context.Background(), 5*time.Second)
		pSyncChunkAck, ret = mc.SyncChunk(ctx, pSyncChunkReq)
		if ret != nil {
			logger.Error("send SyncChunk Failed again:%v\n", pSyncChunkReq.ChunkInfo)
			cfile.Status = 1
			return cfile.Status
		}
	}
	if pSyncChunkAck.Ret != 0 {
		cfile.Status = 1
		logger.Error("send SyncChunk Failed :%v\n", pSyncChunkReq.ChunkInfo)
		return cfile.Status
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
	return cfile.Status
}

// Sync ...
func (cfile *CFile) Sync() int32 {
	return 0
}

// CloseConns ...
func (cfile *CFile) CloseConns() {

	if cfile.ConnM != nil {
		cfile.ConnM.Close()
	}
	if cfile.ConnD[0] != nil {
		cfile.ConnD[0].Close()
	}
	if cfile.ConnD[1] != nil {
		cfile.ConnD[1].Close()
	}
	if cfile.ConnD[2] != nil {
		cfile.ConnD[2].Close()
	}
	cfile.wLastDataNode = [3]string{}
	cfile.Dc = [3]dp.DataNodeClient{}
	cfile.CurChunkID = -1
	cfile.CurChunkStatus = [3]int32{}
}

// Close ...
func (cfile *CFile) Close(flags int) int32 {
	/*
		if cfile.Status != 0 {
			logger.Error("cfile status error , Close func just return")
			return -1
		}

		if (flags&os.O_WRONLY) != 0 || (flags&os.O_RDWR) != 0 {
			return cfile.Flush()
		}
		return 0
	*/
	return 0
}

// ProcessLocalBuffer ...
func ProcessLocalBuffer(buffer []byte, cfile *CFile) {
	cfile.Write(buffer, int32(len(buffer)))
}

// ReadLocalAndWriteCFS ...
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
