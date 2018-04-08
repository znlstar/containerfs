package cfs

import (
	"bytes"
	"fmt"
	"os"
	"time"

	"github.com/tiglabs/containerfs/logger"
	"github.com/tiglabs/containerfs/proto/dp"
	"github.com/tiglabs/containerfs/proto/mp"
	"github.com/tiglabs/containerfs/proto/vp"
	"github.com/tiglabs/containerfs/utils"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
)

var VolMgrHosts []string
var MetaNodeHosts []string

// CFS ...
type CFS struct {
	VolID  string
	Copies int

	VolMgrConn   *grpc.ClientConn
	VolMgrLeader string

	MetaNodeConn   *grpc.ClientConn
	MetaNodeLeader string
}

// OpenFileSystem ...
func OpenFileSystem(uuid string) *CFS {
	cfs := CFS{VolID: uuid}

	cfs.GetVolumeMetaPeers(uuid)

	err := cfs.GetLeaderInfo(uuid)
	if err != nil {
		logger.Error("OpenFileSystem GetLeaderConn Failed err:%v", err)
		return nil
	}
	cfs.CheckLeaderConns()
	return &cfs
}

func (cfs *CFS) GetLeaderHost() (volMgrLeader string, metaNodeLeader string, err error) {

	volMgrLeader, err = utils.GetVolMgrLeader(VolMgrHosts)
	if err != nil {
		logger.Error("GetLeaderHost failed: %v", err)
		return "", "", err
	}
	metaNodeLeader, err = utils.GetMetaNodeLeader(MetaNodeHosts, cfs.VolID)
	if err != nil {
		logger.Error("GretLeaderHost failed: %v", err)
		return "", "", err
	}
	return volMgrLeader, metaNodeLeader, nil
}

func (cfs *CFS) GetLeaderInfo(uuid string) error {

	var err error
	cfs.VolMgrLeader, cfs.VolMgrConn, err = utils.DialVolMgr(VolMgrHosts)
	if err != nil {
		return err
	}

	vc := vp.NewVolMgrClient(cfs.VolMgrConn)
	pGetMetaNodeRGReq := &vp.GetMetaNodeRGReq{
		UUID: uuid,
	}
	ctx, _ := context.WithTimeout(context.Background(), 10*time.Second)
	pGetMetaNodeRGAck, err := vc.GetMetaNodeRG(ctx, pGetMetaNodeRGReq)
	if err != nil {
		return err
	}

	if pGetMetaNodeRGAck.Ret != 0 {
		logger.Error("GetLeaderConn GetMetaNodeRG failed :%v", pGetMetaNodeRGAck.Ret)
		return fmt.Errorf("GetMetaNodeRG Failed Ret:%v", pGetMetaNodeRGAck.Ret)
	}

	cfs.Copies = int(pGetMetaNodeRGAck.Copies)
	cfs.MetaNodeLeader = pGetMetaNodeRGAck.Leader
	cfs.MetaNodeConn, err = utils.Dial(cfs.MetaNodeLeader)
	if err != nil {
		return err
	}
	return nil
}

func (cfs *CFS) GetVolumeMetaPeers(uuid string) error {

	_, conn, err := utils.DialVolMgr(VolMgrHosts)
	if err != nil {
		logger.Error("DialVolMgr failed: %v", err)
		return err
	}

	vc := vp.NewVolMgrClient(conn)
	pGetMetaNodeRGReq := &vp.GetMetaNodeRGReq{
		UUID: uuid,
	}
	ctx, _ := context.WithTimeout(context.Background(), 10*time.Second)
	pGetMetaNodeRGAck, err := vc.GetMetaNodeRG(ctx, pGetMetaNodeRGReq)
	if err != nil {
		return err
	}

	if pGetMetaNodeRGAck.Ret != 0 {
		logger.Error("GetLeaderConn GetMetaNodeRG failed :%v", pGetMetaNodeRGAck.Ret)
		return fmt.Errorf("GetMetaNodeRG Failed Ret:%v", pGetMetaNodeRGAck.Ret)
	}

	for _, v := range pGetMetaNodeRGAck.MetaNodes {
		MetaNodeHosts = append(MetaNodeHosts, v.Host+":9901")
	}
	return nil
}

func (cfs *CFS) CheckLeaderConns() {

	ticker := time.NewTicker(time.Millisecond * 500)
	go func() {
		for range ticker.C {
			vLeader, mLeader, err := cfs.GetLeaderHost()
			if err != nil {
				logger.Error("CheckLeaderConns GetLeaderHost err %v", err)
				continue
			}
			if vLeader != cfs.VolMgrLeader {
				logger.Error("VolMgr Leader Change! Old Leader %v,New Leader %v", cfs.VolMgrLeader, vLeader)

				if cfs.VolMgrConn != nil {
					cfs.VolMgrConn.Close()
					cfs.VolMgrConn = nil
				}

				cfs.VolMgrConn, err = utils.Dial(vLeader)
				cfs.VolMgrLeader = vLeader
			}
			if mLeader != cfs.MetaNodeLeader || cfs.MetaNodeConn == nil {
				logger.Error("MetaNode Leader Change! Old Leader %v,New Leader %v", cfs.MetaNodeLeader, mLeader)

				if cfs.MetaNodeConn != nil {
					cfs.MetaNodeConn.Close()
					cfs.MetaNodeConn = nil
				}

				cfs.MetaNodeConn, err = utils.Dial(mLeader)
				cfs.MetaNodeLeader = mLeader
			}
			if cfs.MetaNodeConn != nil && cfs.MetaNodeLeader != "" && cfs.MetaNodeConn.GetState() == connectivity.TransientFailure {
				logger.Debug("Need to close bad grpc connection of state TransientFailure")
				cfs.MetaNodeConn.Close()
				cfs.MetaNodeConn = nil
				cfs.MetaNodeConn, err = utils.Dial(cfs.MetaNodeLeader)
				if err != nil {
					continue
				}
			}
		}
	}()

}

// GetFSInfo ...
func (cfs *CFS) GetFSInfo() (int32, *mp.GetFSInfoAck) {

	mc := mp.NewMetaNodeClient(cfs.MetaNodeConn)
	pGetFSInfoReq := &mp.GetFSInfoReq{
		VolID: cfs.VolID,
	}
	ctx, _ := context.WithTimeout(context.Background(), 5*time.Second)
	pGetFSInfoAck, err := mc.GetFSInfo(ctx, pGetFSInfoReq)
	if err != nil {
		logger.Error("GetFSInfo failed,grpc func err :%v", err)
		return 1, nil
	}
	if pGetFSInfoAck.Ret != 0 {
		logger.Error("GetFSInfo failed,grpc func ret :%v", pGetFSInfoAck.Ret)
		return 1, nil
	}
	return 0, pGetFSInfoAck
}

func (cfs *CFS) checkMetaConn() int32 {
	for i := 0; cfs.MetaNodeConn == nil && i < 10; i++ {
		time.Sleep(300 * time.Millisecond)
	}

	if cfs.MetaNodeConn == nil {
		return -1
	}
	return 0
}

// CreateDirDirect ...
func (cfs *CFS) CreateDirDirect(pinode uint64, name string) (int32, uint64) {

	ret := cfs.checkMetaConn()
	if ret != 0 {
		return -1, 0
	}

	mc := mp.NewMetaNodeClient(cfs.MetaNodeConn)
	pCreateDirDirectReq := &mp.CreateDirDirectReq{
		PInode: pinode,
		Name:   name,
		VolID:  cfs.VolID,
	}
	ctx, _ := context.WithTimeout(context.Background(), 5*time.Second)
	pCreateDirDirectAck, err := mc.CreateDirDirect(ctx, pCreateDirDirectReq)
	if err != nil {

		time.Sleep(time.Second)

		ret := cfs.checkMetaConn()
		if ret != 0 {
			return -1, 0
		}

		mc = mp.NewMetaNodeClient(cfs.MetaNodeConn)
		ctx, _ = context.WithTimeout(context.Background(), 5*time.Second)
		pCreateDirDirectAck, err = mc.CreateDirDirect(ctx, pCreateDirDirectReq)
		if err != nil {
			return -1, 0
		}

	}
	return pCreateDirDirectAck.Ret, pCreateDirDirectAck.Inode
}

// GetInodeInfoDirect ...
func (cfs *CFS) GetInodeInfoDirect(pinode uint64, name string) (int32, uint64, *mp.InodeInfo) {

	ret := cfs.checkMetaConn()
	if ret != 0 {
		return -1, 0, nil
	}

	mc := mp.NewMetaNodeClient(cfs.MetaNodeConn)
	pGetInodeInfoDirectReq := &mp.GetInodeInfoDirectReq{
		PInode: pinode,
		Name:   name,
		VolID:  cfs.VolID,
	}
	ctx, _ := context.WithTimeout(context.Background(), 5*time.Second)
	pGetInodeInfoDirectAck, err := mc.GetInodeInfoDirect(ctx, pGetInodeInfoDirectReq)
	if err != nil {

		time.Sleep(time.Second)

		ret := cfs.checkMetaConn()
		if ret != 0 {
			return -1, 0, nil
		}

		mc = mp.NewMetaNodeClient(cfs.MetaNodeConn)
		ctx, _ := context.WithTimeout(context.Background(), 5*time.Second)
		pGetInodeInfoDirectAck, err = mc.GetInodeInfoDirect(ctx, pGetInodeInfoDirectReq)
		if err != nil {
			return -1, 0, nil
		}

	}
	return pGetInodeInfoDirectAck.Ret, pGetInodeInfoDirectAck.Inode, pGetInodeInfoDirectAck.InodeInfo
}

// GetSymLinkInfoDirect ...
func (cfs *CFS) GetSymLinkInfoDirect(pinode uint64, name string) (int32, uint64) {

	ret := cfs.checkMetaConn()
	if ret != 0 {
		return -1, 0
	}

	mc := mp.NewMetaNodeClient(cfs.MetaNodeConn)
	pGetSymLinkInfoDirectReq := &mp.GetSymLinkInfoDirectReq{
		PInode: pinode,
		Name:   name,
		VolID:  cfs.VolID,
	}
	ctx, _ := context.WithTimeout(context.Background(), 5*time.Second)
	pGetSymLinkInfoDirectAck, err := mc.GetSymLinkInfoDirect(ctx, pGetSymLinkInfoDirectReq)
	if err != nil {

		time.Sleep(time.Second)

		ret := cfs.checkMetaConn()
		if ret != 0 {
			return -2, 0
		}

		mc = mp.NewMetaNodeClient(cfs.MetaNodeConn)
		ctx, _ := context.WithTimeout(context.Background(), 5*time.Second)
		pGetSymLinkInfoDirectAck, err = mc.GetSymLinkInfoDirect(ctx, pGetSymLinkInfoDirectReq)
		if err != nil {
			return -3, 0
		}

	}
	return pGetSymLinkInfoDirectAck.Ret, pGetSymLinkInfoDirectAck.Inode
}

// StatDirect ...
func (cfs *CFS) StatDirect(pinode uint64, name string) (int32, uint32, uint64) {

	ret := cfs.checkMetaConn()
	if ret != 0 {
		return -1, 0, 0
	}

	mc := mp.NewMetaNodeClient(cfs.MetaNodeConn)
	pStatDirectReq := &mp.StatDirectReq{
		PInode: pinode,
		Name:   name,
		VolID:  cfs.VolID,
	}
	ctx, _ := context.WithTimeout(context.Background(), 5*time.Second)
	pStatDirectAck, err := mc.StatDirect(ctx, pStatDirectReq)
	if err != nil {

		time.Sleep(time.Second)

		ret := cfs.checkMetaConn()
		if ret != 0 {
			return -1, 0, 0
		}

		mc = mp.NewMetaNodeClient(cfs.MetaNodeConn)
		ctx, _ := context.WithTimeout(context.Background(), 5*time.Second)
		pStatDirectAck, err = mc.StatDirect(ctx, pStatDirectReq)
		if err != nil {
			return -1, 0, 0
		}
	}
	return pStatDirectAck.Ret, pStatDirectAck.InodeType, pStatDirectAck.Inode
}

// ListDirect ...
func (cfs *CFS) ListDirect(pinode uint64, name string) (int32, []*mp.DirentN) {

	ret := cfs.checkMetaConn()
	if ret != 0 {
		return -1, nil
	}

	mc := mp.NewMetaNodeClient(cfs.MetaNodeConn)
	pListDirectReq := &mp.ListDirectReq{
		PInode: pinode,
		VolID:  cfs.VolID,
		Name:   name,
	}
	ctx, _ := context.WithTimeout(context.Background(), 60*time.Second)
	pListDirectAck, err := mc.ListDirect(ctx, pListDirectReq)
	if err != nil {
		return -1, nil
	}

	return pListDirectAck.Ret, pListDirectAck.Dirents
}

// DeleteDirDirect ...
func (cfs *CFS) DeleteDirDirect(pinode uint64, name string) int32 {

	ret, _, inode := cfs.StatDirect(pinode, name)
	if ret != 0 {
		logger.Debug("DeleteDirDirect StatDirect Failed , no such dir")
		return 0
	}

	ret = cfs.checkMetaConn()
	if ret != 0 {
		return -1
	}

	mc := mp.NewMetaNodeClient(cfs.MetaNodeConn)

	pListDirectReq := &mp.ListDirectReq{
		PInode: inode,
		VolID:  cfs.VolID,
	}
	ctx, _ := context.WithTimeout(context.Background(), 5*time.Second)
	pListDirectAck, err := mc.ListDirect(ctx, pListDirectReq)
	if err != nil {
		logger.Error("DeleteDirDirect ListDirect :%v\n", err)
		return -1
	}

	for _, v := range pListDirectAck.Dirents {

		if v.InodeType == 1 {
			ret := cfs.DeleteDirDirect(inode, v.Name)
			if ret != 0 {
				return ret
			}
		} else if v.InodeType == 2 {
			ret := cfs.DeleteFileDirect(inode, v.Name)
			if ret != 0 {
				return ret
			}
		} else if v.InodeType == 3 {
			ret := cfs.DeleteSymLinkDirect(inode, v.Name)
			if ret != 0 {
				return ret
			}
		}

	}

	pDeleteDirDirectReq := &mp.DeleteDirDirectReq{
		PInode: pinode,
		Name:   name,
		VolID:  cfs.VolID,
	}
	ctx, _ = context.WithTimeout(context.Background(), 60*time.Second)
	pDeleteDirDirectAck, err := mc.DeleteDirDirect(ctx, pDeleteDirDirectReq)
	if err != nil {
		return -1
	}
	return pDeleteDirDirectAck.Ret
}

// RenameDirect ...
func (cfs *CFS) RenameDirect(oldpinode uint64, oldname string, newpinode uint64, newname string) int32 {

	ret := cfs.checkMetaConn()
	if ret != 0 {
		return -1
	}

	mc := mp.NewMetaNodeClient(cfs.MetaNodeConn)
	pRenameDirectReq := &mp.RenameDirectReq{
		OldPInode: oldpinode,
		OldName:   oldname,
		NewPInode: newpinode,
		NewName:   newname,
		VolID:     cfs.VolID,
	}
	ctx, _ := context.WithTimeout(context.Background(), 5*time.Second)
	pRenameDirectAck, err := mc.RenameDirect(ctx, pRenameDirectReq)
	if err != nil {
		return -1
	}

	return pRenameDirectAck.Ret
}

// CreateFileDirect ...
func (cfs *CFS) CreateFileDirect(pinode uint64, name string, flags int) (int32, *CFile) {
	var writer int32

	if flags&os.O_EXCL != 0 {
		if ret, _, _ := cfs.StatDirect(pinode, name); ret == 0 {
			return 17, nil
		}
	}

	ret, inode := cfs.createFileDirect(pinode, name)
	if ret != 0 {
		return ret, nil
	}

	cfile := CFile{
		OpenFlag:         flags,
		cfs:              cfs,
		Writer:           writer,
		FileSize:         0,
		FileSizeInCache:  0,
		ParentInodeID:    pinode,
		Inode:            inode,
		Name:             name,
		wBuffer:          wBuffer{buffer: new(bytes.Buffer), freeSize: BufferSize},
		isWrite:          int(flags)&os.O_WRONLY != 0 || int(flags)&os.O_RDWR != 0,
		convergeBuffer:   new(bytes.Buffer),
		convergeTimer:    time.NewTimer(time.Second / 10),
		convergeFlushCh:  make(chan *struct{}, 1),
		DataCache:        make(map[uint64]*Data),
		DataQueue:        make(chan *chanData, 1),
		CloseSignal:      make(chan struct{}, 10),
		WriteErrSignal:   make(chan bool, 10),
		DataConn:         make(map[string]*grpc.ClientConn),
		errDataNodeCache: make(map[string]bool),
	}
	if cfile.isWrite {
		go cfile.WriteThread()
		go cfile.startFlushConvergeBuffer()
	}

	return 0, &cfile
}

// OpenFileDirect ...
func (cfs *CFS) OpenFileDirect(pinode uint64, name string, flags int) (int32, *CFile) {

	logger.Debug("OpenFileDirect: name: %v, flags: %v\n", name, flags)

	ret, chunkInfos, inode := cfs.GetFileChunksDirect(pinode, name)
	if ret != 0 {
		return ret, nil
	}
	var tmpFileSize int64
	if len(chunkInfos) > 0 {
		for i := range chunkInfos {
			tmpFileSize += int64(chunkInfos[i].ChunkSize)
		}
	}

	cfile := CFile{
		OpenFlag:         flags,
		cfs:              cfs,
		FileSize:         tmpFileSize,
		FileSizeInCache:  tmpFileSize,
		ParentInodeID:    pinode,
		Inode:            inode,
		wBuffer:          wBuffer{buffer: new(bytes.Buffer), freeSize: BufferSize},
		isWrite:          int(flags)&os.O_WRONLY != 0 || int(flags)&os.O_RDWR != 0,
		convergeBuffer:   new(bytes.Buffer),
		convergeTimer:    time.NewTimer(time.Second / 10),
		convergeFlushCh:  make(chan *struct{}, 1),
		Name:             name,
		chunks:           chunkInfos,
		DataCache:        make(map[uint64]*Data),
		DataQueue:        make(chan *chanData, 1),
		CloseSignal:      make(chan struct{}, 10),
		WriteErrSignal:   make(chan bool, 2),
		DataConn:         make(map[string]*grpc.ClientConn),
		errDataNodeCache: make(map[string]bool),
	}

	if cfile.isWrite {
		go cfile.WriteThread()
		go cfile.startFlushConvergeBuffer()
	}

	return 0, &cfile
}

// UpdateOpenFileDirect ...
func (cfs *CFS) UpdateOpenFileDirect(pinode uint64, name string, cfile *CFile, flags int) int32 {

	cfile.isWrite = int(flags)&os.O_WRONLY != 0 || int(flags)&os.O_RDWR != 0

	if cfile.isWrite {
		go cfile.WriteThread()
		go cfile.startFlushConvergeBuffer()
	}
	return 0
}

// createFileDirect ...
func (cfs *CFS) createFileDirect(pinode uint64, name string) (int32, uint64) {

	ret := cfs.checkMetaConn()
	if ret != 0 {
		logger.Debug("createFileDirect checkMetaConn failed ret %v", ret)
		return -1, 0
	}

	mc := mp.NewMetaNodeClient(cfs.MetaNodeConn)
	pCreateFileDirectReq := &mp.CreateFileDirectReq{
		PInode: pinode,
		Name:   name,
		VolID:  cfs.VolID,
	}
	ctx, _ := context.WithTimeout(context.Background(), 5*time.Second)
	pCreateFileDirectAck, err := mc.CreateFileDirect(ctx, pCreateFileDirectReq)
	if err != nil || pCreateFileDirectAck.Ret != 0 {

		time.Sleep(time.Second)

		ret := cfs.checkMetaConn()
		if ret != 0 {
			return -1, 0
		}

		mc = mp.NewMetaNodeClient(cfs.MetaNodeConn)
		ctx, _ := context.WithTimeout(context.Background(), 5*time.Second)
		pCreateFileDirectAck, err = mc.CreateFileDirect(ctx, pCreateFileDirectReq)
		if err != nil {
			logger.Error("CreateFileDirect failed,grpc func failed :%v\n", err)
			return -1, 0
		}
	}

	logger.Debug("createFileDirect  mc.CreateFileDirect failed ret %v", pCreateFileDirectAck.Ret)

	if pCreateFileDirectAck.Ret == 1 {
		return 1, 0
	}
	if pCreateFileDirectAck.Ret == 2 {
		return 2, 0
	}
	if pCreateFileDirectAck.Ret == 17 {
		return 17, 0
	}
	return 0, pCreateFileDirectAck.Inode
}

func (cfs *CFS) DeleteSymLinkDirect(pinode uint64, name string) int32 {
	ret := cfs.checkMetaConn()
	if ret != 0 {
		return -1
	}

	mc := mp.NewMetaNodeClient(cfs.MetaNodeConn)
	mpDeleteSymLinkDirectReq := &mp.DeleteSymLinkDirectReq{
		PInode: pinode,
		Name:   name,
		VolID:  cfs.VolID,
	}
	ctx, _ := context.WithTimeout(context.Background(), 5*time.Second)
	mpDDeleteSymLinkDirectAck, err := mc.DeleteSymLinkDirect(ctx, mpDeleteSymLinkDirectReq)
	if err != nil || mpDDeleteSymLinkDirectAck.Ret != 0 {
		time.Sleep(time.Second)

		ret := cfs.checkMetaConn()
		if ret != 0 {
			return -1
		}
		mc = mp.NewMetaNodeClient(cfs.MetaNodeConn)
		ctx, _ := context.WithTimeout(context.Background(), 5*time.Second)
		mpDDeleteSymLinkDirectAck, err = mc.DeleteSymLinkDirect(ctx, mpDeleteSymLinkDirectReq)
		if err != nil {
			logger.Error("DeleteFile failed,grpc func err :%v\n", err)
			return -1
		}
	}

	return mpDDeleteSymLinkDirectAck.Ret

}

// DeleteFileDirect ...
func (cfs *CFS) DeleteFileDirect(pinode uint64, name string) int32 {

	ret, chunkInfos, _ := cfs.GetFileChunksDirect(pinode, name)
	if ret == 0 && chunkInfos != nil {
		for _, v1 := range chunkInfos {
			for _, v2 := range v1.BlockGroupWithHost.Hosts {

				conn, err := utils.Dial(v2)
				if err != nil || conn == nil {
					time.Sleep(time.Second)
					conn, err = utils.Dial(v2)
					if err != nil || conn == nil {
						logger.Error("DeleteFile failed,Dial to datanode fail :%v\n", err)
						continue
					}
				}

				dc := dp.NewDataNodeClient(conn)
				dpDeleteChunkReq := &dp.DeleteChunkReq{
					ChunkID:      v1.ChunkID,
					BlockGroupID: v1.BlockGroupWithHost.BlockGroupID,
				}
				ctx, _ := context.WithTimeout(context.Background(), 5*time.Second)
				_, err = dc.DeleteChunk(ctx, dpDeleteChunkReq)
				if err != nil {
					logger.Error("DeleteFile failed,rpc to datanode fail :%v\n", err)
				}
				conn.Close()
			}
		}
	}

	ret = cfs.checkMetaConn()
	if ret != 0 {
		return -1
	}

	mc := mp.NewMetaNodeClient(cfs.MetaNodeConn)
	mpDeleteFileDirectReq := &mp.DeleteFileDirectReq{
		PInode: pinode,
		Name:   name,
		VolID:  cfs.VolID,
	}
	ctx, _ := context.WithTimeout(context.Background(), 5*time.Second)
	mpDeleteFileDirectAck, err := mc.DeleteFileDirect(ctx, mpDeleteFileDirectReq)
	if err != nil || mpDeleteFileDirectAck.Ret != 0 {
		time.Sleep(time.Second)

		ret := cfs.checkMetaConn()
		if ret != 0 {
			return -1
		}
		mc = mp.NewMetaNodeClient(cfs.MetaNodeConn)
		ctx, _ := context.WithTimeout(context.Background(), 5*time.Second)
		mpDeleteFileDirectAck, err = mc.DeleteFileDirect(ctx, mpDeleteFileDirectReq)
		if err != nil {
			logger.Error("DeleteFile failed,grpc func err :%v\n", err)
			return -1
		}
	}

	return mpDeleteFileDirectAck.Ret
}

// GetFileChunksDirect ...
func (cfs *CFS) GetFileChunksDirect(pinode uint64, name string) (int32, []*mp.ChunkInfoWithBG, uint64) {

	ret := cfs.checkMetaConn()
	if ret != 0 {
		logger.Error("GetFileChunksDirect cfs.Conn nil ...")
		return -1, nil, 0
	}

	mc := mp.NewMetaNodeClient(cfs.MetaNodeConn)
	pGetFileChunksDirectReq := &mp.GetFileChunksDirectReq{
		PInode: pinode,
		Name:   name,
		VolID:  cfs.VolID,
	}
	ctx, _ := context.WithTimeout(context.Background(), 5*time.Second)
	pGetFileChunksDirectAck, err := mc.GetFileChunksDirect(ctx, pGetFileChunksDirectReq)
	if err != nil || pGetFileChunksDirectAck.Ret != 0 {

		time.Sleep(time.Second)

		ret := cfs.checkMetaConn()
		if ret != 0 {
			logger.Error("GetFileChunksDirect cfs.Conn nil ...")
			return -1, nil, 0
		}

		mc = mp.NewMetaNodeClient(cfs.MetaNodeConn)
		ctx, _ := context.WithTimeout(context.Background(), 5*time.Second)
		pGetFileChunksDirectAck, err = mc.GetFileChunksDirect(ctx, pGetFileChunksDirectReq)
		if err != nil {
			logger.Error("GetFileChunks failed,grpc func failed :%v\n", err)
			return -1, nil, 0
		}
	}
	return pGetFileChunksDirectAck.Ret, pGetFileChunksDirectAck.ChunkInfos, pGetFileChunksDirectAck.Inode
}

func (cfs *CFS) SymLink(pInode uint64, newName string, target string) (int32, uint64) {

	ret := cfs.checkMetaConn()
	if ret != 0 {
		logger.Error("SymLink cfs.Conn nil ...")
		return -1, 0
	}

	mc := mp.NewMetaNodeClient(cfs.MetaNodeConn)
	pSymLinkReq := &mp.SymLinkReq{
		PInode: pInode,
		Name:   newName,
		Target: target,
		VolID:  cfs.VolID,
	}
	ctx, _ := context.WithTimeout(context.Background(), 5*time.Second)
	pSymLinkAck, err := mc.SymLink(ctx, pSymLinkReq)
	if err != nil || pSymLinkAck.Ret != 0 {

		time.Sleep(time.Second)

		ret := cfs.checkMetaConn()
		if ret != 0 {
			logger.Error("SymLink cfs.Conn nil ...")
			return -1, 0
		}

		mc = mp.NewMetaNodeClient(cfs.MetaNodeConn)
		ctx, _ := context.WithTimeout(context.Background(), 5*time.Second)
		pSymLinkAck, err = mc.SymLink(ctx, pSymLinkReq)
		if err != nil {
			logger.Error("SymLink failed,grpc func failed :%v\n", err)
			return -1, 0
		}
	}
	return pSymLinkAck.Ret, pSymLinkAck.Inode

}

func (cfs *CFS) ReadLink(inode uint64) (int32, string) {

	ret := cfs.checkMetaConn()
	if ret != 0 {
		logger.Error("SymLink cfs.Conn nil ...")
		return -1, ""
	}

	mc := mp.NewMetaNodeClient(cfs.MetaNodeConn)
	pReadLinkReq := &mp.ReadLinkReq{
		Inode: inode,
		VolID: cfs.VolID,
	}
	ctx, _ := context.WithTimeout(context.Background(), 5*time.Second)
	pReadLinkAck, err := mc.ReadLink(ctx, pReadLinkReq)
	if err != nil || pReadLinkAck.Ret != 0 {

		time.Sleep(time.Second)

		ret := cfs.checkMetaConn()
		if ret != 0 {
			logger.Error("ReadLink cfs.Conn nil ...")
			return -1, ""
		}

		mc = mp.NewMetaNodeClient(cfs.MetaNodeConn)
		ctx, _ := context.WithTimeout(context.Background(), 5*time.Second)
		pReadLinkAck, err = mc.ReadLink(ctx, pReadLinkReq)
		if err != nil {
			logger.Error("ReadLink failed,grpc func failed :%v\n", err)
			return -1, ""
		}
	}
	return pReadLinkAck.Ret, pReadLinkAck.Target

}

func (cfs *CFS) CloseFileSystem() {

	if cfs.MetaNodeConn != nil {
		cfs.MetaNodeConn.Close()
	}

	if cfs.VolMgrConn != nil {
		cfs.VolMgrConn.Close()
	}
}
