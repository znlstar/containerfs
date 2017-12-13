package cfs

import (
	"bazil.org/fuse"
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"github.com/tigcode/containerfs/logger"
	"github.com/tigcode/containerfs/proto/dp"
	"github.com/tigcode/containerfs/proto/mp"
	//"github.com/tigcode/containerfs/utils"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"io"
	//"io/ioutil"
	"math/rand"
	"os"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

// MetaNodePeers ...
var MetaNodePeers []string

//MetaNodeAddr ...
var MetaNodeAddr string

// chunksize for write
const (
	chunkSize      = 64 * 1024 * 1024
	oneExpandSize  = 30 * 1024 * 1024 * 1024
	BlockGroupSize = 5 * 1024 * 1024 * 1024
)

const (
	FileNormal = 0
	FileError  = 2
)

// BufferSize ...
var BufferSize int

// CFS ...
type CFS struct {
	VolID  string
	Leader string
	Conn   *grpc.ClientConn

	DataConnLocker sync.RWMutex
	//DataConn       map[string]*grpc.ClientConn
	//Status int // 0 ok , 1 readonly 2 invaild
}

/*
func (cfs *CFS) GetDataConn(addr string) (*grpc.ClientConn, error) {

	cfs.DataConnLocker.RLock()
	if v, ok := cfs.DataConn[addr]; ok {
		cfs.DataConnLocker.RUnlock()
		return v, nil
	}
	cfs.DataConnLocker.RUnlock()
	return nil, errors.New("Key not exists")
}

func (cfs *CFS) SetDataConn(addr string, conn *grpc.ClientConn) {
	cfs.DataConnLocker.Lock()
	cfs.DataConn[addr] = conn
	cfs.DataConnLocker.Unlock()
}

func (cfs *CFS) DelDataConn(addr string) {
	cfs.DataConnLocker.Lock()
	delete(cfs.DataConn, addr)
	cfs.DataConnLocker.Unlock()
}
*/

func GetAllDatanode() (int32, []*mp.Datanode) {
	conn, err := DialMeta("Cluster")
	if err != nil {
		logger.Error("GetAllDatanode failed,Dial to metanode fail :%v", err)
		return -1, nil
	}
	defer conn.Close()
	mc := mp.NewMetaNodeClient(conn)
	pGetAllDatanodeReq := &mp.GetAllDatanodeReq{}
	ctx, _ := context.WithTimeout(context.Background(), 5*time.Second)
	pGetAllDatanodeAck, err := mc.GetAllDatanode(ctx, pGetAllDatanodeReq)
	if err != nil {
		logger.Error("GetAllDatanode failed,grpc func err :%v", err)
		return -1, nil
	}
	if pGetAllDatanodeAck.Ret != 0 {
		logger.Error("GetAllDatanode failed,grpc func ret :%v", pGetAllDatanodeAck.Ret)
		return -1, nil
	}
	return 0, pGetAllDatanodeAck.Datanodes
}

func DelDatanode(host string) int {
	conn, err := DialMeta("Cluster")
	if err != nil {
		logger.Error("GetAllDatanode failed,Dial to metanode fail :%v", err)
		return -1
	}
	defer conn.Close()
	mc := mp.NewMetaNodeClient(conn)

	pDelDatanodeReq := &mp.DelDatanodeReq{
		Host: host,
	}
	ctx, _ := context.WithTimeout(context.Background(), 5*time.Second)
	ack, err := mc.DelDatanode(ctx, pDelDatanodeReq)
	if err != nil {
		logger.Error("DelDatanode failed,grpc func err :%v", err)
		return -1
	}
	if ack.Ret != 0 {
		logger.Error("DelDatanode failed,grpc func ret :%v", ack.Ret)
		return -1
	}
	return 0
}

// CreateVol volume function by Meta
func CreateVolbyMeta(name string, capacity string, tier string) int32 {
	conn, err := DialMeta("Cluster")
	if err != nil {
		logger.Error("CreateVol failed,Dial to Cluster leader metanode fail :%v", err)
		return -1
	}
	defer conn.Close()
	mc := mp.NewMetaNodeClient(conn)

	spaceQuota, _ := strconv.Atoi(capacity)
	pCreateVolReq := &mp.CreateVolReq{
		VolName:    name,
		SpaceQuota: int32(spaceQuota),
		Tier:       tier,
	}
	ctx, _ := context.WithTimeout(context.Background(), 10*time.Second)
	ack, err := mc.CreateVol(ctx, pCreateVolReq)
	if err != nil {
		logger.Error("CreateVol failed, Cluster leader metanode return failed,  err:%v", err)
		return -1
	}
	if ack.Ret != 0 {
		logger.Error("CreateVol failed, Cluster leader metanode return failed, ret:%v", ack.Ret)
		if ack.UUID != "" {
			DeleteVol(ack.UUID)
		}
		return -1
	}

	fmt.Println(ack.UUID)
	return 0
}

// Expand volume once for fuseclient
func ExpandVolRS(UUID string, MtPath string) int32 {
	path := MtPath + "/expanding"

	fd, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0666)
	if err != nil {
		return -2
	}
	defer fd.Close()

	conn, err := DialMeta("Cluster")
	if err != nil {
		logger.Error("ExpandVolRS failed,Dial to Cluster leader metanode fail :%v", err)
		return -1
	}
	defer conn.Close()
	mc := mp.NewMetaNodeClient(conn)

	pExpandVolRSReq := &mp.ExpandVolRSReq{
		VolID: UUID,
	}

	ctx, _ := context.WithTimeout(context.Background(), 10*time.Second)
	pExpandVolRSAck, err := mc.ExpandVolRS(ctx, pExpandVolRSReq)
	if err != nil {
		logger.Error("ExpandVol once volume:%v failed, Cluster leader metanode return error:%v", UUID, err)
		os.Remove(path)
		return -1
	}
	if pExpandVolRSAck.Ret == -1 {
		logger.Error("ExpandVol once volume:%v failed, Cluster leader metanode return -1:%v", UUID)
		os.Remove(path)
		return -1
	} else if pExpandVolRSAck.Ret == 0 {
		logger.Error("ExpandVol volume:%v once failed, Cluster leader metanode return 0 because volume totalsize not enough expand", UUID)
		os.Remove(path)
		return 0
	}

	out := UpdateMetaForExpandVol(UUID, pExpandVolRSAck)

	if out != 0 {
		logger.Error("ExpandVol volume:%v once cluster leader metanode success but update volume leader metanode fail, so rollback cluster leader metanode this expand resource", UUID)
		pDelReq := &mp.DelVolRSForExpandReq{
			UUID: UUID,
			BGPS: pExpandVolRSAck.BGPS,
		}

		pDelAck, err := mc.DelVolRSForExpand(ctx, pDelReq)
		if err != nil || pDelAck.Ret != 0 {
			logger.Error("ExpandVol once volume:%v success but update meta failed, then rollback cluster leader metanode error", UUID)
		}
		os.Remove(path)
		return -1
	}

	os.Remove(path)
	return 1
}

func UpdateMetaForExpandVol(UUID string, ack *mp.ExpandVolRSAck) int {
	var mpBlockGroups []*mp.BlockGroup
	for _, v := range ack.BGPS {
		mpBlockGroup := &mp.BlockGroup{
			BlockGroupID: v.Blocks[0].BGID,
			FreeSize:     BlockGroupSize,
		}
		mpBlockGroups = append(mpBlockGroups, mpBlockGroup)
	}

	logger.Debug("ExpandVolRS volume:%v to leader metanode BlockGroups Info:%v", UUID, mpBlockGroups)
	// Meta handle
	conn2, err := DialMeta(UUID)
	if err != nil {
		logger.Error("ExpandVol volume:%v once volmgr success but Dial to metanode fail :%v", UUID, err)
		return -1
	}
	defer conn2.Close()

	mc := mp.NewMetaNodeClient(conn2)
	pmExpandNameSpaceReq := &mp.ExpandNameSpaceReq{
		VolID:       UUID,
		BlockGroups: mpBlockGroups,
	}
	ctx2, _ := context.WithTimeout(context.Background(), 10*time.Second)
	pmExpandNameSpaceAck, err := mc.ExpandNameSpace(ctx2, pmExpandNameSpaceReq)
	if err != nil {
		logger.Error("ExpandVol volume:%v once volmgr success but MetaNode return error:%v", UUID, err)
		return -1
	}
	if pmExpandNameSpaceAck.Ret != 0 {
		logger.Error("ExpandVol volume:%v once volmgr success but MetaNode return not equal 0:%v", UUID)
		return -1
	}

	return 0
}

// ExpandVol volume totalsize for CLI...
func ExpandVolTS(UUID string, expandQuota string) int32 {
	conn, err := DialMeta("Cluster")
	if err != nil {
		logger.Error("ExpandVolTS failed,Dial to Cluster leader metanode fail :%v", err)
		return -1
	}
	defer conn.Close()
	mc := mp.NewMetaNodeClient(conn)

	tmpExpandQuota, _ := strconv.Atoi(expandQuota)
	pExpandVolTSReq := &mp.ExpandVolTSReq{
		VolID:       UUID,
		ExpandQuota: int32(tmpExpandQuota),
	}
	ctx, _ := context.WithTimeout(context.Background(), 10*time.Second)
	pExpandVolTSAck, err := mc.ExpandVolTS(ctx, pExpandVolTSReq)
	if err != nil {
		logger.Error("Expand Vol:%v TotalSize:%v but VolMgr return error:%v", UUID, expandQuota, err)
		return -1
	}
	if pExpandVolTSAck.Ret != 0 {
		logger.Error("Expand Vol:%v TotalSize:%v but VolMgr return -1", UUID, expandQuota)
		return -1
	}

	return 0

}

// Migrate bad DataNode blocks data to some Good DataNodes
func Migrate(host string) int32 {
	conn, err := DialMeta("Cluster")
	if err != nil {
		logger.Error("Migrate failed,Dial to metanode fail :%v", err)
		return -1
	}
	defer conn.Close()
	mc := mp.NewMetaNodeClient(conn)
	pMigrateReq := &mp.MigrateReq{
		DataNodeHost: host,
	}
	ctx, _ := context.WithTimeout(context.Background(), 10*time.Second)
	_, err = mc.Migrate(ctx, pMigrateReq)
	if err != nil {
		logger.Error("Migrate bad DataNode(%v) all Blocks not finished err : %v", host, err)
		return -1
	}

	return 0
}

// GetVolInfo volume info
func GetVolInfo(name string) (int32, *mp.GetVolInfoAck) {
	conn, err := DialMeta("Cluster")
	if err != nil {
		logger.Error("GetVolInfo failed,Dial to metanode fail :%v", err)
		return -1, &mp.GetVolInfoAck{}
	}
	defer conn.Close()
	mc := mp.NewMetaNodeClient(conn)

	pGetVolInfoReq := &mp.GetVolInfoReq{
		UUID: name,
	}
	ctx, _ := context.WithTimeout(context.Background(), 10*time.Second)
	ack, err := mc.GetVolInfo(ctx, pGetVolInfoReq)
	if err != nil || ack.Ret != 0 {
		return -1, &mp.GetVolInfoAck{}
	}
	return 0, ack
}

// SnapShotVol ...
func SnapShotVol(uuid string) int32 {
	// send to metadata to delete a  map
	conn, err := DialMeta(uuid)
	if err != nil {
		logger.Error("SnapShotVol failed,Dial to metanode fail :%v", err)
		return -1
	}
	defer conn.Close()
	mc := mp.NewMetaNodeClient(conn)
	pmSnapShootNameSpaceReq := &mp.SnapShootNameSpaceReq{
		VolID: uuid,
		Type:  0,
	}
	ctx, _ := context.WithTimeout(context.Background(), 100*time.Second)
	pmSnapShootNameSpaceAck, err := mc.SnapShotNameSpace(ctx, pmSnapShootNameSpaceReq)
	if err != nil {
		logger.Error("SnapShotVol failed,grpc func err :%v", err)
		return -1
	}

	if pmSnapShootNameSpaceAck.Ret != 0 {
		logger.Error("SnapShotVol failed,rpc func ret:%v", pmSnapShootNameSpaceAck.Ret)
		return -1
	}
	return 0
}

func GetVolumeLeader(uuid string) string {
	leader, err := GetLeader(uuid)
	if err != nil {
		return "no leader"
	}
	return leader
}

// DeleteVol function
func DeleteVol(uuid string) int32 {

	// send to metadata to delete a  map
	conn2, err := DialMeta(uuid)
	if err != nil {
		logger.Error("DeleteVol failed,Dial to volume leader metanode fail :%v\n", err)
		return -1
	}
	defer conn2.Close()
	mc := mp.NewMetaNodeClient(conn2)
	pmDeleteNameSpaceReq := &mp.DeleteNameSpaceReq{
		VolID: uuid,
		Type:  0,
	}
	ctx, _ := context.WithTimeout(context.Background(), 10*time.Second)
	pmDeleteNameSpaceAck, err := mc.DeleteNameSpace(ctx, pmDeleteNameSpaceReq)
	if err != nil {
		return -1
	}

	if pmDeleteNameSpaceAck.Ret != 0 {
		logger.Error("DeleteNameSpace failed :%v", pmDeleteNameSpaceAck.Ret)
		return -1
	}

	conn2, err = DialMeta("Cluster")
	if err != nil {
		logger.Error("DeleteVol failed,Dial to Cluster leader metanode fail :%v\n", err)
		return -1
	}
	defer conn2.Close()
	mc = mp.NewMetaNodeClient(conn2)

	pDeleteVolReq := &mp.DeleteVolReq{
		UUID: uuid,
	}
	ctx, _ = context.WithTimeout(context.Background(), 10*time.Second)
	pDeleteVolAck, err := mc.DeleteVol(ctx, pDeleteVolReq)
	if err != nil {
		logger.Error("DeleteVol volume from Cluster leader failed,grpc func err :%v", err)
		return -1
	}
	if pDeleteVolAck.Ret != 0 {
		logger.Error("DeleteVol from Cluster leader failed,grpc func ret :%v", pDeleteVolAck.Ret)
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

// OpenFileSystem ...
func OpenFileSystem(UUID string) *CFS {

	leader, err := GetLeader(UUID)
	if err != nil {
		return nil
	}

	conn, err := DialMeta(UUID)
	if conn == nil || err != nil {
		return nil
	}

	cfs := CFS{VolID: UUID, Conn: conn, Leader: leader}

	ticker := time.NewTicker(time.Millisecond * 500)
	go func() {
		for range ticker.C {
			leader, err := GetLeader(UUID)
			if err != nil {

				cfs.Leader = ""
				if cfs.Conn != nil {
					cfs.Conn.Close()
				}
				cfs.Conn = nil

				logger.Error("Leader Timer : Get leader failed ,volumeID : %s", UUID)
				continue
			}
			if leader != cfs.Leader {

				conn, err := DialMeta(UUID)
				if conn == nil || err != nil {
					logger.Error("Leader Timer : DialMeta failed ,volumeID : %s", UUID)
					continue
				}

				cfs.Leader = leader
				if cfs.Conn != nil {
					cfs.Conn.Close()
				}
				cfs.Conn = conn

			}
		}
	}()

	return &cfs
}

// CreateDirDirect ...
func (cfs *CFS) CreateDirDirect(pinode uint64, name string) (int32, uint64) {

	for i := 0; i < 10; i++ {
		if cfs.Conn != nil {
			break
		}
		time.Sleep(300 * time.Millisecond)
		continue
	}
	if cfs.Conn == nil {
		return -1, 0
	}

	mc := mp.NewMetaNodeClient(cfs.Conn)
	pCreateDirDirectReq := &mp.CreateDirDirectReq{
		PInode: pinode,
		Name:   name,
		VolID:  cfs.VolID,
	}
	ctx, _ := context.WithTimeout(context.Background(), 5*time.Second)
	pCreateDirDirectAck, err := mc.CreateDirDirect(ctx, pCreateDirDirectReq)
	if err != nil {

		time.Sleep(time.Second)

		for i := 0; i < 10; i++ {
			if cfs.Conn != nil {
				break
			}
			time.Sleep(300 * time.Millisecond)
			continue
		}
		if cfs.Conn == nil {
			return -1, 0
		}

		mc = mp.NewMetaNodeClient(cfs.Conn)
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

	for i := 0; i < 10; i++ {
		if cfs.Conn != nil {
			break
		}
		time.Sleep(300 * time.Millisecond)
		continue
	}
	if cfs.Conn == nil {
		return -1, 0, nil
	}

	mc := mp.NewMetaNodeClient(cfs.Conn)
	pGetInodeInfoDirectReq := &mp.GetInodeInfoDirectReq{
		PInode: pinode,
		Name:   name,
		VolID:  cfs.VolID,
	}
	ctx, _ := context.WithTimeout(context.Background(), 5*time.Second)
	pGetInodeInfoDirectAck, err := mc.GetInodeInfoDirect(ctx, pGetInodeInfoDirectReq)
	if err != nil {

		time.Sleep(time.Second)

		for i := 0; i < 10; i++ {
			if cfs.Conn != nil {
				break
			}
			time.Sleep(300 * time.Millisecond)
			continue
		}
		if cfs.Conn == nil {
			return -1, 0, nil
		}

		mc = mp.NewMetaNodeClient(cfs.Conn)
		ctx, _ := context.WithTimeout(context.Background(), 5*time.Second)
		pGetInodeInfoDirectAck, err = mc.GetInodeInfoDirect(ctx, pGetInodeInfoDirectReq)
		if err != nil {
			return -1, 0, nil
		}

	}
	return pGetInodeInfoDirectAck.Ret, pGetInodeInfoDirectAck.Inode, pGetInodeInfoDirectAck.InodeInfo
}

// StatDirect ...
func (cfs *CFS) StatDirect(pinode uint64, name string) (int32, bool, uint64) {

	for i := 0; i < 10; i++ {
		if cfs.Conn != nil {
			break
		}
		time.Sleep(300 * time.Millisecond)
		continue
	}
	if cfs.Conn == nil {
		return -1, false, 0
	}

	mc := mp.NewMetaNodeClient(cfs.Conn)
	pStatDirectReq := &mp.StatDirectReq{
		PInode: pinode,
		Name:   name,
		VolID:  cfs.VolID,
	}
	ctx, _ := context.WithTimeout(context.Background(), 5*time.Second)
	pStatDirectAck, err := mc.StatDirect(ctx, pStatDirectReq)
	if err != nil {

		time.Sleep(time.Second)

		for i := 0; i < 10; i++ {
			if cfs.Conn != nil {
				break
			}
			time.Sleep(300 * time.Millisecond)
			continue
		}
		if cfs.Conn == nil {
			return -1, false, 0
		}

		mc = mp.NewMetaNodeClient(cfs.Conn)
		ctx, _ := context.WithTimeout(context.Background(), 5*time.Second)
		pStatDirectAck, err = mc.StatDirect(ctx, pStatDirectReq)
		if err != nil {
			return -1, false, 0
		}
	}
	return pStatDirectAck.Ret, pStatDirectAck.InodeType, pStatDirectAck.Inode
}

// ListDirect ...
func (cfs *CFS) ListDirect(pinode uint64) (int32, []*mp.DirentN) {

	for i := 0; i < 10; i++ {
		if cfs.Conn != nil {
			break
		}
		time.Sleep(300 * time.Millisecond)
		continue
	}
	if cfs.Conn == nil {
		return -1, nil
	}

	mc := mp.NewMetaNodeClient(cfs.Conn)
	pListDirectReq := &mp.ListDirectReq{
		PInode: pinode,
		VolID:  cfs.VolID,
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

	for i := 0; i < 10; i++ {
		if cfs.Conn != nil {
			break
		}
		time.Sleep(300 * time.Millisecond)
		continue
	}
	if cfs.Conn == nil {
		return -1
	}

	mc := mp.NewMetaNodeClient(cfs.Conn)

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

		if v.InodeType {
			ret := cfs.DeleteFileDirect(inode, v.Name)
			if ret != 0 {
				return ret
			}
		} else {
			ret := cfs.DeleteDirDirect(inode, v.Name)
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

	for i := 0; i < 10; i++ {
		if cfs.Conn != nil {
			break
		}
		time.Sleep(300 * time.Millisecond)
		continue
	}
	if cfs.Conn == nil {
		return -1
	}

	mc := mp.NewMetaNodeClient(cfs.Conn)
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
		OpenFlag:       flags,
		cfs:            cfs,
		Writer:         writer,
		FileSize:       0,
		ParentInodeID:  pinode,
		Inode:          inode,
		Name:           name,
		wBuffer:        wBuffer{buffer: new(bytes.Buffer), freeSize: BufferSize},
		DataCache:      make(map[uint64]*Data),
		DataQueue:      make(chan *chanData, 1),
		CloseSignal:    make(chan struct{}, 10),
		WriteErrSignal: make(chan bool, 2),
		ReaderMap:      make(map[fuse.HandleID]*ReaderInfo),
	}
	go cfile.WriteThread()

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
		OpenFlag:       flags,
		cfs:            cfs,
		FileSize:       tmpFileSize,
		ParentInodeID:  pinode,
		Inode:          inode,
		wBuffer:        wBuffer{buffer: new(bytes.Buffer), freeSize: BufferSize},
		Name:           name,
		chunks:         chunkInfos,
		DataCache:      make(map[uint64]*Data),
		DataQueue:      make(chan *chanData, 1),
		CloseSignal:    make(chan struct{}, 10),
		WriteErrSignal: make(chan bool, 2),
		ReaderMap:      make(map[fuse.HandleID]*ReaderInfo),
	}

	go cfile.WriteThread()

	return 0, &cfile
}

// UpdateOpenFileDirect ...
func (cfs *CFS) UpdateOpenFileDirect(pinode uint64, name string, cfile *CFile, flags int) int32 {

	return 0
}

// createFileDirect ...
func (cfs *CFS) createFileDirect(pinode uint64, name string) (int32, uint64) {

	for i := 0; i < 10; i++ {
		if cfs.Conn != nil {
			break
		}
		time.Sleep(300 * time.Millisecond)
		continue
	}
	if cfs.Conn == nil {
		return -1, 0
	}

	mc := mp.NewMetaNodeClient(cfs.Conn)
	pCreateFileDirectReq := &mp.CreateFileDirectReq{
		PInode: pinode,
		Name:   name,
		VolID:  cfs.VolID,
	}
	ctx, _ := context.WithTimeout(context.Background(), 5*time.Second)
	pCreateFileDirectAck, err := mc.CreateFileDirect(ctx, pCreateFileDirectReq)
	if err != nil || pCreateFileDirectAck.Ret != 0 {

		time.Sleep(time.Second)

		for i := 0; i < 10; i++ {
			if cfs.Conn != nil {
				break
			}
			time.Sleep(300 * time.Millisecond)
			continue
		}
		if cfs.Conn == nil {
			return -1, 0
		}

		mc = mp.NewMetaNodeClient(cfs.Conn)
		ctx, _ := context.WithTimeout(context.Background(), 5*time.Second)
		pCreateFileDirectAck, err = mc.CreateFileDirect(ctx, pCreateFileDirectReq)
		if err != nil {
			logger.Error("CreateFileDirect failed,grpc func failed :%v\n", err)
			return -1, 0
		}
	}
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

// DeleteFileDirect ...
func (cfs *CFS) DeleteFileDirect(pinode uint64, name string) int32 {

	for i := 0; i < 10; i++ {
		if cfs.Conn != nil {
			break
		}
		time.Sleep(300 * time.Millisecond)
		continue
	}
	if cfs.Conn == nil {
		return -1
	}

	mc := mp.NewMetaNodeClient(cfs.Conn)
	mpDeleteFileDirectReq := &mp.DeleteFileDirectReq{
		PInode: pinode,
		Name:   name,
		VolID:  cfs.VolID,
	}
	ctx, _ := context.WithTimeout(context.Background(), 5*time.Second)
	mpDeleteFileDirectAck, err := mc.DeleteFileDirect(ctx, mpDeleteFileDirectReq)
	if err != nil || mpDeleteFileDirectAck.Ret != 0 {
		time.Sleep(time.Second)

		for i := 0; i < 10; i++ {
			if cfs.Conn != nil {
				break
			}
			time.Sleep(300 * time.Millisecond)
			continue
		}
		if cfs.Conn == nil {
			return -1
		}

		mc = mp.NewMetaNodeClient(cfs.Conn)
		ctx, _ := context.WithTimeout(context.Background(), 5*time.Second)
		mpDeleteFileDirectAck, err = mc.DeleteFileDirect(ctx, mpDeleteFileDirectReq)
		if err != nil {
			logger.Error("DeleteFile failed,grpc func err :%v\n", err)
			return -1
		}
	}

	//go func() {
	ret, chunkInfos, _ := cfs.GetFileChunksDirect(pinode, name)
	if ret == 0 && chunkInfos != nil {
		for _, v1 := range chunkInfos {
			for _, v2 := range v1.BGP.Blocks {

				conn, err := DialData(v2.Host)
				if err != nil || conn == nil {
					time.Sleep(time.Second)
					conn, err = DialData(v2.Host)
					if err != nil || conn == nil {
						logger.Error("DeleteFile failed,Dial to datanode fail :%v\n", err)
						continue
					}
				}
				defer conn.Close()

				dc := dp.NewDataNodeClient(conn)
				dpDeleteChunkReq := &dp.DeleteChunkReq{
					ChunkID: v1.ChunkID,
					BlockID: v2.BlkID,
				}
				ctx, _ := context.WithTimeout(context.Background(), 5*time.Second)
				_, err = dc.DeleteChunk(ctx, dpDeleteChunkReq)
				if err != nil {
					logger.Error("DeleteFile failed,rpc to datanode fail :%v\n", err)
					continue
				}

			}
		}
	}
	//}()

	return mpDeleteFileDirectAck.Ret
}

// GetFileChunksDirect ...
func (cfs *CFS) GetFileChunksDirect(pinode uint64, name string) (int32, []*mp.ChunkInfoWithBG, uint64) {

	for i := 0; i < 10; i++ {
		if cfs.Conn != nil {
			break
		}
		time.Sleep(300 * time.Millisecond)
		continue
	}
	if cfs.Conn == nil {
		logger.Error("GetFileChunksDirect cfs.Conn nil ...")
		return -1, nil, 0
	}

	mc := mp.NewMetaNodeClient(cfs.Conn)
	pGetFileChunksDirectReq := &mp.GetFileChunksDirectReq{
		PInode: pinode,
		Name:   name,
		VolID:  cfs.VolID,
	}
	ctx, _ := context.WithTimeout(context.Background(), 5*time.Second)
	pGetFileChunksDirectAck, err := mc.GetFileChunksDirect(ctx, pGetFileChunksDirectReq)
	if err != nil || pGetFileChunksDirectAck.Ret != 0 {

		time.Sleep(time.Second)

		for i := 0; i < 10; i++ {
			if cfs.Conn != nil {
				break
			}
			time.Sleep(300 * time.Millisecond)
			continue
		}
		if cfs.Conn == nil {
			logger.Error("GetFileChunksDirect cfs.Conn nil ...")
			return -1, nil, 0
		}

		mc = mp.NewMetaNodeClient(cfs.Conn)
		ctx, _ := context.WithTimeout(context.Background(), 5*time.Second)
		pGetFileChunksDirectAck, err = mc.GetFileChunksDirect(ctx, pGetFileChunksDirectReq)
		if err != nil {
			logger.Error("GetFileChunks failed,grpc func failed :%v\n", err)
			return -1, nil, 0
		}
	}

	logger.Debug("GetFileChunksDirect pGetFileChunksDirectAck %v", pGetFileChunksDirectAck)

	return pGetFileChunksDirectAck.Ret, pGetFileChunksDirectAck.ChunkInfos, pGetFileChunksDirectAck.Inode
}

type Data struct {
	DataBuf *bytes.Buffer
	Status  int32
	timer   *time.Timer
	ID      uint64
}

// ReaderInfo ...
type ReaderInfo struct {
	LastOffset int64
	readBuf    []byte
	Ch         chan *bytes.Buffer
}

type wBuffer struct {
	freeSize int           // chunk size
	buffer   *bytes.Buffer // chunk data
}
type chanData struct {
	data []byte
}

type Chunk struct {
	CFile                    *CFile
	ChunkFreeSize            int
	ChunkInfo                *mp.ChunkInfoWithBG
	ChunkWriteSteam          dp.DataNode_C2MReplClient
	ChunkWriteRecvExitSignal chan struct{}
}

// CFile ...
type CFile struct {
	cfs           *CFS
	ParentInodeID uint64
	Name          string
	Inode         uint64

	OpenFlag int
	FileSize int64
	Status   int32 // 0 ok

	// for write
	wBuffer          wBuffer
	atomicNum        uint64
	Writer           int32
	DataCacheLocker  sync.RWMutex
	DataCache        map[uint64]*Data
	DataQueue        chan *chanData
	WriteErrSignal   chan bool
	WriteRetrySignal chan bool

	Closing     bool
	CloseSignal chan struct{}

	CurChunk *Chunk

	WriteLocker sync.Mutex

	// for read
	//lastoffset int64
	RMutex sync.Mutex
	chunks []*mp.ChunkInfoWithBG // chunkinfo
	//readBuf    []byte
	ReaderMap map[fuse.HandleID]*ReaderInfo
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

	for n := 0; n < len(cfile.chunks[chunkidx].BGP.Blocks); n++ {
		i := idxs[n]

		buffer = new(bytes.Buffer)

		addr := cfile.chunks[chunkidx].BGP.Blocks[i].Host
		conn, err = DialData(addr)
		if err != nil || conn == nil {
			time.Sleep(time.Second)
			conn, err = DialData(addr)
			if err != nil || conn == nil {
				logger.Error("streamread failed,Dial to datanode fail :%v", err)
				outflag++
				continue
			}
		}

		dc := dp.NewDataNodeClient(conn)
		streamreadChunkReq := &dp.StreamReadChunkReq{
			ChunkID:  cfile.chunks[chunkidx].ChunkID,
			BlockID:  cfile.chunks[chunkidx].BGP.Blocks[i].BlkID,
			Offset:   offset,
			Readsize: size,
		}
		ctx, _ := context.WithTimeout(context.Background(), 10*time.Second)
		stream, err := dc.StreamReadChunk(ctx, streamreadChunkReq)
		if err != nil {
			conn.Close()
			conn, err = DialData(addr)
			if err != nil || conn == nil {
				logger.Error("StreamReadChunk DialData error:%v, so retry other datanode!", err)
				outflag++
				continue
			} else {
				dc = dp.NewDataNodeClient(conn)
				streamreadChunkReq := &dp.StreamReadChunkReq{
					ChunkID:  cfile.chunks[chunkidx].ChunkID,
					BlockID:  cfile.chunks[chunkidx].BGP.Blocks[i].BlkID,
					Offset:   offset,
					Readsize: size,
				}
				ctx, _ = context.WithTimeout(context.Background(), 10*time.Second)
				stream, err = dc.StreamReadChunk(ctx, streamreadChunkReq)
				if err != nil {
					conn.Close()
					logger.Error("StreamReadChunk StreamReadChunk error:%v, so retry other datanode!", err)
					outflag++
					continue
				}

			}

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
			break
		} else if inflag == 3 {
			buffer = new(bytes.Buffer)
			buffer.Write([]byte{})
			logger.Error("Stream Read the chunk three copy Recv error")
			ch <- buffer
			break
		} else if inflag < 3 {
			logger.Error("Stream Read the chunk %v copy Recv error, so need retry other datanode!!!", inflag)
			continue
		}
	}
	if outflag >= 3 {
		buffer = new(bytes.Buffer)
		buffer.Write([]byte{})
		logger.Error("Stream Read the chunk three copy Datanode error")
		ch <- buffer
	}
}

// Read ...
func (cfile *CFile) Read(handleID fuse.HandleID, data *[]byte, offset int64, readsize int64) int64 {
	// read data from write buffer

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
func (cfile *CFile) Write(buf []byte, length int32) int32 {

	if cfile.Status == FileError {
		return -2
	}

	data := &chanData{}
	data.data = append(data.data, buf...)

	select {
	case <-cfile.WriteErrSignal:
		logger.Error("Write recv WriteErrSignal ,volumeid %v , pid %v ,fname %v!", cfile.cfs.VolID, cfile.ParentInodeID, cfile.Name)
		return -2
	case cfile.DataQueue <- data:
	}
	return length
}

func (cfile *CFile) WriteThread() {

	logger.Debug("Write Thread: file %v start writethread!\n", cfile.Name)

	for cfile.Status != FileError {
		select {
		case chanData := <-cfile.DataQueue:
			if chanData == nil {
				logger.Debug("WriteThread recv channel close ...")
				var ti uint32
				for cfile.Status == FileNormal {
					if len(cfile.DataCache) == 0 {
						logger.Debug("WriteThread cfile.DataCache == 0 ")
						break
					}
					ti++
					time.Sleep(time.Millisecond * 5)
				}
				if cfile.CurChunk != nil {
					if cfile.CurChunk.ChunkWriteSteam != nil {
						cfile.CurChunk.ChunkWriteSteam.CloseSend()
					}
				}
				cfile.CloseSignal <- struct{}{}
				return
			} else {

				newData := &Data{}
				atomic.AddUint64(&cfile.atomicNum, 1)
				newData.ID = cfile.atomicNum
				newData.DataBuf = new(bytes.Buffer)
				newData.DataBuf.Write(chanData.data)
				newData.Status = 1

				if err := cfile.WriteHandler(newData); err != nil {
					logger.Error("WriteThread WriteHandler err !!%v", err)

					cfile.Status = FileError
					logger.Debug("WriteHandler send WriteErrSignal")
					cfile.WriteErrSignal <- true
				}
			}

		}
	}

}

func (cfile *CFile) WriteHandler(newData *Data) error {

	length := newData.DataBuf.Len()

	logger.Debug("WriteHandler: file %v, num:%v,  length: %v, \n", cfile.Name, cfile.atomicNum, length)

	var ret int32

ALLOCATECHUNK:

	if cfile.CurChunk != nil {
		if cfile.CurChunk.ChunkFreeSize-length < 0 {

			if cfile.CurChunk.ChunkWriteSteam != nil {
				var ti uint32
				logger.Debug("WriteHandler: file %v, begin waiting last chunk: %v\n", cfile.Name, len(cfile.DataCache))
				for cfile.Status == FileNormal {
					if len(cfile.DataCache) == 0 {
						break
					}
					time.Sleep(time.Millisecond * 2)
					ti++
				}
				if cfile.Status == FileError {
					return errors.New("cfile status err")
				}
				logger.Debug("WriteHandler: file %v, end wait after %v ms\n", cfile.Name, ti)
				if cfile.CurChunk.ChunkWriteSteam != nil {
					cfile.CurChunk.ChunkWriteSteam.CloseSend()
				}

				flag := false
				for retryCnt := 0; retryCnt < 5; retryCnt++ {
					ret, cfile.CurChunk = cfile.AllocateChunk(true)
					if ret != 0 {
						time.Sleep(time.Millisecond * 500)
						continue
					} else {
						flag = true
						break
					}
				}
				if !flag {
					return errors.New("AllocateChunk on 5 retry ... ")
				}

			}

		}
	} else {
		flag := false
		for retryCnt := 0; retryCnt < 5; retryCnt++ {
			ret, cfile.CurChunk = cfile.AllocateChunk(true)
			if ret != 0 {
				time.Sleep(time.Millisecond * 500)
				continue
			} else {
				flag = true
				break
			}
		}
		if !flag {
			return errors.New("AllocateChunk on 5 retry ... ")
		}
	}

	cfile.DataCacheLocker.Lock()
	cfile.DataCache[cfile.atomicNum] = newData
	cfile.DataCacheLocker.Unlock()

	req := &dp.StreamWriteReq{
		ChunkID:      cfile.CurChunk.ChunkInfo.ChunkID,
		Master:       &dp.Block{BlockID: cfile.CurChunk.ChunkInfo.BGP.Blocks[0].BlkID, Host: cfile.CurChunk.ChunkInfo.BGP.Blocks[0].Host},
		Slave:        &dp.Block{BlockID: cfile.CurChunk.ChunkInfo.BGP.Blocks[1].BlkID, Host: cfile.CurChunk.ChunkInfo.BGP.Blocks[1].Host},
		Backup:       &dp.Block{BlockID: cfile.CurChunk.ChunkInfo.BGP.Blocks[2].BlkID, Host: cfile.CurChunk.ChunkInfo.BGP.Blocks[2].Host},
		Databuf:      newData.DataBuf.Next(length),
		DataLen:      uint32(length),
		CommitID:     cfile.atomicNum,
		BlockGroupID: cfile.CurChunk.ChunkInfo.BGP.Blocks[0].BGID,
	}

	if cfile.CurChunk != nil {
		if cfile.CurChunk.ChunkWriteSteam != nil {
			if err := cfile.CurChunk.ChunkWriteSteam.Send(req); err != nil {
				logger.Debug("WriteHandler: send file %v, chunk %v len: %v failed\n", cfile.Name, cfile.CurChunk, length)
				cfile.CurChunk.ChunkFreeSize = 0
			} else {
				logger.Debug("WriteHandler: send file %v, chunk %v len: %v success\n", cfile.Name, cfile.CurChunk, length)
				cfile.CurChunk.ChunkFreeSize -= length
			}
		} else {
			goto ALLOCATECHUNK
		}
	} else {
		goto ALLOCATECHUNK
	}

	var lastChunkID uint64
	if len(cfile.chunks) > 0 {
		//for appned write
		lastChunkID = cfile.chunks[len(cfile.chunks)-1].ChunkID
		if lastChunkID == cfile.CurChunk.ChunkInfo.ChunkID {
			cfile.FileSize = cfile.FileSize + int64(length)
			cfile.chunks[len(cfile.chunks)-1].ChunkSize = cfile.chunks[len(cfile.chunks)-1].ChunkSize + int32(length)
		} else {
			chunkinfo := &mp.ChunkInfoWithBG{ChunkID: cfile.CurChunk.ChunkInfo.ChunkID, ChunkSize: int32(length), BGP: cfile.CurChunk.ChunkInfo.BGP}
			cfile.chunks = append(cfile.chunks, chunkinfo)
			cfile.FileSize += int64(length)
		}
	} else {
		chunkinfo := &mp.ChunkInfoWithBG{ChunkID: cfile.CurChunk.ChunkInfo.ChunkID, ChunkSize: int32(length), BGP: cfile.CurChunk.ChunkInfo.BGP}
		cfile.chunks = append(cfile.chunks, chunkinfo)
		cfile.FileSize += int64(length)
	}

	return nil
}

// AllocateChunk ...
func (cfile *CFile) AllocateChunk(IsStream bool) (int32, *Chunk) {

	for i := 0; i < 10; i++ {
		if cfile.cfs.Conn != nil {
			break
		}
		time.Sleep(300 * time.Millisecond)
		continue
	}
	if cfile.cfs.Conn == nil {
		return -1, nil
	}

	mc := mp.NewMetaNodeClient(cfile.cfs.Conn)
	pAllocateChunkReq := &mp.AllocateChunkReq{
		VolID: cfile.cfs.VolID,
	}
	ctx, _ := context.WithTimeout(context.Background(), 5*time.Second)
	pAllocateChunkAck, err := mc.AllocateChunk(ctx, pAllocateChunkReq)
	if err != nil || pAllocateChunkAck.Ret != 0 {
		time.Sleep(time.Second)

		for i := 0; i < 10; i++ {
			if cfile.cfs.Conn != nil {
				break
			}
			time.Sleep(300 * time.Millisecond)
			continue
		}
		if cfile.cfs.Conn == nil {
			return -1, nil
		}

		mc = mp.NewMetaNodeClient(cfile.cfs.Conn)
		ctx, _ := context.WithTimeout(context.Background(), 5*time.Second)
		pAllocateChunkAck, err = mc.AllocateChunk(ctx, pAllocateChunkReq)
		if err != nil {
			logger.Error("AllocateChunk failed,grpc func failed :%v\n", err)
			return -1, nil
		}
	}

	curChunk := &Chunk{}

	curChunk.CFile = cfile
	curChunk.ChunkInfo = pAllocateChunkAck.ChunkInfo

	if IsStream {

		C2Mconn, err := grpc.Dial(pAllocateChunkAck.ChunkInfo.BGP.Blocks[0].Host, grpc.WithInsecure(), grpc.WithBlock(), grpc.FailOnNonTempDialError(true))
		if err != nil {
			return -1, nil
		}
		C2Mclient := dp.NewDataNodeClient(C2Mconn)
		curChunk.ChunkWriteSteam, err = C2Mclient.C2MRepl(context.Background())
		if err != nil {
			return -1, nil
		}

		curChunk.ChunkFreeSize = chunkSize
		curChunk.ChunkWriteRecvExitSignal = make(chan struct{})

		go curChunk.C2MRecv()
	}

	logger.Debug("AllocateChunk success: chunk info:%v\n", pAllocateChunkAck.ChunkInfo)

	return pAllocateChunkAck.Ret, curChunk
}

func (chunk *Chunk) Retry() {

	chunk.CFile.DataCacheLocker.Lock()
	defer chunk.CFile.DataCacheLocker.Unlock()

	flag := false
	var err error
	for retryCnt := 0; retryCnt < 5; retryCnt++ {
		err = chunk.WriteRetryHandle()
		if err != nil {
			time.Sleep(time.Millisecond * 500)
			continue
		} else {
			flag = true
			break
		}
	}
	if !flag {
		chunk.CFile.Status = FileError
		logger.Debug("C2MRecv send WriteErrSignal")
		chunk.CFile.WriteErrSignal <- true
	} else {
		chunk.CFile.DataCache = make(map[uint64]*Data)
		chunk.ChunkFreeSize = 0
		chunk.ChunkWriteSteam = nil
	}

}
func (chunk *Chunk) C2MRecv() {
	logger.Debug("C2MRecv thread started success for file %v chunk %v", chunk.CFile.Name, chunk.ChunkInfo.ChunkID)

	defer chunk.Retry()

	for {
		in, err := chunk.ChunkWriteSteam.Recv()
		if err == io.EOF {
			logger.Debug("C2MRecv: stream %v EOF\n", chunk.ChunkWriteSteam)
			break
		}
		if err != nil {
			logger.Debug("C2MRecv: stream %v error return :%v\n", chunk.ChunkWriteSteam, err)
			break
		}

		if in.Ret == -1 {
			logger.Error("C2MRecv ack.Ret -1 , means M2S2B stream err")
			break
		}

		// comfirm data
		chunk.CFile.DataCacheLocker.Lock()
		//cfile.DataCache[in.CommitID].timer.Stop()
		delete(chunk.CFile.DataCache, in.CommitID)
		chunk.CFile.DataCacheLocker.Unlock()

		// update to metanode
		logger.Debug("C2MRecv: Write success! try to update metadata file: %v, ID；%v, chunk: %v, len: %v\n",
			chunk.CFile.Name, in.CommitID, in.ChunkID, in.DataLen)

		mc := mp.NewMetaNodeClient(chunk.CFile.cfs.Conn)
		pAsyncChunkReq := &mp.AsyncChunkReq{
			VolID:         chunk.CFile.cfs.VolID,
			ParentInodeID: chunk.CFile.ParentInodeID,
			Name:          chunk.CFile.Name,
			ChunkID:       in.ChunkID,
			CommitSize:    in.DataLen,
			BlockGroupID:  in.BlockGroupID,
		}
		ctx, _ := context.WithTimeout(context.Background(), 5*time.Second)
		_, err2 := mc.AsyncChunk(ctx, pAsyncChunkReq)
		if err2 != nil {
			break
		}

	}
}

func (chunk *Chunk) WriteRetryHandle() error {

	if len(chunk.CFile.DataCache) == 0 {
		return nil
	}

	logger.Debug("WriteRetryHandle in ...")

	ret, tmpchunk := chunk.CFile.AllocateChunk(false)
	if ret != 0 {
		return errors.New("error")
	}

	sortedKeys := make([]int, 0)

	for k:= range chunk.CFile.DataCache {
		sortedKeys = append(sortedKeys, int(k))
	}
	sort.Ints(sortedKeys)

	var chunkSize int
	for _, v := range tmpchunk.ChunkInfo.BGP.Blocks {
		conn, err := DialData(v.Host)
		if err != nil {
			return err
		}
		dc := dp.NewDataNodeClient(conn)

		chunkSize = 0
		for _, vv := range sortedKeys {
			req := dp.WriteChunkReq{ChunkID: tmpchunk.ChunkInfo.ChunkID, BlockID: v.BlkID, Databuf: chunk.CFile.DataCache[uint64(vv)].DataBuf.Next(chunk.CFile.DataCache[uint64(vv)].DataBuf.Len()), CommitID: uint64(vv)}
			ctx, _ := context.WithTimeout(context.Background(), 5*time.Second)
			_, err = dc.WriteChunk(ctx, &req)
			if err != nil {
				return err
			}
			chunkSize += chunk.CFile.DataCache[uint64(vv)].DataBuf.Len()
		}
	}

	mc := mp.NewMetaNodeClient(chunk.CFile.cfs.Conn)
	pAsyncChunkReq := &mp.AsyncChunkReq{
		VolID:         chunk.CFile.cfs.VolID,
		ParentInodeID: chunk.CFile.ParentInodeID,
		Name:          chunk.CFile.Name,
		ChunkID:       tmpchunk.ChunkInfo.ChunkID,
		CommitSize:    uint32(chunkSize),
		BlockGroupID:  tmpchunk.ChunkInfo.BGP.Blocks[0].BGID,
	}
	ctx, _ := context.WithTimeout(context.Background(), 5*time.Second)
	_, err2 := mc.AsyncChunk(ctx, pAsyncChunkReq)
	if err2 != nil {
		return err2
	}

	return nil
}

// Sync ...
func (cfile *CFile) Sync() int32 {
	if cfile.Status == FileError {
		return -1
	}
	return 0
}

// Sync ...
func (cfile *CFile) Flush() int32 {
	if cfile.Status == FileError {
		return -1
	}
	return 0
}

// Close ...
func (cfile *CFile) CloseWrite() int32 {
	if cfile.Status == FileError {
		return -1
	} else {
		cfile.Closing = true
		logger.Debug("CloseWrite close cfile.DataQueue")
		close(cfile.DataQueue)
		<-cfile.CloseSignal
		logger.Debug("CloseWrite recv CloseSignal!")
	}
	return 0
}

