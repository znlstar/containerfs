package cfs

import (
	"bazil.org/fuse"
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"github.com/tigcode/containerfs/logger"
	dp "github.com/tigcode/containerfs/proto/dp"
	mp "github.com/tigcode/containerfs/proto/mp"
	//"github.com/tigcode/containerfs/utils"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"io"
	"math/rand"
	"os"
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

// BufferSize ...
var BufferSize int32

// CFS ...
type CFS struct {
	VolID  string
	Leader string
	Conn   *grpc.ClientConn

	DataConnLocker sync.RWMutex
	DataConn       map[string]*grpc.ClientConn
	//Status int // 0 ok , 1 readonly 2 invaild
}

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

func DelDatanode(ip string, port string) int {
	conn, err := DialMeta("Cluster")
	if err != nil {
		logger.Error("GetAllDatanode failed,Dial to metanode fail :%v", err)
		return -1
	}
	defer conn.Close()
	mc := mp.NewMetaNodeClient(conn)

	pDelDatanodeReq := &mp.DelDatanodeReq{
		Ip:   ip,
		Port: port,
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
		if ack.UUID != "" {
			DeleteVol(ack.UUID)
		}
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
func Migrate(ip string, port string) int32 {
	conn, err := DialMeta("Cluster")
	if err != nil {
		logger.Error("Migrate failed,Dial to metanode fail :%v", err)
		return -1
	}
	defer conn.Close()
	mc := mp.NewMetaNodeClient(conn)
	dport, _ := strconv.Atoi(port)
	pMigrateReq := &mp.MigrateReq{
		DataNodeIP:   ip,
		DataNodePort: int32(dport),
	}
	ctx, _ := context.WithTimeout(context.Background(), 10*time.Second)
	_, err = mc.Migrate(ctx, pMigrateReq)
	if err != nil {
		logger.Error("Migrate bad DataNode(%v:%v) all Blocks not finished err : %v", ip, port, err)
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

// SnapShootVol ...
func SnapShootVol(uuid string) int32 {
	// send to metadata to delete a  map
	conn, err := DialMeta(uuid)
	if err != nil {
		logger.Error("SnapShootVol failed,Dial to metanode fail :%v", err)
		return -1
	}
	defer conn.Close()
	mc := mp.NewMetaNodeClient(conn)
	pmSnapShootNameSpaceReq := &mp.SnapShootNameSpaceReq{
		VolID: uuid,
		Type:  0,
	}
	ctx, _ := context.WithTimeout(context.Background(), 100*time.Second)
	pmSnapShootNameSpaceAck, err := mc.SnapShootNameSpace(ctx, pmSnapShootNameSpaceReq)
	if err != nil {
		logger.Error("SnapShootVol failed,grpc func err :%v", err)
		return -1
	}

	if pmSnapShootNameSpaceAck.Ret != 0 {
		logger.Error("SnapShootVol failed,rpc func ret:%v", pmSnapShootNameSpaceAck.Ret)
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

	cfs := CFS{VolID: UUID, Conn: conn, Leader: leader, DataConn: make(map[string]*grpc.ClientConn)}

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
		/*
			if v.InodeType {
				cfs.DeleteFileDirect(inode, v.Name)
			} else {
				cfs.DeleteDirDirect(inode, v.Name)
			}
		*/

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

	/*
		if flags&os.O_TRUNC != 0 {
			if ret, _ := cfs.Stat(path); ret == 0 {
				cfs.DeleteFile(path)
			}
		}
	*/

	if flags&os.O_EXCL != 0 {
		if ret, _, _ := cfs.StatDirect(pinode, name); ret == 0 {
			return 17, nil
		}
	}

	ret, inode := cfs.createFileDirect(pinode, name)
	if ret != 0 {
		return ret, nil
	}

	tmpBuffer := wBuffer{
		buffer:   new(bytes.Buffer),
		freeSize: BufferSize,
	}

	cfile := CFile{
		OpenFlag:      flags,
		cfs:           cfs,
		FileSize:      0,
		ParentInodeID: pinode,
		Inode:         inode,
		Name:          name,
		ReaderMap:     make(map[fuse.HandleID]*ReaderInfo),
		wBuffer:       tmpBuffer,
	}
	//go cfile.send()

	return 0, &cfile
}

// OpenFileDirect ...
func (cfs *CFS) OpenFileDirect(pinode uint64, name string, flags int) (int32, *CFile) {
	var ret int32
	var writer int32
	var tmpFileSize int64

	cfile := CFile{}

	if (flags&os.O_WRONLY) != 0 || (flags&os.O_RDWR) != 0 {

		chunkInfos := make([]*mp.ChunkInfoWithBG, 0)
		var inode uint64
		if ret, chunkInfos, inode = cfs.GetFileChunksDirect(pinode, name); ret != 0 {
			return ret, nil
		}

		if len(chunkInfos) > 0 {

			for i := range chunkInfos {
				tmpFileSize += int64(chunkInfos[i].ChunkSize)
			}
			lastChunk := chunkInfos[len(chunkInfos)-1]

			tmpBuffer := wBuffer{
				buffer:    new(bytes.Buffer),
				freeSize:  BufferSize - (lastChunk.ChunkSize % BufferSize),
				chunkInfo: lastChunk,
			}

			cfile = CFile{
				OpenFlag:      flags,
				cfs:           cfs,
				Writer:        writer,
				FileSize:      tmpFileSize,
				wBuffer:       tmpBuffer,
				ParentInodeID: pinode,
				Inode:         inode,
				Name:          name,
				chunks:        chunkInfos,
				ReaderMap:     make(map[fuse.HandleID]*ReaderInfo),
			}

		} else {

			tmpBuffer := wBuffer{
				buffer:   new(bytes.Buffer),
				freeSize: BufferSize,
			}
			cfile = CFile{
				OpenFlag:      flags,
				cfs:           cfs,
				Writer:        writer,
				FileSize:      0,
				ParentInodeID: pinode,
				Inode:         inode,
				Name:          name,
				wBuffer:       tmpBuffer,
				ReaderMap:     make(map[fuse.HandleID]*ReaderInfo),
			}

		}

	} else {
		chunkInfos := make([]*mp.ChunkInfoWithBG, 0)
		var inode uint64
		if ret, chunkInfos, inode = cfs.GetFileChunksDirect(pinode, name); ret != 0 {
			logger.Error("OpenFile failed , GetFileChunksDirect failed !")
			return ret, nil
		}

		for i := range chunkInfos {
			tmpFileSize += int64(chunkInfos[i].ChunkSize)
		}

		tmpBuffer := wBuffer{
			buffer:   new(bytes.Buffer),
			freeSize: BufferSize,
		}

		cfile = CFile{
			OpenFlag:      flags,
			cfs:           cfs,
			Writer:        writer,
			FileSize:      tmpFileSize,
			wBuffer:       tmpBuffer,
			ParentInodeID: pinode,
			Inode:         inode,
			Name:          name,
			chunks:        chunkInfos,
			ReaderMap:     make(map[fuse.HandleID]*ReaderInfo),
		}

	}
	logger.Debug("OpenFile %v with flags:%v success return cfile: %p", name, flags, &cfile)
	return 0, &cfile
}

// UpdateOpenFileDirect ...
func (cfs *CFS) UpdateOpenFileDirect(pinode uint64, name string, cfile *CFile, flags int) int32 {

	if (flags&os.O_WRONLY) != 0 || (flags&os.O_RDWR) != 0 {

		chunkInfos := make([]*mp.ChunkInfoWithBG, 0)

		var ret int32
		if ret, chunkInfos, _ = cfs.GetFileChunksDirect(pinode, name); ret != 0 {
			return ret
		}

		if len(chunkInfos) > 0 {
			lastChunk := chunkInfos[len(chunkInfos)-1]
			tmpBuffer := wBuffer{
				buffer:    new(bytes.Buffer),
				freeSize:  BufferSize - (lastChunk.ChunkSize % BufferSize),
				chunkInfo: lastChunk,
			}
			cfile.wBuffer = tmpBuffer
		}
	}
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

	ret, chunkInfos, _ := cfs.GetFileChunksDirect(pinode, name)
	if ret == 0 && chunkInfos != nil {
		for _, v1 := range chunkInfos {
			for _, v2 := range v1.BGP.Blocks {

				addr := v2.Ip + ":" + strconv.Itoa(int(v2.Port))
				conn, err := cfs.GetDataConn(addr)
				if err != nil || conn == nil {
					conn, err = DialData(addr)
					if err != nil || conn == nil {
						logger.Error("DeleteFile failed,Dial to datanode fail :%v\n", err)
						//return 0
					} else {
						cfs.SetDataConn(addr, conn)
					}
				}

				dc := dp.NewDataNodeClient(conn)

				dpDeleteChunkReq := &dp.DeleteChunkReq{
					ChunkID: v1.ChunkID,
					BlockID: v2.BlkID,
				}
				ctx, _ := context.WithTimeout(context.Background(), 5*time.Second)
				_, err = dc.DeleteChunk(ctx, dpDeleteChunkReq)
				if err != nil {
					conn.Close()
					cfs.DelDataConn(addr)
					time.Sleep(time.Second)
					conn, err = cfs.GetDataConn(addr)
					if err != nil || conn == nil {
						logger.Error("DeleteChunk failed,Dial to metanode fail :%v\n", err)
					} else {
						dc = dp.NewDataNodeClient(conn)
						ctx, _ := context.WithTimeout(context.Background(), 5*time.Second)
						_, err = dc.DeleteChunk(ctx, dpDeleteChunkReq)
						if err != nil {
							conn.Close()
							cfs.DelDataConn(addr)
							logger.Error("DeleteChunk failed,grpc func failed :%v\n", err)
						}
					}
				}

				//conn.Close()
			}
		}
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
	return pGetFileChunksDirectAck.Ret, pGetFileChunksDirectAck.ChunkInfos, pGetFileChunksDirectAck.Inode
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
	cfs           *CFS
	ParentInodeID uint64
	Name          string
	Inode         uint64

	OpenFlag int
	FileSize int64
	Status   int32 // 0 ok

	// for write
	//WMutex sync.Mutex
	Writer int32
	//FirstW bool
	wBuffer     wBuffer
	wgWriteReps sync.WaitGroup

	// for read
	//lastoffset int64
	RMutex sync.Mutex
	chunks []*mp.ChunkInfoWithBG // chunkinfo
	//readBuf    []byte
	ReaderMap map[fuse.HandleID]*ReaderInfo
}

// AllocateChunk ...
func (cfile *CFile) AllocateChunk() (int32, *mp.ChunkInfoWithBG) {

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

	return pAllocateChunkAck.Ret, pAllocateChunkAck.ChunkInfo
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

		addr := cfile.chunks[chunkidx].BGP.Blocks[i].Ip + ":" + strconv.Itoa(int(cfile.chunks[chunkidx].BGP.Blocks[i].Port))
		conn, err = cfile.cfs.GetDataConn(addr)
		if err != nil || conn == nil {
			conn, err = DialData(addr)
			if err != nil || conn == nil {
				logger.Error("streamread failed,Dial to datanode fail :%v", err)
				outflag++
				continue
			} else {
				cfile.cfs.SetDataConn(addr, conn)
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
			cfile.cfs.DelDataConn(addr)
			conn, err = DialData(addr)
			if err != nil || conn == nil {
				cfile.cfs.DelDataConn(addr)
				logger.Error("StreamReadChunk DialData error:%v, so retry other datanode!", err)
				outflag++
				continue
			} else {
				cfile.cfs.SetDataConn(addr, conn)

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
					cfile.cfs.DelDataConn(addr)
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

func (cfile *CFile) readBuffer(eInfo extentInfo, data *[]byte) int32 {

	bufLen := cfile.wBuffer.buffer.Len()
	if bufLen < int(eInfo.length) {
		logger.Error("cfile %v wBuffer length %v less than request %v", cfile.Name, bufLen, eInfo.length)
		return -1
	}
	wbuf := cfile.wBuffer.buffer.Next(int(eInfo.length))
	*data = append(*data, wbuf[:eInfo.length]...)
	cfile.wBuffer.buffer.Write(wbuf)
	logger.Debug("cfile %v read %v from wBuffer len: %v,%v success", cfile.Name, eInfo.length, bufLen, cfile.wBuffer.buffer.Len())
	return eInfo.length
}

func (cfile *CFile) readChunk(eInfo extentInfo, handleID fuse.HandleID, data *[]byte, offset int64) int32 {

	//check if hit readBuf
	readBufOffset := cfile.ReaderMap[handleID].LastOffset
	readBufLen := len(cfile.ReaderMap[handleID].readBuf)
	if offset >= readBufOffset && offset+int64(eInfo.length) <= readBufOffset+int64(readBufLen) {
		pos := int32(offset - readBufOffset)
		*data = append(*data, cfile.ReaderMap[handleID].readBuf[pos:pos+eInfo.length]...)
		return eInfo.length
	}

	//prepare to read from datanode
	cfile.ReaderMap[handleID].readBuf = []byte{}
	buffer := new(bytes.Buffer)
	cfile.ReaderMap[handleID].Ch = make(chan *bytes.Buffer)
	readSize := eInfo.length
	if readSize < 2097152 {
		readSize = 2097152
	}

	//go streamread
	go cfile.streamread(int(eInfo.pos), cfile.ReaderMap[handleID].Ch, int64(eInfo.offset), int64(readSize))
	buffer = <-cfile.ReaderMap[handleID].Ch
	bLen := buffer.Len()
	if bLen == 0 {
		logger.Error("try to read %v chunk:%v from datanode size:%v, but return:%v", cfile.Name, eInfo.pos, readSize, bLen)
		return -1
	}
	cfile.ReaderMap[handleID].readBuf = buffer.Next(bLen)
	cfile.ReaderMap[handleID].LastOffset = offset
	appendLen := eInfo.length
	if appendLen > int32(bLen) {
		appendLen = int32(bLen)
	}
	*data = append(*data, cfile.ReaderMap[handleID].readBuf[0:appendLen]...)
	buffer.Reset()
	buffer = nil
	return appendLen
}

const (
	ExtentFlagInvalid    = 0 //extent is invalid
	ExtentFlagOnDisk     = 1 //extent is on disk
	ExtentFlagInBuffer   = 2 //extent is in buffer
	ExtentFlagBeyondFile = 3 //extent is beyond file size
)

type extentInfo struct {
	flag   int32
	pos    int32 //pos in chunks of cfile
	offset int32 //offset in chunk or buffer
	length int32
}

//get extent info by [start, end)
func (cfile *CFile) getExtentInfo(start int64, end int64, eInfo []extentInfo) int32 {
	var i, c int32
	var chunkStart, chunkEnd int64

	//1.seek all on-disk chunks
	for i = 0; i < int32(len(cfile.chunks)); i++ {
		chunkEnd += int64(cfile.chunks[i].ChunkSize) //@chunkEnd is next chunk's @chunkStart

		if start < chunkEnd {
			eInfo[c].flag = ExtentFlagOnDisk
			eInfo[c].pos = i
			eInfo[c].offset = int32(start - chunkStart)
			if chunkEnd < end {
				eInfo[c].length = int32(chunkEnd - start)
				start = chunkEnd //update @start to next chunk
			} else {
				eInfo[c].length = int32(end - start)
				return 0 //reach @end, done
			}
			c++
		}

		chunkStart = chunkEnd
	}

	bufLen := cfile.wBuffer.buffer.Len()
	if start > chunkEnd+int64(bufLen) {
		logger.Error("cfile %v unsupport read/write %v beyond file size %v", cfile.Name, start, cfile.FileSize)
		return -1
	}

	//2.extent in wBuffer with @start to min(wBufferEnd, @end)
	if bufLen != 0 {
		if chunkEnd != cfile.wBuffer.startOffset {
			logger.Error("cfile %v chunkEnd:%v != wBuffer.startOffset:%v", cfile.Name, chunkEnd, cfile.wBuffer.startOffset)
			return -1
		}
		eInfo[c].flag = ExtentFlagInBuffer
		eInfo[c].pos = i
		eInfo[c].offset = int32(start - cfile.wBuffer.startOffset)

		eInfo[c].length = int32(bufLen) - eInfo[c].offset
		if eInfo[c].length > int32(end-start) {
			eInfo[c].length = int32(end - start)
		}
		start += int64(eInfo[c].length)
		c++
	}

	//3.extent beyond file size
	if start < end {
		eInfo[c].flag = ExtentFlagBeyondFile
		eInfo[c].pos = i
		eInfo[c].offset = 0
		eInfo[c].length = int32(end - start)
	}
	return 0
}

// Read ...
func (cfile *CFile) Read(handleID fuse.HandleID, data *[]byte, offset int64, readsize int64) int64 {

	if cfile.Status != 0 {
		logger.Error("cfile %v status error , read func return -2 ", cfile.Name)
		return -2
	}

	if offset >= cfile.FileSize {
		logger.Error("cfile %v unsupport read beyond file size return -3 ", cfile.Name)
		return -3
	}

	var ret int32
	eInfo := make([]extentInfo, 4)
	if ret = cfile.getExtentInfo(offset, offset+readsize, eInfo); ret < 0 {
		logger.Error("cfile %v getExtentInfo failed, return -3 ", cfile.Name)
		return -3
	}
	logger.Debug("cfile %v getExtentInfo for Read: offset: %v, len: %v, eInfo: %v", cfile.Name, offset, readsize, eInfo)

	var c int
	var doneFlag bool
	curOffset := offset
	for c = 0; c <= len(eInfo) && !doneFlag; c++ {
		ret = 0
		switch eInfo[c].flag {
		case ExtentFlagOnDisk:
			ret = cfile.readChunk(eInfo[c], handleID, data, curOffset)
			if ret < 0 {
				logger.Error("cfile %v readChunk failed %v", cfile.Name, eInfo[c])
				return -1
			}
			if ret != eInfo[c].length {
				logger.Error("cfile %v eInfo:%v, readChunk ret %v", cfile.Name, eInfo[c], ret)
				doneFlag = true
			}

		case ExtentFlagInBuffer:
			ret = cfile.readBuffer(eInfo[c], data)
			if ret < 0 {
				logger.Error("cfile %v readBuffer failed %v", cfile.Name, eInfo[c])
				return -1
			}

		case ExtentFlagBeyondFile:
			logger.Debug("cfile %v read %v:%v beyond file size %v, do nothing...", cfile.Name, offset, readsize, cfile.FileSize)
			doneFlag = true

		default:
			doneFlag = true
		}

		curOffset += int64(ret)
	}
	return curOffset - offset
}

// Write ...
func (cfile *CFile) Write(buf []byte, offset int64, length int32) int32 {

	if cfile.Status != 0 {
		logger.Error("cfile %v status error , Write func return -2 ", cfile.Name)
		return -2
	}

	if offset > cfile.FileSize {
		logger.Error("cfile %v unsupport write beyond file size return -3 ", cfile.Name)
		return -3
	}

	if offset == cfile.FileSize {
		logger.Debug("cfile %v write append only: offset %v, length %v", cfile.Name, offset, length)
		return cfile.appendWrite(buf, length)
	}

	eInfo := make([]extentInfo, 4)
	if ret := cfile.getExtentInfo(offset, offset+int64(length), eInfo); ret < 0 {
		logger.Error("cfile %v getExtentInfo failed, return -3 ", cfile.Name)
		return -3
	}
	logger.Debug("cfile %v getExtentInfo for Write: offset: %v, len: %v, eInfo: %v", cfile.Name, offset, length, eInfo)

	var c int
	var ret, pos int32
	var doneFlag bool
	for c = 0; c <= len(eInfo) && !doneFlag; c++ {
		switch eInfo[c].flag {
		case ExtentFlagOnDisk:
			ret = cfile.seekWrite(eInfo[c], buf[pos:(pos+eInfo[c].length)])
			if ret < 0 {
				logger.Error("cfile %v doOverwrite failed %v", cfile.Name, eInfo[c])
				return -1
			}

		case ExtentFlagInBuffer:
			ret = cfile.overwriteBuffer(eInfo[c], buf[pos:(pos+eInfo[c].length)])
			if ret < 0 {
				logger.Error("cfile %v doOverwrite failed %v", cfile.Name, eInfo[c])
				return -1
			}

		case ExtentFlagBeyondFile:
			ret = cfile.appendWrite(buf[pos:(pos+eInfo[c].length)], eInfo[c].length)
			if ret < 0 {
				logger.Error("cfile %v Write failed %v", cfile.Name, eInfo[c])
				return -1
			}

		default:
			doneFlag = true
		}

		pos += eInfo[c].length
	}
	return length
}

func (cfile *CFile) overwriteBuffer(eInfo extentInfo, buf []byte) int32 {

	//read wBuffer all bytes to tmpBuf
	bufLen := cfile.wBuffer.buffer.Len()
	tmpBuf := cfile.wBuffer.buffer.Next(bufLen)
	if len(tmpBuf) != bufLen {
		logger.Error("cfile %v read wBuffer len: %v return: %v ", cfile.Name, bufLen, len(tmpBuf))
		return -1
	}

	//copy buf to tmpBuf
	n := copy(tmpBuf[eInfo.offset:], buf)
	if n != int(eInfo.length) {
		logger.Error("cfile %v copy to wBuffer len: %v return n: %v", cfile.Name, eInfo.length, n)
		return -1
	}

	//write to wBuffer
	cfile.wBuffer.buffer.Reset()
	n, err := cfile.wBuffer.buffer.Write(tmpBuf)
	if n != int(bufLen) || err != nil {
		logger.Error("cfile %v write wBuffer len: %v return n: %v err %v", cfile.Name, bufLen, n, err)
		return -1
	}

	return 0
}

func (cfile *CFile) seekWrite(eInfo extentInfo, buf []byte) int32 {

	chunkInfo := cfile.chunks[eInfo.pos]
	var copies uint64

	for i := range chunkInfo.BGP.Blocks {

		ip := chunkInfo.BGP.Blocks[i].Ip
		port := chunkInfo.BGP.Blocks[i].Port
		addr := ip + ":" + strconv.Itoa(int(port))

		conn, err := cfile.cfs.GetDataConn(addr)
		if err != nil || conn == nil {
			conn, err = DialData(addr)
			if err == nil && conn != nil {
				logger.Debug("new datanode conn !!!")
				cfile.cfs.SetDataConn(addr, conn)
			} else {
				logger.Error("new conn to %v failed", addr)
				return -1
			}
		} else {
			//logger.Debug("reuse datanode conn !!!")
		}
	}

	for i := range chunkInfo.BGP.Blocks {

		ip := chunkInfo.BGP.Blocks[i].Ip
		port := chunkInfo.BGP.Blocks[i].Port
		addr := ip + ":" + strconv.Itoa(int(port))

		conn, _ := cfile.cfs.GetDataConn(addr)

		pSeekWriteChunkReq := &dp.SeekWriteChunkReq{
			ChunkID:     chunkInfo.ChunkID,
			BlockID:     chunkInfo.BGP.Blocks[i].BlkID,
			Databuf:     buf,
			ChunkOffset: int64(eInfo.offset),
		}

		cfile.wgWriteReps.Add(1)

		go cfile.seekWriteChunk(addr, conn, pSeekWriteChunkReq, &copies)

	}

	cfile.wgWriteReps.Wait()

	if copies < 3 {
		cfile.Status = 1
		logger.Error("cfile %v seekWriteChunk copies: %v, set error!", cfile.Name, copies)
		return -1
	}
	return 0
}

// Write only appending file...
func (cfile *CFile) appendWrite(buf []byte, length int32) int32 {

	if cfile.Status != 0 {
		logger.Error("cfile %v status error , Write func return -2 ", cfile.Name, cfile.Status)
		return -2
	}

	var ret, w int32
	w = 0
	for w < length {
		if (cfile.FileSize%chunkSize) == 0 || cfile.wBuffer.chunkInfo.ChunkSize+length > chunkSize {
			logger.Debug("cfile %v need a new chunk...filesize: %v", cfile.Name, cfile.FileSize)
			ret, cfile.wBuffer.chunkInfo = cfile.AllocateChunk()
			if ret != 0 {
				if ret == 28 /*ENOSPC*/ {
					return -1
				}
				return -2
			}
		}
		if cfile.wBuffer.freeSize == 0 {
			cfile.wBuffer.buffer = new(bytes.Buffer)
			cfile.wBuffer.freeSize = BufferSize
		}
		// set startOffset when writing first byte
		if cfile.wBuffer.buffer.Len() == 0 {
			cfile.wBuffer.startOffset = cfile.FileSize
		}
		if length-w < cfile.wBuffer.freeSize {
			if length != w {
				cfile.wBuffer.buffer.Write(buf[w:length])
				cfile.wBuffer.freeSize = cfile.wBuffer.freeSize - (length - w)
				//cfile.wBuffer.startOffset = cfile.FileSize
				cfile.FileSize = cfile.FileSize + int64(length-w)
				cfile.wBuffer.endOffset = cfile.FileSize
				//cfile.wBuffer.chunkInfo.ChunkSize = cfile.wBuffer.chunkInfo.ChunkSize + int32(length-w)
				w = length
			}
			break
		} else {
			cfile.wBuffer.buffer.Write(buf[w : w+cfile.wBuffer.freeSize])
			w = w + cfile.wBuffer.freeSize
			//cfile.wBuffer.startOffset = cfile.FileSize
			cfile.FileSize = cfile.FileSize + int64(cfile.wBuffer.freeSize)
			cfile.wBuffer.endOffset = cfile.FileSize
			//cfile.wBuffer.chunkInfo.ChunkSize = cfile.wBuffer.chunkInfo.ChunkSize + int32(cfile.wBuffer.freeSize)
			cfile.wBuffer.freeSize = 0
		}

		if cfile.wBuffer.freeSize == 0 {

			ret := cfile.push()
			if ret != 0 {
				logger.Error("cfile %v push error", cfile.Name)
				return -1
			}
		}
	}

	return w
}

func (cfile *CFile) push() int32 {

	if cfile.Status != 0 {
		logger.Error("cfile %v status error , push func return err ", cfile.Name)
		return -1
	}

	if cfile.wBuffer.chunkInfo == nil {
		logger.Error("cfile %v chunkInfo is nil", cfile.Name)
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

		logger.Debug("Flush!!!!! len %v name %v pinode %v", cfile.wBuffer.buffer.Len(), cfile.Name, cfile.ParentInodeID)

		return cfile.send(&wBuffer)
	}

	return 0
}

func (cfile *CFile) seekWriteChunk(addr string, conn *grpc.ClientConn, req *dp.SeekWriteChunkReq, copies *uint64) {

	if conn == nil {
	} else {
		dc := dp.NewDataNodeClient(conn)
		ctx, _ := context.WithTimeout(context.Background(), 5*time.Second)
		ret, err := dc.SeekWriteChunk(ctx, req)
		if err != nil {
			logger.Error("SeekWriteChunk err %v", err)
			conn.Close()
			cfile.cfs.DelDataConn(addr)
		} else {
			if ret.Ret != 0 {
			} else {
				atomic.AddUint64(copies, 1)
			}
		}
	}
	cfile.wgWriteReps.Add(-1)

}

func (cfile *CFile) writeChunk(addr string, conn *grpc.ClientConn, req *dp.WriteChunkReq, copies *uint64) {

	if conn == nil {
	} else {
		dc := dp.NewDataNodeClient(conn)
		ctx, _ := context.WithTimeout(context.Background(), 5*time.Second)
		ret, err := dc.WriteChunk(ctx, req)
		if err != nil {
			logger.Error("WriteChunk err %v", err)
			conn.Close()
			cfile.cfs.DelDataConn(addr)
		} else {
			if ret.Ret != 0 {
			} else {
				atomic.AddUint64(copies, 1)
			}
		}
	}
	cfile.wgWriteReps.Add(-1)

}

func (cfile *CFile) send(v *wBuffer) int32 {

	sendLen := v.buffer.Len()
	if sendLen <= 0 {
		return 0
	}

	dataBuf := v.buffer.Next(v.buffer.Len())

	var chunkWriteFlag bool

	for cnt := 0; cnt < 10; cnt++ {

		var copies uint64

		for i := range v.chunkInfo.BGP.Blocks {

			ip := v.chunkInfo.BGP.Blocks[i].Ip
			port := v.chunkInfo.BGP.Blocks[i].Port
			addr := ip + ":" + strconv.Itoa(int(port))

			conn, err := cfile.cfs.GetDataConn(addr)
			if err != nil || conn == nil {
				conn, err = DialData(addr)
				if err == nil && conn != nil {
					logger.Debug("new datanode conn !!!")
					cfile.cfs.SetDataConn(addr, conn)
				} else {
					logger.Error("new conn to %v failed", addr)
					goto fail
				}
			} else {
				//logger.Debug("reuse datanode conn !!!")
			}
		}
		for i := range v.chunkInfo.BGP.Blocks {

			ip := v.chunkInfo.BGP.Blocks[i].Ip
			port := v.chunkInfo.BGP.Blocks[i].Port
			addr := ip + ":" + strconv.Itoa(int(port))

			conn, _ := cfile.cfs.GetDataConn(addr)

			pWriteChunkReq := &dp.WriteChunkReq{
				ChunkID: v.chunkInfo.ChunkID,
				BlockID: v.chunkInfo.BGP.Blocks[i].BlkID,
				Databuf: dataBuf,
			}

			cfile.wgWriteReps.Add(1)

			go cfile.writeChunk(addr, conn, pWriteChunkReq, &copies)

		}

		cfile.wgWriteReps.Wait()

	fail:
		if copies < 3 {
			var ret int32
			ret, cfile.wBuffer.chunkInfo = cfile.AllocateChunk()
			//cfile.wBuffer.chunkInfo.ChunkSize = int32(sendLen)
			v.chunkInfo = cfile.wBuffer.chunkInfo
			if ret != 0 {
				cfile.Status = 1
				return cfile.Status
			}
			logger.Debug("write 3 copies failed ,choose new chunk! cnt=%v", cnt)
			continue
		} else {
			chunkWriteFlag = true
			break
		}

	}

	if !chunkWriteFlag {
		cfile.Status = 1
		return cfile.Status
	}

	pSyncChunkReq := &mp.SyncChunkReq{
		ParentInodeID: cfile.ParentInodeID,
		Name:          cfile.Name,
		VolID:         cfile.cfs.VolID,
	}

	var tmpChunkInfo mp.ChunkInfo
	tmpChunkInfo.ChunkSize = v.chunkInfo.ChunkSize + int32(sendLen) //we do not update ChunkSize when write to wBuffer
	tmpChunkInfo.ChunkID = v.chunkInfo.ChunkID
	tmpChunkInfo.BlockGroupID = v.chunkInfo.BGP.Blocks[0].BGID
	pSyncChunkReq.ChunkInfo = &tmpChunkInfo

	wflag := false

	for i := 0; i < 10; i++ {
		if cfile.cfs.Conn != nil {
			break
		}
		time.Sleep(300 * time.Millisecond)
		continue
	}
	if cfile.cfs.Conn == nil {
		cfile.Status = 1
		return cfile.Status
	}

	mc := mp.NewMetaNodeClient(cfile.cfs.Conn)
	ctx, _ := context.WithTimeout(context.Background(), 5*time.Second)
	pSyncChunkAck, err := mc.SyncChunk(ctx, pSyncChunkReq)
	if err == nil && pSyncChunkAck.Ret == 0 {
		wflag = true
	} else {
		logger.Error("SyncChunk failed start to try ,name %v,inode %v,pinode %v", cfile.Name, cfile.Inode, cfile.ParentInodeID)

		for i := 0; i < 15; i++ {

			time.Sleep(time.Second)

			for i := 0; i < 10; i++ {
				if cfile.cfs.Conn != nil {
					break
				}
				time.Sleep(300 * time.Millisecond)
				continue
			}
			if cfile.cfs.Conn == nil {
				cfile.Status = 1
				return cfile.Status
			}

			logger.Error("SyncChunk try %v times", i+1)

			mc := mp.NewMetaNodeClient(cfile.cfs.Conn)
			ctx, _ := context.WithTimeout(context.Background(), 5*time.Second)
			pSyncChunkAck, err := mc.SyncChunk(ctx, pSyncChunkReq)
			if err == nil && pSyncChunkAck.Ret == 0 {
				wflag = true
				break
			} else {
				logger.Error("SyncChunk grpc func  try %v times ,err %v", i+1, err)
				if pSyncChunkAck != nil {
					logger.Error("SyncChunk grpc func  try %v times ,ret %v", i+1, pSyncChunkAck.Ret)
				}
			}
		}

	}
	if !wflag {
		cfile.Status = 1
		return cfile.Status
	}

	chunkNum := len(cfile.chunks)
	//v.chunkInfo.Status = tmpChunkInfo.Status
	if chunkNum != 0 && cfile.chunks[chunkNum-1].ChunkID == v.chunkInfo.ChunkID {
		cfile.chunks[chunkNum-1].ChunkSize += int32(sendLen) // update ChunkSize only if chunk's data has be writted to datanode
		//cfile.chunks[chunkNum-1].Status = v.chunkInfo.Status
	} else {
		cfile.chunks = append(cfile.chunks, v.chunkInfo)
		cfile.chunks[chunkNum].ChunkSize = int32(sendLen) //set ChunkSize when write chunk to datanode first time
	}
	return cfile.Status
}

// Sync ...
func (cfile *CFile) Sync() int32 {
	return 0
}

// Close ...
func (cfile *CFile) Close(flags int) int32 {
	return 0
}

// ProcessLocalBuffer ...
func ProcessLocalBuffer(buffer []byte, cfile *CFile) {
	cfile.appendWrite(buffer, int32(len(buffer)))
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
