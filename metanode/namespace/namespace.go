package namespace

import (
	pbproto "github.com/golang/protobuf/proto"
	"github.com/ipdcode/containerfs/logger"
	"github.com/ipdcode/containerfs/metanode/raftopt"
	mp "github.com/ipdcode/containerfs/proto/mp"
	vp "github.com/ipdcode/containerfs/proto/vp"
	"github.com/ipdcode/raft"
	"github.com/ipdcode/raft/proto"
	"github.com/ipdcode/raft/storage/wal"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"path"
	"runtime/debug"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	//BlockGroupSize 5GB
	BlockGroupSize = 5 * 1024 * 1024 * 1024
	//ChunkSize 64MB
	ChunkSize = 64 * 1024 * 1024
)

//VolMgrAddress ...
var VolMgrAddress string

type nameSpace struct {
	sync.RWMutex
	VolID       string
	RaftGroupID uint64
	RaftGroup   *raftopt.KvStateMachine
	RaftStorage *wal.Storage
}

//AllNameSpace ...
var AllNameSpace map[string]*nameSpace
var gMutex sync.RWMutex

func catchPanic() {
	if err := recover(); err != nil {
		logger.Error("panic !!! :%v", err)
		logger.Error("stacks:%v", string(debug.Stack()))
	}
}

//CreateGNameSpace ...
func CreateGNameSpace() {
	gMutex.Lock()
	AllNameSpace = make(map[string]*nameSpace)
	gMutex.Unlock()
}

func createRaftGroup(rs *raft.RaftServer, peers []proto.Peer, nodeID uint64, dir string, UUID string, raftGroupID uint64) (*raftopt.KvStateMachine, *wal.Storage, error) {
	sm, sg, err := raftopt.CreateKvStateMachine(rs, peers, nodeID, dir, UUID, raftGroupID)
	if err != nil {
		return nil, nil, err
	}
	return sm, sg, nil
}

func initNameSpace(rs *raft.RaftServer, nameSpace *nameSpace, UUID string) int32 {

	defer catchPanic()

	time.Sleep(time.Second * 2)

	var flag bool
	for i := 0; i < 3; i++ {
		if !rs.IsLeader(nameSpace.RaftGroupID) {
			time.Sleep(time.Second * 1)
			continue
		} else {
			flag = true
			break
		}
	}
	if !flag {
		return 0
	}

	ret, tmpBlockGroups := nameSpace.GetVolInfo(UUID)
	if ret != 0 {
		return ret
	}

	if len(tmpBlockGroups) <= 0 {
		return 0
	}

	for _, v := range tmpBlockGroups {
		v.FreeSize = BlockGroupSize
		err := nameSpace.BlockGroupDBSet(v.BlockGroupID, nameSpace.BlockGroupVp2Mp(v))
		if err != nil {
			continue
		}
	}

	tmpInodeInfo := mp.InodeInfo{
		AccessTime: time.Now().Unix(),
		ModifiTime: time.Now().Unix(),
	}

	err := nameSpace.InodeDBSet(0, &tmpInodeInfo)
	if err != nil {
		return 1
	}

	return 0
}

//CreateNameSpace ...
func CreateNameSpace(rs *raft.RaftServer, peers []proto.Peer, nodeID uint64, dir string, UUID string, raftGroupID uint64, IsLoad bool) int32 {

	defer catchPanic()

	var err error
	var errno int32

	nameSpace := nameSpace{}
	nameSpace.VolID = UUID
	nameSpace.RaftGroupID = raftGroupID
	nameSpace.RaftGroup, nameSpace.RaftStorage, err = createRaftGroup(rs, peers, nodeID, dir, UUID, nameSpace.RaftGroupID)
	if err != nil {
		logger.Error("createRaftGroup, failed,err:%v", err)
		errno = -1
		return errno
	}

	logger.Info("createRaftGroup, success")

	gMutex.Lock()
	AllNameSpace[UUID] = &nameSpace
	gMutex.Unlock()

	if !IsLoad {
		go initNameSpace(rs, &nameSpace, UUID)
	}
	return errno
}

//SnapShootNameSpace ...
func SnapShootNameSpace(rs *raft.RaftServer, UUID string, dir string) int32 {

	defer catchPanic()

	ret, nameSpace := GetNameSpace(UUID)
	if ret != 0 {
		return ret
	}
	raftopt.TakeKvSnapShoot(nameSpace.RaftGroup, nameSpace.RaftStorage, path.Join(dir, UUID, "wal", "snap"))
	return 0
}

//DeleteNameSpace ...
func DeleteNameSpace(rs *raft.RaftServer, UUID string) int32 {

	defer catchPanic()

	ret, nameSpace := GetNameSpace(UUID)
	if ret != 0 {
		return 0
	}
	rs.RemoveRaft(nameSpace.RaftGroupID)

	gMutex.Lock()
	delete(AllNameSpace, UUID)
	gMutex.Unlock()
	return 0
}

//GetNameSpace ...
func GetNameSpace(UUID string) (int32, *nameSpace) {

	defer catchPanic()

	gMutex.RLock()

	if v, ok := AllNameSpace[UUID]; ok {
		gMutex.RUnlock()
		return 0, v
	}
	gMutex.RUnlock()
	return -1, nil
}

//GetFSInfo ...
func (ns *nameSpace) GetFSInfo(volID string) mp.GetFSInfoAck {

	defer catchPanic()

	ack := mp.GetFSInfoAck{}
	var totalSpace uint64
	var freeSpace uint64

	ns.RaftGroup.BlockGroupLocker.RLock()

	bgmap, _ := ns.BlockGroupDBGetAll()

	var blockGroup mp.BlockGroup

	for _, v := range *bgmap {

		err := pbproto.Unmarshal(v, &blockGroup)
		if err != nil {
			continue
		}
		totalSpace = totalSpace + BlockGroupSize
		freeSpace = freeSpace + uint64(blockGroup.FreeSize)
	}

	ns.RaftGroup.BlockGroupLocker.RUnlock()

	ack.TotalSpace = totalSpace
	ack.FreeSpace = freeSpace
	ack.Ret = 0

	return ack
}

//GetFSInfo ...
func (ns *nameSpace) ExpandNameSpace(blockGroups []*mp.BlockGroup) int32 {

	defer catchPanic()

	logger.Debug("ExpandNameSpace %v , blockgroups num %v", blockGroups, len(blockGroups))

	for _, v := range blockGroups {
		v.FreeSize = BlockGroupSize
		err := ns.BlockGroupDBSet(v.BlockGroupID, v)
		if err != nil {
			return -1
		}
	}
	return 0
}

// GetVolInfo ...
func (ns *nameSpace) GetVolInfo(name string) (int32, []*vp.BlockGroup) {

	defer catchPanic()

	conn, err := grpc.Dial(VolMgrAddress, grpc.WithInsecure())
	if err != nil {
		logger.Error("Dial failed: %v", err)
		return -1, nil
	}
	defer conn.Close()
	vc := vp.NewVolMgrClient(conn)
	pGetVolInfoReq := &vp.GetVolInfoReq{UUID: name}
	pGetVolInfoAck, _ := vc.GetVolInfo(context.Background(), pGetVolInfoReq)
	if pGetVolInfoAck.Ret != 0 {
		logger.Error("GetVolInfo failed: %v", pGetVolInfoAck.Ret)
		return pGetVolInfoAck.Ret, nil
	}
	return 0, pGetVolInfoAck.VolInfo.BlockGroups
}

//GetVolList ...
func GetVolList() (int32, []*vp.VolIDs) {

	defer catchPanic()

	conn, err := grpc.Dial(VolMgrAddress, grpc.WithInsecure())
	if err != nil {
		logger.Error("Dial failed: %v", err)
		return -1, nil
	}
	defer conn.Close()
	vc := vp.NewVolMgrClient(conn)
	pGetVolListReq := &vp.GetVolListReq{}
	pGetVolListAck, err := vc.GetVolList(context.Background(), pGetVolListReq)
	if err != nil {
		return -1, nil
	}
	if pGetVolListAck.Ret != 0 {
		logger.Error("GetVolList failed: %v", pGetVolListAck.Ret)
		return pGetVolListAck.Ret, nil
	}
	return 0, pGetVolListAck.VolIDs
}

//CreateDirDirect ...
func (ns *nameSpace) CreateDirDirect(pinode uint64, name string) (int32, uint64) {

	defer catchPanic()

	/*update inode info*/
	inodeID, err := ns.AllocateInodeID()
	if err != nil {
		return 2, 0
	}
	tmpInodeInfo := mp.InodeInfo{
		AccessTime: time.Now().Unix(),
		ModifiTime: time.Now().Unix(),
	}

	err = ns.InodeDBSet(inodeID, &tmpInodeInfo)
	if err != nil {
		return 1, 0
	}

	err = ns.DentryDBSet(strconv.FormatUint(pinode, 10)+"-"+name, false, inodeID)
	if err != nil {
		ns.InodeDBDelete(inodeID)
		return 1, 0
	}

	return 0, inodeID
}

//GetInodeInfoDirect ...
func (ns *nameSpace) GetInodeInfoDirect(pinode uint64, name string) (int32, *mp.InodeInfo, uint64) {

	defer catchPanic()

	var ok bool
	var pInodeInfo *mp.InodeInfo

	ok, dirent := ns.DentryDBGet(strconv.FormatUint(pinode, 10) + "-" + name)
	if !ok {
		return -1, nil, 0
	}

	if ok, pInodeInfo = ns.InodeDBGet(dirent.Inode); !ok {
		return 2, nil, 0
	}
	return 0, pInodeInfo, dirent.Inode
}

//StatDirect ...
func (ns *nameSpace) StatDirect(pinode uint64, name string) (bool, uint64, int32) {

	defer catchPanic()

	var ok bool

	ok, dirent := ns.DentryDBGet(strconv.FormatUint(pinode, 10) + "-" + name)
	if !ok {
		return false, 0, 2
	}

	return dirent.InodeType, dirent.Inode, 0
}

//ListDirect ...
func (ns *nameSpace) ListDirect(pinode uint64) ([]*mp.DirentN, int32) {

	//defer catchPanic()

	var tmpDirents []*mp.DirentN

	pinodePrefix := strconv.FormatUint(pinode, 10) + "-"

	allMap, _ := ns.RaftGroup.DentryGetAll(ns.RaftGroupID)

	ns.RaftGroup.DentryLocker.RLock()
	for k, v := range *allMap {

		idex := strings.Index(k, "-")
		runes := []rune(k)

		if string(runes[0:idex+1]) == pinodePrefix {

			name := string(runes[idex+1:])

			dirent := mp.Dirent{}
			pbproto.Unmarshal(v, &dirent)

			direntN := mp.DirentN{Name: name, Inode: dirent.Inode, InodeType: dirent.InodeType}
			tmpDirents = append(tmpDirents, &direntN)

		}
	}
	ns.RaftGroup.DentryLocker.RUnlock()

	return tmpDirents, 0
}

//DeleteDirDirect ...
func (ns *nameSpace) DeleteDirDirect(pinode uint64, name string) int32 {

	defer catchPanic()

	ok, dirent := ns.DentryDBGet(strconv.FormatUint(pinode, 10) + "-" + name)
	if !ok {
		return 1
	}
	ns.InodeDBDelete(dirent.Inode)
	ns.DentryDBDelete(strconv.FormatUint(pinode, 10) + "-" + name)

	return 0
}

//RenameDirect ...
func (ns *nameSpace) RenameDirect(oldpinode uint64, oldName string, newpinode uint64, newName string) int32 {

	defer catchPanic()

	oldDentryKey := strconv.FormatUint(oldpinode, 10) + "-" + oldName
	newDentryKey := strconv.FormatUint(newpinode, 10) + "-" + newName

	ok, dirent := ns.DentryDBGet(oldDentryKey)
	if !ok {
		return 1
	}

	err := ns.DentryDBSet(newDentryKey, dirent.InodeType, dirent.Inode)
	if err != nil {
		return 1
	}
	err = ns.DentryDBDelete(oldDentryKey)
	if err != nil {
		ns.DentryDBDelete(newDentryKey)
		return 1
	}
	return 0
}

//CreateFileDirect ...
func (ns *nameSpace) CreateFileDirect(pinode uint64, name string) (int32, uint64) {

	defer catchPanic()

	/*update inode info*/
	inodeID, err := ns.AllocateInodeID()
	if err != nil {
		return 1, 0
	}
	tmpInodeInfo := mp.InodeInfo{
		AccessTime: time.Now().Unix(),
		ModifiTime: time.Now().Unix(),
	}

	err = ns.InodeDBSet(inodeID, &tmpInodeInfo)
	if err != nil {
		return 1, 0
	}

	tmpKey := strconv.FormatUint(pinode, 10) + "-" + name
	err = ns.DentryDBSet(tmpKey, true, inodeID)
	if err != nil {
		ns.InodeDBDelete(inodeID)
		return 1, 0
	}

	return 0, inodeID
}

//DeleteFileDirect ...
func (ns *nameSpace) DeleteFileDirect(pinode uint64, name string) int32 {

	defer catchPanic()

	ok, dirent := ns.DentryDBGet(strconv.FormatUint(pinode, 10) + "-" + name)
	if !ok {
		return 1
	}
	ok, pInodeInfo := ns.InodeDBGet(dirent.Inode)
	if !ok {
		return 1
	}

	if pInodeInfo.Chunks != nil {
		for _, v := range pInodeInfo.Chunks {
			ns.ReleaseBlockGroup(v.BlockGroupID, v.ChunkSize)
		}
	}

	ns.InodeDBDelete(dirent.Inode)
	ns.DentryDBDelete(strconv.FormatUint(pinode, 10) + "-" + name)

	return 0
}

//GetFileChunksDirect ...
func (ns *nameSpace) GetFileChunksDirect(pinode uint64, name string) (int32, []*mp.ChunkInfo, uint64) {

	defer catchPanic()

	ok, dirent := ns.DentryDBGet(strconv.FormatUint(pinode, 10) + "-" + name)
	if !ok {
		return 1, nil, 0
	}
	ok, pInodeInfo := ns.InodeDBGet(dirent.Inode)
	if !ok {
		return 1, nil, 0
	}
	return 0, pInodeInfo.Chunks, dirent.Inode
}

//AllocateChunk ...
func (ns *nameSpace) AllocateChunk() (int32, *mp.ChunkInfo) {

	defer catchPanic()

	/*
		var ret int32

		key := strconv.FormatUint(pinode, 10) + "-" + name

		fmt.Println("AllocateChunk...")

		ok, dirent := ns.DentryDBGet(key)
		if !ok {
			ret = 2 //ENOENT
			return ret, nil
		}

		ok, inodeInfo := ns.InodeDBGet(dirent.Inode)
		if !ok {
			ret = 2 //ENOENT
			return ret, nil
		}

	*/

	var chunkInfo = mp.ChunkInfo{}
	ret, _, blockGroup := ns.ChooseBlockGroup()

	if ret != 0 {
		return 28, nil //ENOSPC
	}
	chunkInfo.BlockGroupID = blockGroup.BlockGroupID
	chunkInfo.ChunkSize = 0

	var err error
	chunkInfo.ChunkID, err = ns.AllocateChunkID()
	if err != nil {
		return 1, nil
	}

	//inodeInfo.Chunks = append(inodeInfo.Chunks, &chunkInfo)
	//ns.InodeDBSet(dirent.Inode, inodeInfo)

	return 0, &chunkInfo

}

//SyncChunk ...
func (ns *nameSpace) SyncChunk(pinode uint64, name string, chunkinfo *mp.ChunkInfo) int32 {

	defer catchPanic()

	var ret int32

	key := strconv.FormatUint(pinode, 10) + "-" + name

	ok, dirent := ns.DentryDBGet(key)
	if !ok {
		ret = 2 /*ENOENT*/
		return ret
	}

	ok, inodeInfo := ns.InodeDBGet(dirent.Inode)
	if !ok {
		ret = 2 /*ENOENT*/
		return ret
	}

	inodeInfo.ModifiTime = time.Now().Unix()

	var lastChunkID uint64
	var blockGroupUsed int32
	if len(inodeInfo.Chunks) > 0 {
		//for appned write
		lastChunkID = inodeInfo.Chunks[len(inodeInfo.Chunks)-1].ChunkID
		if lastChunkID == chunkinfo.ChunkID {
			inodeInfo.FileSize = inodeInfo.FileSize + int64(chunkinfo.ChunkSize) - int64(inodeInfo.Chunks[len(inodeInfo.Chunks)-1].ChunkSize)
			blockGroupUsed = chunkinfo.ChunkSize - inodeInfo.Chunks[len(inodeInfo.Chunks)-1].ChunkSize
			inodeInfo.Chunks[len(inodeInfo.Chunks)-1] = chunkinfo
		} else {
			inodeInfo.Chunks = append(inodeInfo.Chunks, chunkinfo)
			inodeInfo.FileSize += int64(chunkinfo.ChunkSize)
			blockGroupUsed = chunkinfo.ChunkSize
		}
	} else {
		inodeInfo.Chunks = append(inodeInfo.Chunks, chunkinfo)
		inodeInfo.FileSize += int64(chunkinfo.ChunkSize)
		blockGroupUsed = chunkinfo.ChunkSize
	}

	err := ns.InodeDBSet(dirent.Inode, inodeInfo)
	if err != nil {

		return 1
	}

	ns.Lock()

	var pTmpBlockGroup *mp.BlockGroup
	if ok, pTmpBlockGroup = ns.BlockGroupDBGet(chunkinfo.BlockGroupID); !ok {
		ns.Unlock()
		return 2
	}

	pTmpBlockGroup.FreeSize = pTmpBlockGroup.FreeSize - int64(blockGroupUsed)

	if pTmpBlockGroup.FreeSize <= ChunkSize {
		pTmpBlockGroup.Status = blockGroupFull
	}

	err = ns.BlockGroupDBSet(chunkinfo.BlockGroupID, pTmpBlockGroup)
	if err != nil {
		ns.Unlock()
		return 1
	}

	ns.Unlock()
	return 0

}

//BlockGroupVp2Mp ...
func (ns *nameSpace) BlockGroupVp2Mp(in *vp.BlockGroup) *mp.BlockGroup {

	defer catchPanic()

	var mpBlockGroup = mp.BlockGroup{}

	mpBlockInfos := make([]*mp.BlockInfo, len(in.BlockInfos))

	mpBlockGroup.BlockGroupID = in.BlockGroupID
	mpBlockGroup.FreeSize = in.FreeSize
	mpBlockGroup.Status = in.Status

	for i := range in.BlockInfos {
		var pVpBlockInfo *vp.BlockInfo
		var mpBlockInfo mp.BlockInfo

		pVpBlockInfo = in.BlockInfos[i]
		mpBlockInfo.BlockID = pVpBlockInfo.BlockID
		mpBlockInfo.DataNodeIP = pVpBlockInfo.DataNodeIP
		mpBlockInfo.DataNodePort = pVpBlockInfo.DataNodePort

		mpBlockInfos[i] = &mpBlockInfo

	}

	mpBlockGroup.BlockInfos = mpBlockInfos
	return &mpBlockGroup

}

const (
	blockGroupFree = 0
	blockGroupFull = 2
)

//ChooseBlockGroup ...
func (ns *nameSpace) ChooseBlockGroup() (int32, uint32, *mp.BlockGroup) {

	defer catchPanic()

	var blockGroup mp.BlockGroup
	flag := false

	ns.RaftGroup.BlockGroupLocker.RLock()

	bgmap, _ := ns.BlockGroupDBGetAll()

	for _, v := range *bgmap {

		err := pbproto.Unmarshal(v, &blockGroup)
		if err != nil {
			continue
		}

		if blockGroup.Status == blockGroupFull {
			continue
		}

		blksBadFlag := false
		for _, v := range blockGroup.BlockInfos {
			if v.Status != 0 {
				blksBadFlag = true
				break
			}
		}
		if blksBadFlag {
			continue
		}

		logger.Debug("find a blockgroup,blgid:%v\n", blockGroup.BlockGroupID)
		flag = true
		break
	}

	ns.RaftGroup.BlockGroupLocker.RUnlock()

	if flag {
		//ns.BlockGroupDBSet(blockGroup.BlockGroupID, &blockGroup)
		return 0, blockGroup.BlockGroupID, &blockGroup
	}
	return 1, 0, nil
}

//ReleaseBlockGroup ...
func (ns *nameSpace) ReleaseBlockGroup(blockGroupID uint32, chunSize int32) {

	ns.Lock()
	defer ns.Unlock()
	defer catchPanic()

	ok, blockGroup := ns.BlockGroupDBGet(blockGroupID)
	if !ok {
		return
	}

	blockGroup.FreeSize = blockGroup.FreeSize + int64(chunSize)

	/*
		if blockGroup.FreeSize > BlockGroupSize {
			blockGroup.FreeSize = BlockGroupSize
		}
	*/

	if blockGroup.FreeSize > int64(ChunkSize) {

		if blockGroup.Status == blockGroupFull {
			blockGroup.Status = blockGroupFree
		}

	}

	ns.BlockGroupDBSet(blockGroupID, blockGroup)

}

func (ns *nameSpace) UpdateBlockGroup(blkinfo []*mp.BlkInfo) int32 {

	ns.Lock()

	logger.Debug("UpdateBlockGroup blkinfo :%v ", blkinfo)

	for _, v := range blkinfo {
		ok, blockGroup := ns.BlockGroupDBGet(v.BgpID)
		if !ok {
			continue
		}
		for i, vv := range blockGroup.BlockInfos {
			if vv.BlockID == v.BlockID {
				blockGroup.BlockInfos[i].Status = v.Status
				break
			}
		}
		ns.BlockGroupDBSet(v.BgpID, blockGroup)
	}
	ns.Unlock()

	return 0

}
func (ns *nameSpace) MigrateBlockGroup(blockGroupID uint32, oldBlockID uint32, newBlock *mp.BlockInfo) int32 {

	ns.Lock()
	defer ns.Unlock()

	logger.Debug("MigrateBlockGroup Start ...")

	ok, blockGroup := ns.BlockGroupDBGet(blockGroupID)
	if !ok {
		return -1
	}

	logger.Debug("MigrateBlockGroup befor , blockgroup %v", blockGroup)

	var newBlockInfos []*mp.BlockInfo

	for _, v := range blockGroup.BlockInfos {
		if v.BlockID == oldBlockID {
			continue
		} else {
			newBlockInfos = append(newBlockInfos, v)
		}
	}
	newBlockInfos = append(newBlockInfos, newBlock)

	blockGroup.BlockInfos = newBlockInfos

	logger.Debug("MigrateBlockGroup After , blockgroup %v", blockGroup)

	ns.BlockGroupDBSet(blockGroupID, blockGroup)

	return 0
}

//AllocateInodeID ...
func (ns *nameSpace) AllocateInodeID() (uint64, error) {
	return ns.RaftGroup.InodeIDGET(ns.RaftGroupID)
}

//AllocateChunkID ...
func (ns *nameSpace) AllocateChunkID() (uint64, error) {
	return ns.RaftGroup.ChunkIDGET(ns.RaftGroupID)
}

//InodeDBGet ...
func (ns *nameSpace) InodeDBGet(inode uint64) (bool, *mp.InodeInfo) {
	inodestr := strconv.FormatUint(inode, 10)

	value, err := ns.RaftGroup.InodeGet(ns.RaftGroupID, inodestr)
	if err != nil {
		value, err = ns.RaftGroup.InodeGet(ns.RaftGroupID, inodestr)
		if err != nil {
			//logger.Error("InodeDBGet vol:%v,key:%v,err:%v\n", ns.VolID, inodestr, err)
			return false, nil
		}
	}

	inodeInfo := mp.InodeInfo{}
	err = pbproto.Unmarshal(value, &inodeInfo)
	if err != nil {
		return false, nil
	}

	return true, &inodeInfo
}

//InodeDBSet ...
func (ns *nameSpace) InodeDBSet(inode uint64, v *mp.InodeInfo) error {

	inodestr := strconv.FormatUint(inode, 10)

	val, _ := pbproto.Marshal(v)
	err := ns.RaftGroup.InodeSet(ns.RaftGroupID, inodestr, val)
	if err != nil {
		err := ns.RaftGroup.InodeSet(ns.RaftGroupID, inodestr, val)
		if err != nil {
			logger.Error("InodeSet vol:%v,key:%v,err:%v\n", ns.VolID, inodestr, err)
			return err
		}
	}

	return nil

}

//InodeDBDelete ...
func (ns *nameSpace) InodeDBDelete(inode uint64) error {

	inodestr := strconv.FormatUint(inode, 10)

	err := ns.RaftGroup.InodeDel(ns.RaftGroupID, inodestr)
	if err != nil {
		err := ns.RaftGroup.InodeDel(ns.RaftGroupID, inodestr)
		if err != nil {
			logger.Error("InodeDBDelete vol:%v,key:%v,err:%v\n", ns.VolID, inodestr, err)
			return err
		}
	}
	return nil
}

//DentryDBGet ...
func (ns *nameSpace) DentryDBGet(dentryKey string) (bool, *mp.Dirent) {
	value, err := ns.RaftGroup.DentryGet(ns.RaftGroupID, dentryKey)
	if err != nil {
		value, err = ns.RaftGroup.DentryGet(ns.RaftGroupID, dentryKey)
		if err != nil {
			//logger.Error("DentryDBGet vol:%v,key:%v,err:%v\n", ns.VolID, dentryKey, err)
			return false, nil
		}
	}

	dirent := mp.Dirent{}
	err = pbproto.Unmarshal(value, &dirent)
	if err != nil {
		return false, nil
	}

	return true, &dirent
}

//BlockGroupDBGet ...
func (ns *nameSpace) DentryDBGetAll() (*map[string][]byte, error) {
	return ns.RaftGroup.DentryGetAll(ns.RaftGroupID)
}

//DentryDBSet ...
func (ns *nameSpace) DentryDBSet(dentryKey string, inodeType bool, inode uint64) error {

	dirent := &mp.Dirent{InodeType: inodeType, Inode: inode}

	val, _ := pbproto.Marshal(dirent)

	err := ns.RaftGroup.DentrySet(ns.RaftGroupID, dentryKey, val)
	if err != nil {
		err := ns.RaftGroup.DentrySet(ns.RaftGroupID, dentryKey, val)
		if err != nil {
			logger.Error("DentryDBSet vol:%v,key:%v,err:%v\n", ns.VolID, dentryKey, err)
			return err
		}
	}

	return nil

}

//DentryDBDelete ...
func (ns *nameSpace) DentryDBDelete(dentryKey string) error {

	err := ns.RaftGroup.DentryDel(ns.RaftGroupID, dentryKey)
	if err != nil {
		err := ns.RaftGroup.DentryDel(ns.RaftGroupID, dentryKey)
		if err != nil {
			logger.Error("DentryDBDelete vol:%v,key:%v,err:%v\n", ns.VolID, dentryKey, err)
			return err
		}
	}

	return nil
}

//BlockGroupDBGet ...
func (ns *nameSpace) BlockGroupDBGet(k uint32) (bool, *mp.BlockGroup) {
	value, err := ns.RaftGroup.BGGet(ns.RaftGroupID, strconv.Itoa(int(k)))
	if err != nil {
		value, err = ns.RaftGroup.BGGet(ns.RaftGroupID, strconv.Itoa(int(k)))
		if err != nil {
			logger.Error("BlockGroupDBGet vol:%v,key:%v,err:%v\n", ns.VolID, strconv.Itoa(int(k)), err)
			return false, nil
		}
	}

	blockGroup := mp.BlockGroup{}
	err = pbproto.Unmarshal(value, &blockGroup)
	if err != nil {
		return false, nil
	}
	return true, &blockGroup

}

//BlockGroupDBSet ...
func (ns *nameSpace) BlockGroupDBSet(k uint32, v *mp.BlockGroup) error {
	val, _ := pbproto.Marshal(v)
	err := ns.RaftGroup.BGSet(ns.RaftGroupID, strconv.Itoa(int(k)), val)
	if err != nil {
		err := ns.RaftGroup.BGSet(ns.RaftGroupID, strconv.Itoa(int(k)), val)
		if err != nil {
			logger.Error("BlockGroupDBSet vol:%v,key:%v,err=%v\n", ns.VolID, strconv.Itoa(int(k)), err)
			return err
		}
	}
	return nil
}

//BlockGroupDBGet ...
func (ns *nameSpace) BlockGroupDBGetAll() (*map[string][]byte, error) {
	return ns.RaftGroup.BGGetAll(ns.RaftGroupID)
}
