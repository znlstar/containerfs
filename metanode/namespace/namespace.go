package namespace

import (
	"bytes"
	//"encoding/json"
	"fmt"
	pbproto "github.com/golang/protobuf/proto"
	"github.com/ipdcode/containerfs/logger"
	"github.com/ipdcode/containerfs/metanode/raftopt"
	kvp "github.com/ipdcode/containerfs/proto/kvp"
	mp "github.com/ipdcode/containerfs/proto/mp"
	vp "github.com/ipdcode/containerfs/proto/vp"
	"github.com/ipdcode/containerfs/utils"
	"github.com/ipdcode/raft"
	"github.com/ipdcode/raft/proto"
	"github.com/ipdcode/raft/storage/wal"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"math/rand"
	"path"
	"runtime/debug"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	//Blksize G
	Blksize = 10
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

	logger.Error("initNameSpace IsLeader %v", rs.IsLeader(nameSpace.RaftGroupID))
	if !rs.IsLeader(nameSpace.RaftGroupID) {
		return 0
	}

	ret, tmpBlockGroups := nameSpace.GetVolInfo(UUID)
	if ret != 0 {
		return ret
	}

	if len(tmpBlockGroups) <= 0 {
		return 0
	}

	var blockgroupIDs []int32
	for _, v := range tmpBlockGroups {
		v.FreeCnt = 160
		logger.Error("initNameSpace BlockGroupDBSet %v,%v", v.BlockGroupID, v)

		err := nameSpace.BlockGroupDBSet(v.BlockGroupID, v)
		if err != nil {
			continue
		}
		blockgroupIDs = append(blockgroupIDs, v.BlockGroupID)
	}

	bks := kvp.Slice{}
	bks.IDS = blockgroupIDs

	err := nameSpace.VolumeDBSet(&bks)
	if err != nil {
		return 1
	}

	tmpInodeInfo := mp.InodeInfo{
		InodeID: 0, Name: "/",
		AccessTime: time.Now().Unix(),
		ModifiTime: time.Now().Unix(),
		InodeType:  false}

	err = nameSpace.InodeDBSet("0", &tmpInodeInfo)
	if err != nil {
		return 1
	}

	err = nameSpace.InitInodeID()
	if err != nil {
		return 1
	}

	err = nameSpace.InitChunkID()
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

	logger.Error("createRaftGroup, success")

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
	defer gMutex.RUnlock()
	if v, ok := AllNameSpace[UUID]; ok {
		return 0, v
	}
	return -1, nil
}

//GetFSInfo ...
func (ns *nameSpace) GetFSInfo(volID string) mp.GetFSInfoAck {

	defer catchPanic()

	ack := mp.GetFSInfoAck{}
	var totalSpace uint64
	var freeSpace uint64

	ret, blkgrps := ns.VolumeDBGet()
	if !ret {
		return ack
	}
	for _, v := range blkgrps.IDS {
		_, bg := ns.BlockGroupDBGet(v)
		totalSpace = totalSpace + (Blksize * 1073741824)
		freeSpace = freeSpace + 64*1024*1024*uint64(bg.FreeCnt)
	}

	ack.TotalSpace = totalSpace
	ack.FreeSpace = freeSpace
	ack.Ret = 0

	return ack
}

// GetVolInfo ...
func (ns *nameSpace) GetVolInfo(name string) (int32, []*vp.BlockGroup) {

	defer catchPanic()

	conn, err := grpc.Dial(VolMgrAddress, grpc.WithInsecure())
	if err != nil {
		logger.Debug("Dial failed: %v", err)
		return -1, nil
	}
	defer conn.Close()
	vc := vp.NewVolMgrClient(conn)
	pGetVolInfoReq := &vp.GetVolInfoReq{UUID: name}
	pGetVolInfoAck, _ := vc.GetVolInfo(context.Background(), pGetVolInfoReq)
	if pGetVolInfoAck.Ret != 0 {
		logger.Debug("GetVolInfo failed: %v", pGetVolInfoAck.Ret)
		return pGetVolInfoAck.Ret, nil
	}
	return 0, pGetVolInfoAck.VolInfo.BlockGroups
}

//GetVolList ...
func GetVolList() (int32, []*vp.VolIDs) {

	defer catchPanic()

	conn, err := grpc.Dial(VolMgrAddress, grpc.WithInsecure())
	if err != nil {
		logger.Debug("Dial failed: %v", err)
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
		logger.Debug("GetVolList failed: %v", pGetVolListAck.Ret)
		return pGetVolListAck.Ret, nil
	}
	return 0, pGetVolListAck.VolIDs
}

//CreateDir ...
func (ns *nameSpace) CreateDir(path string) int32 {

	defer catchPanic()

	var ret int32
	ret = 0
	keys := ns.GetAllKeyByFullPath(path)
	keysNum := len(keys)
	var pParentInodeInfo *mp.InodeInfo
	var ok bool
	if ok, pParentInodeInfo = ns.InodeDBGet(keys[keysNum-2]); !ok {
		ret = 2 /*ENOENT*/
		return ret
	}
	if pParentInodeInfo.InodeType == true {
		ret = 1 /*EPERM*/
		return ret
	}
	if ok, _ = ns.InodeDBGet(keys[keysNum-1]); ok {
		ret = 17 /*ENOENT*/
		return ret
	}
	/*update inode info*/
	inodeID, err := ns.AllocateInodeID()
	if err != nil {
		return 2
	}
	name := utils.GetSelfName(path)
	tmpInodeInfo := mp.InodeInfo{
		ParentInodeID: pParentInodeInfo.InodeID,
		InodeID:       inodeID,
		Name:          name,
		AccessTime:    time.Now().Unix(),
		ModifiTime:    time.Now().Unix(),
		InodeType:     false}

	tmpKey := strconv.FormatInt(pParentInodeInfo.InodeID, 10) + "-" + name
	err = ns.InodeDBSet(tmpKey, &tmpInodeInfo)
	if err != nil {
		return 1
	}
	/*update patent inode info*/
	pParentInodeInfo.ChildrenInodeIDs = append(pParentInodeInfo.ChildrenInodeIDs, inodeID)
	parentName := utils.GetParentName(path)
	if parentName == "/" {
		err = ns.InodeDBSet("0", pParentInodeInfo)
		if err != nil {
			return 1
		}
	} else {
		tmpKey = strconv.FormatInt(pParentInodeInfo.ParentInodeID, 10) + "-" + utils.GetParentName(path)
		err = ns.InodeDBSet(tmpKey, pParentInodeInfo)
		if err != nil {
			return 1
		}
	}
	return ret
}

//Stat ...
func (ns *nameSpace) Stat(path string) (*mp.InodeInfo, int32) {

	defer catchPanic()

	var ret int32
	ret = 0
	var ok bool
	var pInodeInfo *mp.InodeInfo

	if path == "/" {
		if ok, pInodeInfo = ns.InodeDBGet("0"); !ok {
			ret = 1
			return nil, ret
		}
		return pInodeInfo, 0
	}
	keys := ns.GetAllKeyByFullPath(path)
	keysNum := len(keys)

	if ok, pInodeInfo = ns.InodeDBGet(keys[keysNum-1]); !ok {
		ret = 2 /*ENOENT*/
		return nil, ret
	}
	return pInodeInfo, 0
}

//List ...
func (ns *nameSpace) List(path string) ([]*mp.InodeInfo, int32) {

	defer catchPanic()

	var ret int32
	ret = 0

	var pInodeInfo *mp.InodeInfo
	var ok bool
	if path == "/" {
		ok, pInodeInfo = ns.InodeDBGet("0")
		if !ok {
			ret = 1 /*EIO*/
			return nil, ret
		}
	} else {
		keys := ns.GetAllKeyByFullPath(path)
		keysNum := len(keys)

		if ok, pInodeInfo = ns.InodeDBGet(keys[keysNum-1]); !ok {
			ret = 2 /*ENOENT*/
			return nil, ret
		}
	}
	var tmpInodeInfos []*mp.InodeInfo
	if pInodeInfo.InodeType == true {
		tmpInodeInfos = append(tmpInodeInfos, pInodeInfo)
		return tmpInodeInfos, ret
	}

	allMap, _ := raftopt.KvGetAll(ns.RaftGroup, ns.RaftGroupID)
	children := pInodeInfo.ChildrenInodeIDs
	if len(children) == 0 {
		return tmpInodeInfos, ret
	}

	ns.RaftGroup.RLock()
	for k, v := range allMap {
		if strings.Contains(k, "InodeDB/") {
			inodeInfo := mp.InodeInfo{}
			err := pbproto.Unmarshal(v, &inodeInfo)
			if err != nil {
				continue
			}
			index := -1
			for i, child := range children {
				if child == inodeInfo.InodeID {
					tmpInodeInfos = append(tmpInodeInfos, &inodeInfo)
					index = i
					break
				}
			}
			if index >= 0 {
				children = append(children[:index], children[index+1:]...)
				if len(children) == 0 {
					break
				}
			}

		}
	}
	ns.RaftGroup.RUnlock()

	/*
		var pTmpInodeInfo *mp.InodeInfo
		for _, value := range pInodeInfo.ChildrenInodeIDs {
			ok, pTmpInodeInfo = ns.InodeDBGet(strconv.FormatInt(value, 10))
			if !ok {
				continue
			}
			tmpInodeInfos = append(tmpInodeInfos, pTmpInodeInfo)
		}
	*/

	return tmpInodeInfos, ret
}

//DeleteDir ...
func (ns *nameSpace) DeleteDir(path string) int32 {

	defer catchPanic()

	var ret int32
	ret = 0

	if path == "/" {
		ret = 1
		return ret
	}

	keys := ns.GetAllKeyByFullPath(path)
	keysNum := len(keys)

	var ok bool
	var pInodeInfo *mp.InodeInfo

	if ok, pInodeInfo = ns.InodeDBGet(keys[keysNum-1]); !ok {
		ret = 2 /*ENOENT*/
		return ret
	}
	if pInodeInfo.InodeType == true {
		ret = 1 /*EPERM*/
		return ret
	}
	if tmplen := len(pInodeInfo.ChildrenInodeIDs); tmplen > 0 {
		ret = 1 /*EPERM*/
		return ret
	}

	/*update patent inode info*/
	var pTmpParentInodeInfo *mp.InodeInfo
	ok, pTmpParentInodeInfo = ns.InodeDBGet(keys[keysNum-2])
	if !ok {
		return 1
	}
	for index, value := range pTmpParentInodeInfo.ChildrenInodeIDs {
		if value == pInodeInfo.InodeID {
			pTmpParentInodeInfo.ChildrenInodeIDs = append(pTmpParentInodeInfo.ChildrenInodeIDs[:index], pTmpParentInodeInfo.ChildrenInodeIDs[index+1:]...)
			break
		}
	}
	err := ns.InodeDBSet(keys[keysNum-2], pTmpParentInodeInfo)
	if err != nil {
		return 1
	}

	/*delete inode info*/
	err = ns.InodeDBDelete(strconv.FormatInt(pInodeInfo.ParentInodeID, 10) + "-" + utils.GetSelfName(path))
	if err != nil {
		return 1
	}
	return ret
}

//Rename ...
func (ns *nameSpace) Rename(path1 string, path2 string) int32 {

	defer catchPanic()

	var ret int32
	ret = 0

	if path1 == "/" || path2 == "/" {
		ret = 1
		return ret
	}

	key1s := ns.GetAllKeyByFullPath(path1)
	key1sNum := len(key1s)
	var pInodeInfo *mp.InodeInfo
	var ok bool
	if ok, pInodeInfo = ns.InodeDBGet(key1s[key1sNum-1]); !ok {
		ret = 17 /*ENOENT*/
		return ret
	}

	key2s := ns.GetAllKeyByFullPath(path2)
	key2sNum := len(key2s)

	var pParentInodeInfo2 *mp.InodeInfo

	if ok, pParentInodeInfo2 = ns.InodeDBGet(key2s[key2sNum-2]); !ok {
		ret = 2 /*ENOENT*/
		return ret
	}
	if pParentInodeInfo2.InodeType == true {
		ret = 1 /*EPERM*/
		return ret
	}
	if ok, _ = ns.InodeDBGet(key2s[key2sNum-1]); ok {
		ret = 17 /*ENOENT*/
		return ret
	}

	if key2s[key2sNum-2] != key1s[key1sNum-2] {
		return 1 /*EPERM*/
	}

	//delete old parentID + name key
	err := ns.InodeDBDelete(strconv.FormatInt(pInodeInfo.ParentInodeID, 10) + "-" + utils.GetSelfName(path1))
	if err != nil {
		return 1
	}
	//add a new parentID + name key
	name := utils.GetSelfName(path2)
	tmpInodeInfo := mp.InodeInfo{
		ParentInodeID:    pParentInodeInfo2.InodeID,
		InodeID:          pInodeInfo.InodeID,
		Name:             name,
		AccessTime:       pInodeInfo.AccessTime,
		ModifiTime:       pInodeInfo.ModifiTime,
		InodeType:        pInodeInfo.InodeType,
		FileSize:         pInodeInfo.FileSize,
		Chunks:           pInodeInfo.Chunks,
		ChildrenInodeIDs: pInodeInfo.ChildrenInodeIDs}

	tmpKey := strconv.FormatInt(pParentInodeInfo2.InodeID, 10) + "-" + name
	err = ns.InodeDBSet(tmpKey, &tmpInodeInfo)
	if err != nil {
		return 1
	}
	return ret
}

//CreateFile ...
func (ns *nameSpace) CreateFile(path string) int32 {

	defer catchPanic()

	if path == "/" {
		return 1
	}

	var ret int32
	ret = 0

	keys := ns.GetAllKeyByFullPath(path)
	keysNum := len(keys)

	var ok bool
	var pParentInodeInfo *mp.InodeInfo
	if ok, pParentInodeInfo = ns.InodeDBGet(keys[keysNum-2]); !ok {
		ret = 2 /*ENOENT*/
		return ret
	}

	if pParentInodeInfo.InodeType == true {
		ret = 1 /*EPERM*/
		return ret
	}

	if ok, _ = ns.InodeDBGet(keys[keysNum-1]); ok {
		ret = 17 /*ENOENT*/
		return ret
	}

	/*update inode info*/
	inodeID, err := ns.AllocateInodeID()
	if err != nil {
		return 1
	}
	name := utils.GetSelfName(path)
	tmpInodeInfo := mp.InodeInfo{
		ParentInodeID: pParentInodeInfo.InodeID,
		InodeID:       inodeID,
		Name:          name,
		AccessTime:    time.Now().Unix(),
		ModifiTime:    time.Now().Unix(),
		InodeType:     true}

	tmpKey := strconv.FormatInt(pParentInodeInfo.InodeID, 10) + "-" + name
	err = ns.InodeDBSet(tmpKey, &tmpInodeInfo)
	if err != nil {
		return 1
	}

	/*update patent inode info*/
	pParentInodeInfo.ChildrenInodeIDs = append(pParentInodeInfo.ChildrenInodeIDs, inodeID)

	parentName := utils.GetParentName(path)
	if parentName == "/" {
		ns.InodeDBSet("0", pParentInodeInfo)

	} else {

		tmpKey = strconv.FormatInt(pParentInodeInfo.ParentInodeID, 10) + "-" + utils.GetParentName(path)
		err = ns.InodeDBSet(tmpKey, pParentInodeInfo)
		if err != nil {
			return 1
		}
	}

	return ret
}

//DeleteFile ...
func (ns *nameSpace) DeleteFile(path string) int32 {

	defer catchPanic()

	var ret int32
	ret = 0
	if path == "/" {
		ret = 1
		return ret
	}
	keys := ns.GetAllKeyByFullPath(path)
	keysNum := len(keys)

	var ok bool
	var pInodeInfo *mp.InodeInfo
	if ok, pInodeInfo = ns.InodeDBGet(keys[keysNum-1]); !ok {
		ret = 0 /*ENOENT*/
		return ret
	}
	if pInodeInfo.InodeType == false {
		ret = 1 /*EPERM*/
		return ret
	}
	/*update patent inode info*/
	var pTmpParentInodeInfo *mp.InodeInfo
	ok, pTmpParentInodeInfo = ns.InodeDBGet(keys[keysNum-2])
	if !ok {
		return 1
	}
	for index, value := range pTmpParentInodeInfo.ChildrenInodeIDs {
		if value == pInodeInfo.InodeID {
			pTmpParentInodeInfo.ChildrenInodeIDs = append(pTmpParentInodeInfo.ChildrenInodeIDs[:index], pTmpParentInodeInfo.ChildrenInodeIDs[index+1:]...)
			break
		}
	}
	err := ns.InodeDBSet(keys[keysNum-2], pTmpParentInodeInfo)
	if err != nil {
		return 1
	}

	/*release bg cnt*/
	for _, v := range pInodeInfo.Chunks {
		ns.ReleaseBlockGroup(v.BlockGroupID)
	}

	err = ns.InodeDBDelete(strconv.FormatInt(pInodeInfo.ParentInodeID, 10) + "-" + utils.GetSelfName(path))
	if err != nil {
		return 1
	}
	return ret
}

//AllocateChunk ...
func (ns *nameSpace) AllocateChunk(path string) (int32, *mp.ChunkInfo) {

	defer catchPanic()

	var ret int32

	fmt.Println("AllocateChunk...")
	keys := ns.GetAllKeyByFullPath(path)
	keysNum := len(keys)

	var inodeInfo *mp.InodeInfo
	var ok bool
	if ok, inodeInfo = ns.InodeDBGet(keys[keysNum-1]); !ok {
		ret = 2 /*ENOENT*/
		return ret, nil
	}

	var chunkInfo = mp.ChunkInfo{}
	ret, _, blockGroup := ns.ChooseBlockGroup()

	if ret != 0 {
		return 28 /*ENOSPC*/, nil
	}
	chunkInfo.BlockGroupID = blockGroup.BlockGroupID
	chunkInfo.ChunkSize = 0

	var err error
	chunkInfo.ChunkID, err = ns.AllocateChunkID()
	if err != nil {
		return 1, nil
	}

	inodeInfo.Chunks = append(inodeInfo.Chunks, &chunkInfo)
	ns.InodeDBSet(keys[keysNum-1], inodeInfo)

	return 0, &chunkInfo

}

//GetFileChunks ...
func (ns *nameSpace) GetFileChunks(path string) (int32, []*mp.ChunkInfo) {

	defer catchPanic()

	var ret int32
	var ok bool
	keys := ns.GetAllKeyByFullPath(path)
	keysNum := len(keys)

	var pTmpInodeInfo *mp.InodeInfo
	if ok, pTmpInodeInfo = ns.InodeDBGet(keys[keysNum-1]); !ok {
		ret = 2 /*ENOENT*/
		return ret, nil
	}
	return 0, pTmpInodeInfo.Chunks

}

//SyncChunk ...
func (ns *nameSpace) SyncChunk(path string, chunkinfo *mp.ChunkInfo) int32 {

	defer catchPanic()

	var ret int32
	var ok bool
	keys := ns.GetAllKeyByFullPath(path)
	keysNum := len(keys)

	var pTmpInodeInfo *mp.InodeInfo
	if ok, pTmpInodeInfo = ns.InodeDBGet(keys[keysNum-1]); !ok {
		ret = 2 /*ENOENT*/
		return ret
	}

	pTmpInodeInfo.ModifiTime = time.Now().Unix()

	var lastChunkID int64
	if len(pTmpInodeInfo.Chunks) > 0 {
		//for appned write
		lastChunkID = pTmpInodeInfo.Chunks[len(pTmpInodeInfo.Chunks)-1].ChunkID
		if lastChunkID == chunkinfo.ChunkID {
			pTmpInodeInfo.FileSize = pTmpInodeInfo.FileSize + int64(chunkinfo.ChunkSize) - int64(pTmpInodeInfo.Chunks[len(pTmpInodeInfo.Chunks)-1].ChunkSize)
			pTmpInodeInfo.Chunks[len(pTmpInodeInfo.Chunks)-1] = chunkinfo
		} else {
			pTmpInodeInfo.Chunks = append(pTmpInodeInfo.Chunks, chunkinfo)
			pTmpInodeInfo.FileSize += int64(chunkinfo.ChunkSize)
		}
	} else {
		pTmpInodeInfo.Chunks = append(pTmpInodeInfo.Chunks, chunkinfo)
		pTmpInodeInfo.FileSize += int64(chunkinfo.ChunkSize)
	}
	err := ns.InodeDBSet(keys[keysNum-1], pTmpInodeInfo)
	if err != nil {
		return 1
	}

	return 0

}

//BlockGroupVp2Mp ...
func (ns *nameSpace) BlockGroupVp2Mp(in *vp.BlockGroup) *mp.BlockGroup {

	defer catchPanic()

	var mpBlockGroup = mp.BlockGroup{}

	mpBlockInfos := make([]*mp.BlockInfo, len(in.BlockInfos))

	mpBlockGroup.BlockGroupID = in.BlockGroupID
	mpBlockGroup.FreeCnt = in.FreeCnt
	mpBlockGroup.Status = in.Status

	for i := range in.BlockInfos {
		var pVpBlockInfo *vp.BlockInfo
		var mpBlockInfo mp.BlockInfo

		pVpBlockInfo = in.BlockInfos[i]
		mpBlockInfo.BlockID = pVpBlockInfo.BlockID
		mpBlockInfo.DataNodeIP = pVpBlockInfo.DataNodeIP
		mpBlockInfo.DataNodePort = pVpBlockInfo.DataNodePort
		mpBlockInfo.Status = pVpBlockInfo.Status

		mpBlockInfos[i] = &mpBlockInfo

	}

	mpBlockGroup.BlockInfos = mpBlockInfos
	return &mpBlockGroup

}

/*
//ChooseBlockGroup
func (ns *nameSpace) ChooseBlockGroup() (int32, int32, *vp.BlockGroup) {

	ns.RLock()
	defer ns.RUnlock()

	var blockGroupID int32
	var blockGroup *vp.BlockGroup
	flag := false

	ret, blkgrps := ns.VolumeDBGet()
	if !ret {
		return 1, -1, nil
	}
	for _, v := range blkgrps {
		ret, bg := ns.BlockGroupDBGet(v)
		if !ret {
			continue
		}
		logger.Debug("bg1:%v\n", bg)

		if bg.Status == 1 {
			blockGroupID = bg.BlockGroupID
			bg.FreeCnt = bg.FreeCnt - 1
			if bg.FreeCnt <= 0 {
				bg.Status = 2
				//ns.SetBlockGroupStatus(blockGroupID, bg.Status)
			}
			blockGroup = bg
			logger.Debug("find a using blockgroup,blgid:%v\n", bg.BlockGroupID)
			flag = true
			break
		}
	}

	//find the free blockgroup
	if flag == false {
		ret, blkgrps := ns.VolumeDBGet()
		if !ret {
			return 1, -1, nil
		}
		for _, v := range blkgrps {
			ret, bg := ns.BlockGroupDBGet(v)
			if !ret {
				continue
			}
			logger.Debug("bg2:%v\n", bg)
			if bg.Status == 0 {
				blockGroupID = bg.BlockGroupID
				bg.FreeCnt = bg.FreeCnt - 1
				if bg.FreeCnt == 0 {
					bg.Status = 2
					//ns.SetBlockGroupStatus(blockGroupID, bg.Status)
				} else {
					bg.Status = 1
				}
				blockGroup = bg
				logger.Debug("find a free blockgroup,blgid:%v\n", bg.BlockGroupID)
				flag = true
				break
			}
		}
	}

	if flag {
		ns.BlockGroupDBSet(blockGroupID, blockGroup)
		return 0, blockGroupID, blockGroup
	} else {
		return 1, -1, nil
	}

}
*/

//ChooseBlockGroup ...
func (ns *nameSpace) ChooseBlockGroup() (int32, int32, *vp.BlockGroup) {

	defer catchPanic()

	ns.RLock()
	defer ns.RUnlock()

	var blockGroupID int32
	var blockGroup *vp.BlockGroup
	flag := false

	ret, tmp := ns.VolumeDBGet()

	blkgrps := tmp.IDS
	if !ret {
		return 1, -1, nil
	}

	for true {
		r := rand.New(rand.NewSource(time.Now().UnixNano()))
		id := r.Intn(len(blkgrps))
		bgid := blkgrps[id]

		ret, bg := ns.BlockGroupDBGet(bgid)
		if !ret {
			continue
		}
		logger.Debug("bg1:%v\n", bg)

		if bg.Status == 2 {
			blkgrps = append(blkgrps[:id], blkgrps[id+1:]...)
			if len(blkgrps) == 0 {
				return 1, -1, nil
			}
			continue
		}

		blockGroupID = bg.BlockGroupID
		bg.FreeCnt = bg.FreeCnt - 1
		if bg.FreeCnt <= 0 {
			bg.Status = 2
		} else {
			bg.Status = 1
		}
		blockGroup = bg
		logger.Debug("find a blockgroup,blgid:%v\n", bg.BlockGroupID)
		flag = true
		break
	}

	if flag {
		ns.BlockGroupDBSet(blockGroupID, blockGroup)
		return 0, blockGroupID, blockGroup
	}
	return 1, -1, nil
}

//ReleaseBlockGroup ...
func (ns *nameSpace) ReleaseBlockGroup(blockGroupID int32) {

	defer catchPanic()

	ok, blockGroup := ns.BlockGroupDBGet(blockGroupID)
	if !ok {
		return
	}

	var status int32
	blockGroup.FreeCnt++
	if blockGroup.FreeCnt > 160 {
		blockGroup.FreeCnt = 160
	}
	if blockGroup.FreeCnt > 0 {
		status = 1
		if blockGroup.Status != status && blockGroup.Status != 3 {
			blockGroup.Status = 1
			//ns.SetBlockGroupStatus(blockGroupID, blockGroup.Status)
		}

	}
	if blockGroup.FreeCnt == 160 {
		status = 0
		if blockGroup.Status != status && blockGroup.Status != 3 {
			blockGroup.Status = 0
			//ns.SetBlockGroupStatus(blockGroupID, blockGroup.Status)
		}

	}

	ns.BlockGroupDBSet(blockGroupID, blockGroup)

}

//UpdateChunkInfo ...
func (ns *nameSpace) UpdateChunkInfo(in *mp.UpdateChunkInfoReq) int32 {

	defer catchPanic()

	return 0
}

//GetAllKeyByFullPath ...
func (ns *nameSpace) GetAllKeyByFullPath(in string) (keys []string) {

	defer catchPanic()

	tmp := strings.Split(in, "/")
	keys = make([]string, 1)
	for i, v := range tmp {
		if i == 0 {
			keys[i] = "0"
		} else {
			if ok, pInodeInfo := ns.InodeDBGet(keys[i-1]); ok {
				buffer := new(bytes.Buffer)
				buffer.WriteString(strconv.FormatInt(pInodeInfo.InodeID, 10))
				buffer.WriteString("-")
				buffer.WriteString(v)
				//newkey := strconv.FormatInt(pInodeInfo.InodeID, 10) + "-" + v
				newkey := buffer.String()
				keys = append(keys, newkey)
			} else {
				keys = append(keys, " ")
			}
		}
	}

	return
}

//InitInodeID ...
func (ns *nameSpace) InitInodeID() error {

	err := raftopt.KvSet(ns.RaftGroup, ns.RaftGroupID, "InodeID/", []byte(strconv.Itoa(0)))
	if err != nil {
		err := raftopt.KvSet(ns.RaftGroup, ns.RaftGroupID, "InodeID/", []byte(strconv.Itoa(0)))
		if err != nil {
			//logger.Error("AllocateInodeID put vol:%v,key:%v,err:%v\n", ns.VolID, "InodeID/", err)
			return err
		}
	}

	return nil
}

//InitChunkID ...
func (ns *nameSpace) InitChunkID() error {

	err := raftopt.KvSet(ns.RaftGroup, ns.RaftGroupID, "ChunkID/", []byte(strconv.Itoa(0)))
	if err != nil {
		err := raftopt.KvSet(ns.RaftGroup, ns.RaftGroupID, "ChunkID/", []byte(strconv.Itoa(0)))
		if err != nil {
			//logger.Error("AllocateChunkID put vol:%v,key:%v,err:%v\n", ns.VolID, "ChunkID/", err)
			return err
		}
	}

	return nil
}

//VolumeDBSet ...
func (ns *nameSpace) VolumeDBSet(v *kvp.Slice) error {
	val, _ := pbproto.Marshal(v)
	err := raftopt.KvSet(ns.RaftGroup, ns.RaftGroupID, "VolumeDB/", val)
	if err != nil {
		err := raftopt.KvSet(ns.RaftGroup, ns.RaftGroupID, "VolumeDB/", val)
		if err != nil {
			//logger.Error("VolumeDBSet vol:%v,key:%v,v:%v,err:%v\n", ns.VolID, "VolumeDB/", val, err)
			return err
		}
	}
	return nil
}

//VolumeDBGet  ...
func (ns *nameSpace) VolumeDBGet() (bool, *kvp.Slice) {
	value, err := raftopt.KvGet(ns.RaftGroup, ns.RaftGroupID, "VolumeDB/")
	if err != nil {
		value, err = raftopt.KvGet(ns.RaftGroup, ns.RaftGroupID, "VolumeDB/")
		if err != nil {
			//logger.Error("BlockGroupDBGet vol:%v,key:%v,err:%v\n", ns.VolID, "VolumeDB/", err)
			return false, nil
		}
	}

	blkgrps := kvp.Slice{}
	err = pbproto.Unmarshal(value, &blkgrps)
	if err != nil {
		return false, nil
	}
	return true, &blkgrps

}

//AllocateInodeID ...
func (ns *nameSpace) AllocateInodeID() (int64, error) {
	ns.Lock()
	defer ns.Unlock()

	value, err := raftopt.KvGet(ns.RaftGroup, ns.RaftGroupID, "InodeID/")
	if err != nil {
		value, err = raftopt.KvGet(ns.RaftGroup, ns.RaftGroupID, "InodeID/")
		if err != nil {
			//logger.Error("AllocateInodeID get vol:%v,key:%v,err:%v\n", ns.VolID, "InodeID/", err)
			return 0, err
		}
	}

	id, _ := strconv.ParseInt(string(value), 10, 64)
	id = id + 1

	err = raftopt.KvSet(ns.RaftGroup, ns.RaftGroupID, "InodeID/", []byte(strconv.FormatInt(id, 10)))
	if err != nil {
		err := raftopt.KvSet(ns.RaftGroup, ns.RaftGroupID, "InodeID/", []byte(strconv.FormatInt(id, 10)))
		if err != nil {
			//logger.Error("AllocateInodeID put vol:%v,key:%v,v:%v,err:%v\n", ns.VolID, "InodeID/", strconv.FormatInt(id, 10), err)
			return 0, err
		}
	}

	return id, nil
}

//AllocateChunkID ...
func (ns *nameSpace) AllocateChunkID() (int64, error) {
	ns.Lock()
	defer ns.Unlock()

	value, err := raftopt.KvGet(ns.RaftGroup, ns.RaftGroupID, "ChunkID/")
	if err != nil {
		value, err = raftopt.KvGet(ns.RaftGroup, ns.RaftGroupID, "ChunkID/")
		if err != nil {
			//logger.Error("AllocateChunkID get vol:%v,key:%v,err:%v\n", ns.VolID, "ChunkID/", err)
			return -1, err
		}
	}

	id, _ := strconv.ParseInt(string(value), 10, 64)
	id = id + 1

	err = raftopt.KvSet(ns.RaftGroup, ns.RaftGroupID, "ChunkID/", []byte(strconv.FormatInt(id, 10)))
	if err != nil {
		err := raftopt.KvSet(ns.RaftGroup, ns.RaftGroupID, "ChunkID/", []byte(strconv.FormatInt(id, 10)))
		if err != nil {
			//logger.Error("AllocateChunkID put vol:%v,key:%v,v:%v,err:%v\n", ns.VolID, "ChunkID/", strconv.FormatInt(id, 10), err)
			return -1, err
		}
	}

	return id, nil
}

//InodeDBGet ...
func (ns *nameSpace) InodeDBGet(k string) (bool, *mp.InodeInfo) {
	value, err := raftopt.KvGet(ns.RaftGroup, ns.RaftGroupID, "InodeDB/"+k)
	if err != nil {
		value, err = raftopt.KvGet(ns.RaftGroup, ns.RaftGroupID, "InodeDB/"+k)
		if err != nil {
			//logger.Error("InodeDBGet vol:%v,key:%v,err:%v\n", ns.VolID, "InodeDB/"+k, err)
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
func (ns *nameSpace) InodeDBSet(k string, v *mp.InodeInfo) error {
	val, _ := pbproto.Marshal(v)
	err := raftopt.KvSet(ns.RaftGroup, ns.RaftGroupID, "InodeDB/"+k, val)
	if err != nil {
		err := raftopt.KvSet(ns.RaftGroup, ns.RaftGroupID, "InodeDB/"+k, val)
		if err != nil {
			//logger.Error("InodeDBSet vol:%v,key:%v,err:%v\n", ns.VolID, "InodeDB/"+k, err)
			return err
		}
	}
	return nil

}

//InodeDBDelete ...
func (ns *nameSpace) InodeDBDelete(k string) error {
	err := raftopt.KvSet(ns.RaftGroup, ns.RaftGroupID, "InodeDB/"+k, []byte("!delete!"))
	if err != nil {
		err := raftopt.KvSet(ns.RaftGroup, ns.RaftGroupID, "InodeDB/"+k, []byte("!delete!"))
		if err != nil {
			//logger.Error("InodeDBSet vol:%v,key:%v,err:%v\n", ns.VolID, "InodeDB/"+k, err)
			return err
		}
	}
	return nil
}

//BlockGroupDBGet ...
func (ns *nameSpace) BlockGroupDBGet(k int32) (bool, *vp.BlockGroup) {
	value, err := raftopt.KvGet(ns.RaftGroup, ns.RaftGroupID, "BGDB/"+strconv.Itoa(int(k)))
	if err != nil {
		value, err = raftopt.KvGet(ns.RaftGroup, ns.RaftGroupID, "BGDB/"+strconv.Itoa(int(k)))
		if err != nil {
			//logger.Error("BlockGroupDBGet vol:%v,key:%v,err:%v\n", ns.VolID, "BGDB/"+strconv.Itoa(int(k)), err)
			return false, nil
		}
	}

	blockGroup := vp.BlockGroup{}
	err = pbproto.Unmarshal(value, &blockGroup)
	if err != nil {
		return false, nil
	}
	return true, &blockGroup

}

//BlockGroupDBSet ...
func (ns *nameSpace) BlockGroupDBSet(k int32, v *vp.BlockGroup) error {
	val, _ := pbproto.Marshal(v)
	err := raftopt.KvSet(ns.RaftGroup, ns.RaftGroupID, "BGDB/"+strconv.Itoa(int(k)), val)
	if err != nil {
		err := raftopt.KvSet(ns.RaftGroup, ns.RaftGroupID, "BGDB/"+strconv.Itoa(int(k)), val)
		if err != nil {
			//logger.Error("BlockGroupDBSet vol:%v,key:%v,err=%v\n", ns.VolID, "BGDB/"+strconv.Itoa(int(k)), err)
			return err
		}
	}
	return nil
}

/*
//BlockGroupDBDelete
func (ns *nameSpace) BlockGroupDBDelete(k int32) error {
	err := raftopt.KvSet(ns.RaftGroup, ns.RaftGroupID, "BGDB/"+strconv.Itoa(int(k)), "!delete!")
	if err != nil {
		err := raftopt.KvSet(ns.RaftGroup, ns.RaftGroupID, "BGDB/"+strconv.Itoa(int(k)), "!delete!")
		if err != nil {
			logger.Error("BlockGroupDBSet vol:%v,key:%v,err=%v\n", ns.VolID, "BGDB/"+strconv.Itoa(int(k)), err)
			return err
		}
	}
	return nil
}
*/
