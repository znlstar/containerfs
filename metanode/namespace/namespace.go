package namespace

import (
	"bytes"
	mp "github.com/ipdcode/containerfs/proto/mp"
	vp "github.com/ipdcode/containerfs/proto/vp"

	"github.com/ipdcode/containerfs/logger"
	"github.com/ipdcode/containerfs/utils"

	// "github.com/chasex/redis-go-cluster"
	"github.com/go-redis/redis"

	"encoding/json"
	"fmt"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	Blksize = 10 //Blksize G
)

// VolMgrAddress
var VolMgrAddress string

//var RedisClusters [10]*redis.Cluster
var RedisClient *redis.ClusterClient

type nameSpace struct {
	VolID string
}

var AllNameSpace map[string]*nameSpace
var gMutex sync.RWMutex

// CreateGNameSpace
func CreateGNameSpace() {
	gMutex.Lock()
	AllNameSpace = make(map[string]*nameSpace)
	gMutex.Unlock()
}

// CreateNameSpace
func CreateNameSpace(UUID string, IsLoad bool) int32 {
	nameSpace := nameSpace{}
	nameSpace.VolID = UUID

	gMutex.Lock()
	AllNameSpace[UUID] = &nameSpace
	gMutex.Unlock()

	if !IsLoad {
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
			err := nameSpace.BlockGroupDBSet(v.BlockGroupID, v)
			if err != nil {
				continue
			}
			blockgroupIDs = append(blockgroupIDs, v.BlockGroupID)
		}

		err := nameSpace.VolumeDBSet(UUID, blockgroupIDs)
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

		tmpChunkInfo := mp.ChunkInfo{}
		err = nameSpace.ChunkDBSet(0, &tmpChunkInfo)
		if err != nil {
			return 1
		}
	}

	return 0
}

// GetNameSpace
func GetNameSpace(UUID string) (int32, *nameSpace) {
	gMutex.RLock()
	defer gMutex.RUnlock()
	if v, ok := AllNameSpace[UUID]; ok {
		return 0, v
	} else {
		return -1, nil
	}
}

// GetFSInfo
func (ns *nameSpace) GetFSInfo(volID string) mp.GetFSInfoAck {
	ack := mp.GetFSInfoAck{}
	var totalSpace uint64 = 0
	var freeSpace uint64 = 0
	blkgrps, err := ns.VolumeDBGet(volID)
	if err != nil {
		return ack
	}

	for _, v := range blkgrps {
		ret, bg := ns.BlockGroupDBGet(v)
		if !ret {
			continue
		}
		totalSpace = totalSpace + (Blksize * 1073741824)
		freeSpace = freeSpace + 64*1024*1024*uint64(bg.FreeCnt)
	}

	ack.TotalSpace = totalSpace
	ack.FreeSpace = freeSpace
	ack.Ret = 0
	return ack
}

func (ns *nameSpace) GetVolInfo(name string) (int32, []*vp.BlockGroup) {
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

// GetVolList
func GetVolList() (int32, []string) {
	conn, err := grpc.Dial(VolMgrAddress, grpc.WithInsecure())
	if err != nil {
		logger.Debug("Dial failed: %v", err)
		return -1, nil
	}
	defer conn.Close()
	vc := vp.NewVolMgrClient(conn)
	pGetVolListReq := &vp.GetVolListReq{}
	pGetVolListAck, _ := vc.GetVolList(context.Background(), pGetVolListReq)
	if pGetVolListAck.Ret != 0 {
		logger.Debug("GetVolList failed: %v", pGetVolListAck.Ret)
		return pGetVolListAck.Ret, nil
	}
	return 0, pGetVolListAck.VolIDs
}

// CreateDir
func (ns *nameSpace) CreateDir(path string) int32 {
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

	tmpKey := strconv.FormatInt(inodeID, 10)
	err = ns.InodeDBSet(tmpKey, &tmpInodeInfo)
	if err != nil {
		return 1
	}

	tmpKey = strconv.FormatInt(pParentInodeInfo.InodeID, 10) + "-" + name
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
		tmpKey = strconv.FormatInt(pParentInodeInfo.InodeID, 10)
		err = ns.InodeDBSet(tmpKey, pParentInodeInfo)
		if err != nil {
			return 1
		}

		tmpKey = strconv.FormatInt(pParentInodeInfo.ParentInodeID, 10) + "-" + utils.GetParentName(path)
		err = ns.InodeDBSet(tmpKey, pParentInodeInfo)
		if err != nil {
			return 1
		}
	}
	return ret
}

// Stat
func (ns *nameSpace) Stat(path string) (*mp.InodeInfo, int32) {
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

// List
func (ns *nameSpace) List(path string) ([]*mp.InodeInfo, int32) {
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

	var pTmpInodeInfo *mp.InodeInfo
	for _, value := range pInodeInfo.ChildrenInodeIDs {
		ok, pTmpInodeInfo = ns.InodeDBGet(strconv.FormatInt(value, 10))
		if !ok {
			continue
		}
		tmpInodeInfos = append(tmpInodeInfos, pTmpInodeInfo)
	}
	return tmpInodeInfos, ret
}

// DeleteDir
func (ns *nameSpace) DeleteDir(path string) int32 {
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
	tmpKey := strconv.FormatInt(pTmpParentInodeInfo.InodeID, 10)
	err := ns.InodeDBSet(tmpKey, pTmpParentInodeInfo)
	if err != nil {
		return 1
	}
	err = ns.InodeDBSet(keys[keysNum-2], pTmpParentInodeInfo)
	if err != nil {
		return 1
	}

	/*delete inode info*/
	tmpKey = strconv.FormatInt(pInodeInfo.InodeID, 10)
	err = ns.InodeDBDelete(tmpKey)
	if err != nil {
		return 1
	}
	err = ns.InodeDBDelete(strconv.FormatInt(pInodeInfo.ParentInodeID, 10) + "-" + utils.GetSelfName(path))
	if err != nil {
		return 1
	}
	return ret
}

// Rename
func (ns *nameSpace) Rename(path1 string, path2 string) int32 {
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

	// delete old inode key
	tmpKey := strconv.FormatInt(pInodeInfo.InodeID, 10)
	err := ns.InodeDBDelete(tmpKey)
	if err != nil {
		return 1
	}
	// delete old parentID + name key
	err = ns.InodeDBDelete(strconv.FormatInt(pInodeInfo.ParentInodeID, 10) + "-" + utils.GetSelfName(path1))
	if err != nil {
		return 1
	}
	// add a new parentID + name key
	name := utils.GetSelfName(path2)
	tmpInodeInfo := mp.InodeInfo{
		ParentInodeID:    pParentInodeInfo2.InodeID,
		InodeID:          pInodeInfo.InodeID,
		Name:             name,
		AccessTime:       pInodeInfo.AccessTime,
		ModifiTime:       pInodeInfo.ModifiTime,
		InodeType:        pInodeInfo.InodeType,
		FileSize:         pInodeInfo.FileSize,
		ChunkIDs:         pInodeInfo.ChunkIDs,
		ChildrenInodeIDs: pInodeInfo.ChildrenInodeIDs}

	// add a new inode key
	tmpKey = strconv.FormatInt(pInodeInfo.InodeID, 10)
	err = ns.InodeDBSet(tmpKey, &tmpInodeInfo)
	if err != nil {
		return 1
	}

	tmpKey = strconv.FormatInt(pParentInodeInfo2.InodeID, 10) + "-" + name
	err = ns.InodeDBSet(tmpKey, &tmpInodeInfo)
	if err != nil {
		return 1
	}
	/*
		// modify parents inodeinfo if they are not same
		if key2s[key2sNum-2] != key1s[key1sNum-2] {
			// update patent1 inode info
			var pTmpParentInodeInfo1 *mp.InodeInfo
			_, pTmpParentInodeInfo1 = ns.InodeDBGet(key1s[key1sNum-2])
			for index, value := range pTmpParentInodeInfo1.ChildrenInodeIDs {
				if value == pInodeInfo.InodeID {
					pTmpParentInodeInfo1.ChildrenInodeIDs = append(pTmpParentInodeInfo1.ChildrenInodeIDs[:index], pTmpParentInodeInfo1.ChildrenInodeIDs[index+1:]...)
					break
				}
			}
			ns.InodeDBSet(strconv.FormatInt(pTmpParentInodeInfo1.InodeID, 10), pTmpParentInodeInfo1)
			ns.InodeDBSet(strconv.FormatInt(pTmpParentInodeInfo1.ParentInodeID, 10)+"-"+utils.GetParentName(path1), pTmpParentInodeInfo1)

			// update patent2 inode info
			pParentInodeInfo2.ChildrenInodeIDs = append(pParentInodeInfo2.ChildrenInodeIDs, pInodeInfo.InodeID)
			parent2Name := utils.GetParentName(path2)
			if parent2Name == "/" {
				ns.InodeDBSet("0", pParentInodeInfo2)
			} else {
				ns.InodeDBSet(strconv.FormatInt(pParentInodeInfo2.InodeID, 10), pParentInodeInfo2)
				tmpKey2 := strconv.FormatInt(pParentInodeInfo2.ParentInodeID, 10) + "-" + utils.GetParentName(path2)
				ns.InodeDBSet(tmpKey2, pParentInodeInfo2)
			}
		}
	*/

	return ret
}

// CreateFile
func (ns *nameSpace) CreateFile(path string) int32 {

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

	tmpKey := strconv.FormatInt(inodeID, 10)
	err = ns.InodeDBSet(tmpKey, &tmpInodeInfo)
	if err != nil {
		return 1
	}

	tmpKey = strconv.FormatInt(pParentInodeInfo.InodeID, 10) + "-" + name
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
		tmpKey = strconv.FormatInt(pParentInodeInfo.InodeID, 10)
		err = ns.InodeDBSet(tmpKey, pParentInodeInfo)
		if err != nil {
			return 1
		}

		tmpKey = strconv.FormatInt(pParentInodeInfo.ParentInodeID, 10) + "-" + utils.GetParentName(path)
		err = ns.InodeDBSet(tmpKey, pParentInodeInfo)
		if err != nil {
			return 1
		}
	}

	return ret
}

// DeleteFile
func (ns *nameSpace) DeleteFile(path string) int32 {
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
	tmpKey := strconv.FormatInt(pTmpParentInodeInfo.InodeID, 10)
	err := ns.InodeDBSet(tmpKey, pTmpParentInodeInfo)
	if err != nil {
		return 1
	}

	err = ns.InodeDBSet(keys[keysNum-2], pTmpParentInodeInfo)
	if err != nil {
		return 1
	}

	/*delete chunk info*/
	for _, value := range pInodeInfo.ChunkIDs {

		ok, chunkInfo := ns.ChunkDBGet(value)
		if !ok {
			continue
		}
		/*release bg cnt*/
		ns.ReleaseBlockGroup(chunkInfo.BlockGroupID)
		ns.ChunkDBDelete(value)
	}

	/*delete inode info*/
	tmpKey = strconv.FormatInt(pInodeInfo.InodeID, 10)
	err = ns.InodeDBDelete(tmpKey)
	if err != nil {
		return 1
	}
	err = ns.InodeDBDelete(strconv.FormatInt(pInodeInfo.ParentInodeID, 10) + "-" + utils.GetSelfName(path))
	if err != nil {
		return 1
	}
	return ret
}

// AllocateChunk
func (ns *nameSpace) AllocateChunk(path string) (int32, *mp.ChunkInfo) {
	var ret int32

	fmt.Println("AllocateChunk...")
	keys := ns.GetAllKeyByFullPath(path)
	keysNum := len(keys)

	if ok, _ := ns.InodeDBGet(keys[keysNum-1]); !ok {
		ret = 2 /*ENOENT*/
		return ret, nil
	}

	var chunkInfo = mp.ChunkInfo{}
	ret, _, blockGroup := ns.ChooseBlockGroup()

	fmt.Println("ChooseBlockGroup...")
	fmt.Println(ret)
	fmt.Println(blockGroup)

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

	fmt.Println("chunkInfo...")
	fmt.Println(chunkInfo)

	return 0, &chunkInfo

}

// GetFileChunks
func (ns *nameSpace) GetFileChunks(path string) (int32, []*mp.ChunkInfo) {
	var ret int32
	var ok bool
	keys := ns.GetAllKeyByFullPath(path)
	keysNum := len(keys)

	var pTmpInodeInfo *mp.InodeInfo
	if ok, pTmpInodeInfo = ns.InodeDBGet(keys[keysNum-1]); !ok {
		ret = 2 /*ENOENT*/
		return ret, nil
	}
	chunkInfos := make([]*mp.ChunkInfo, 0)

	for i := range pTmpInodeInfo.ChunkIDs {
		chunkID := pTmpInodeInfo.ChunkIDs[i]
		ok, tmpChunkInfo := ns.ChunkDBGet(chunkID)
		if !ok {
			continue
		}
		chunkInfos = append(chunkInfos, tmpChunkInfo)
	}

	return 0, chunkInfos

}

// SyncChunk
func (ns *nameSpace) SyncChunk(path string, chunkinfo *mp.ChunkInfo) int32 {
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

	var lastChunkInfo *mp.ChunkInfo
	var lastChunkID int64
	if len(pTmpInodeInfo.ChunkIDs) > 0 {
		// for appned write
		lastChunkID = pTmpInodeInfo.ChunkIDs[len(pTmpInodeInfo.ChunkIDs)-1]
		if lastChunkID == chunkinfo.ChunkID {
			ok, lastChunkInfo = ns.ChunkDBGet(chunkinfo.ChunkID)
			if !ok {
				return 1
			}
			pTmpInodeInfo.FileSize = pTmpInodeInfo.FileSize + int64(chunkinfo.ChunkSize) - int64(lastChunkInfo.ChunkSize)
		} else {
			pTmpInodeInfo.ChunkIDs = append(pTmpInodeInfo.ChunkIDs, chunkinfo.ChunkID)
			pTmpInodeInfo.FileSize += int64(chunkinfo.ChunkSize)
		}
	} else {
		pTmpInodeInfo.ChunkIDs = append(pTmpInodeInfo.ChunkIDs, chunkinfo.ChunkID)
		pTmpInodeInfo.FileSize += int64(chunkinfo.ChunkSize)
	}
	tmpKey := strconv.FormatInt(pTmpInodeInfo.InodeID, 10)
	err := ns.InodeDBSet(tmpKey, pTmpInodeInfo)
	if err != nil {
		return 1
	}
	err = ns.InodeDBSet(keys[keysNum-1], pTmpInodeInfo)
	if err != nil {
		return 1
	}
	err = ns.ChunkDBSet(chunkinfo.ChunkID, chunkinfo)
	if err != nil {
		return 1
	}

	return 0

}

// BlockGroupVp2Mp
func (ns *nameSpace) BlockGroupVp2Mp(in *vp.BlockGroup) *mp.BlockGroup {
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
		mpBlockInfos[i] = &mpBlockInfo

	}

	mpBlockGroup.BlockInfos = mpBlockInfos
	return &mpBlockGroup

}

// ChooseBlockGroup
func (ns *nameSpace) ChooseBlockGroup() (int32, int32, *vp.BlockGroup) {
	blkgrps, err := ns.VolumeDBGet(ns.VolID)
	if err != nil {
		return 1, -1, nil
	}

	var blockGroupID int32
	var blockGroup *vp.BlockGroup
	flag := false

	logger.Debug("blkgrps:%v\n", blkgrps)

	for i := range blkgrps {
		ret, bg := ns.BlockGroupDBGet(blkgrps[i])
		if !ret {
			continue
		}
		logger.Debug("bg1:%v\n", bg)

		if bg.Status == 1 {
			blockGroupID = bg.BlockGroupID
			bg.FreeCnt = bg.FreeCnt - 1
			if bg.FreeCnt <= 0 {
				bg.Status = 2
				ns.SetBlockGroupStatus(blockGroupID, bg.Status)
			}
			blockGroup = bg
			logger.Debug("find a using blockgroup,blgid:%v\n", bg.BlockGroupID)
			flag = true
			break
		}
	}
	// find the free blockgroup
	if flag == false {
		for i := range blkgrps {
			ret, bg := ns.BlockGroupDBGet(blkgrps[i])
			if !ret {
				continue
			}
			logger.Debug("bg2:%v\n", bg)
			if bg.Status == 0 {
				blockGroupID = bg.BlockGroupID
				bg.FreeCnt = bg.FreeCnt - 1
				if bg.FreeCnt == 0 {
					bg.Status = 2
					ns.SetBlockGroupStatus(blockGroupID, bg.Status)
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

// ReleaseBlockGroup
func (ns *nameSpace) ReleaseBlockGroup(blockGroupID int32) {

	ok, blockGroup := ns.BlockGroupDBGet(blockGroupID)
	if !ok {
		return
	}

	var status int32
	blockGroup.FreeCnt += 1
	if blockGroup.FreeCnt > 160 {
		blockGroup.FreeCnt = 160
	}
	if blockGroup.FreeCnt > 0 {
		status = 1
		if blockGroup.Status != status && blockGroup.Status != 3 {
			blockGroup.Status = 1
			ns.SetBlockGroupStatus(blockGroupID, blockGroup.Status)
		}

	}
	if blockGroup.FreeCnt == 160 {
		status = 0
		if blockGroup.Status != status && blockGroup.Status != 3 {
			blockGroup.Status = 0
			ns.SetBlockGroupStatus(blockGroupID, blockGroup.Status)
		}

	}

	ns.BlockGroupDBSet(blockGroupID, blockGroup)

}

// SetBlockGroupStatus
func (ns *nameSpace) SetBlockGroupStatus(blockGroupID int32, status int32) {
	conn, err := grpc.Dial(VolMgrAddress, grpc.WithInsecure())
	if err != nil {
		logger.Debug("Dial failed: %v", err)
		return
	}
	defer conn.Close()
	vc := vp.NewVolMgrClient(conn)
	pSetBlockGroupStatusReq := &vp.SetBlockGroupStatusReq{}
	pSetBlockGroupStatusReq.BlockGroupID = blockGroupID
	pSetBlockGroupStatusReq.Status = status
	vc.SetBlockGroupStatus(context.Background(), pSetBlockGroupStatusReq)
}

// UpdateBlkGrp
func (ns *nameSpace) UpdateBlkGrp(blockGroupID int32, blockID int32, status int32) int32 {
	/*
		ok, blockGroup := ns.BlockGroupDBGet(blockGroupID)
		if !ok {
			return -1
		}
		if 0 != status {
			for i := range blockGroup.BlockInfos {
				if blockGroup.BlockInfos[i].BlockID == blockID {
					blockGroup.BlockInfos = append(blockGroup.BlockInfos[:i], blockGroup.BlockInfos[i+1:]...)
					break
				}
			}
			if len(blockGroup.BlockInfos) == 0 {
				blockGroup.Status = 3
			}
			ns.BlockGroupDBSet(blockGroupID, blockGroup)
		}
	*/
	return 0
}

// GetAllKeyByFullPath
func (ns *nameSpace) GetAllKeyByFullPath(in string) (keys []string) {
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

/*
// VolumeEtcdSet
func (ns *nameSpace) VolumeDBSet(volID string, v []int32) {
	val, _ := json.Marshal(v)
	RedisClusters[1].Do("HMSET", ns.VolID, "VolDB", val)
}

// VolumeEtcdGet
func (ns *nameSpace) VolumeDBGet(volID string) []int32 {
	value, _ := redis.Strings(RedisClusters[1].Do("HMGET", ns.VolID, "VolDB"))
	var blkgrps []int32
	json.Unmarshal([]byte(value[0]), &blkgrps)
	return blkgrps
}


// AllocateInodeID
func (ns *nameSpace) AllocateInodeID() (int64, error) {
	id, err := redis.Int64(RedisClusters[0].Do("INCR", ns.VolID+"-InodeID"))
	return id, err
}

// AllocateChunkID
func (ns *nameSpace) AllocateChunkID() (int64, error) {
	id, err := redis.Int64(RedisClusters[1].Do("INCR", ns.VolID+"-ChunkID"))
	return id, err
}

// InodeDBGet
func (ns *nameSpace) InodeDBGet(k string) (bool, *mp.InodeInfo) {
	value, err := redis.Strings(RedisClusters[1].Do("HMGET", ns.VolID, "InodeDB/"+k))
	inodeInfo := mp.InodeInfo{}
	err = json.Unmarshal([]byte(value[0]), &inodeInfo)
	if err != nil {
		return false, nil
	}
	return true, &inodeInfo
}

// InodeDBSet
func (ns *nameSpace) InodeDBSet(k string, v *mp.InodeInfo) {
	val, _ := json.Marshal(v)
	RedisClusters[1].Do("HMSET", ns.VolID, "InodeDB/"+k, val)
}

// InodeDBDelete
func (ns *nameSpace) InodeDBDelete(k string) {
	RedisClusters[1].Do("HDEL", ns.VolID, "InodeDB/"+k)
}

// BlockGroupDBGet
func (ns *nameSpace) BlockGroupDBGet(k int32) (bool, *vp.BlockGroup) {
	value, err := redis.Strings(RedisClusters[1].Do("HMGET", ns.VolID, "BGDB/"+strconv.Itoa(int(k))))
	blockGroup := vp.BlockGroup{}
	err = json.Unmarshal([]byte(value[0]), &blockGroup)
	if err != nil {
		return false, nil
	}
	return true, &blockGroup

}

// BlockGroupDBSet
func (ns *nameSpace) BlockGroupDBSet(k int32, v *vp.BlockGroup) {
	val, _ := json.Marshal(v)
	RedisClusters[1].Do("HMSET", ns.VolID, "BGDB/"+strconv.Itoa(int(k)), val)
}

// BlockGroupDBDelete
func (ns *nameSpace) BlockGroupDBDelete(k int32) {
	RedisClusters[1].Do("HDEL", ns.VolID, "BGDB/"+strconv.Itoa(int(k)))
}

// ChunkDBGet
func (ns *nameSpace) ChunkDBGet(k int64) (bool, *mp.ChunkInfo) {
	value, err := redis.Strings(RedisClusters[1].Do("HMGET", ns.VolID, "ChunkDB/"+strconv.FormatInt(k, 10)))
	chunkInfo := mp.ChunkInfo{}
	err = json.Unmarshal([]byte(value[0]), &chunkInfo)
	if err != nil {
		return false, nil
	}
	return true, &chunkInfo

}

// ChunkDBSet
func (ns *nameSpace) ChunkDBSet(k int64, v *mp.ChunkInfo) {
	val, _ := json.Marshal(v)
	RedisClusters[1].Do("HMSET", ns.VolID, "ChunkDB/"+strconv.FormatInt(k, 10), val)
}

// ChunkDBDelete
func (ns *nameSpace) ChunkDBDelete(k int64) {
	RedisClusters[1].Do("HDEL", ns.VolID, "ChunkDB/"+strconv.FormatInt(k, 10))
}

*/

// VolumeEtcdSet
func (ns *nameSpace) VolumeDBSet(volID string, v []int32) error {
	val, _ := json.Marshal(v)
	err := RedisClient.HSet(ns.VolID, "VolDB", val).Err()
	if err != nil {
		time.Sleep(time.Second * 2)
		err = RedisClient.HSet(ns.VolID, "VolDB", val).Err()
		if err != nil {
			logger.Error("VolumeDBSet vol:%v,key:%v,err:%v\n", ns.VolID, "VolDB", err)
			return err
		}
	}
	return nil

}

// VolumeEtcdGet
func (ns *nameSpace) VolumeDBGet(volID string) ([]int32, error) {
	value, err := RedisClient.HGet(ns.VolID, "VolDB").Result()
	if err != nil {
		time.Sleep(time.Second * 2)
		value, err = RedisClient.HGet(ns.VolID, "VolDB").Result()
		if err != nil {
			logger.Error("VolumeDBGet vol:%v,key:%v,err:%v\n", ns.VolID, "VolDB", err)
			return nil, err
		}
	}

	var blkgrps []int32
	json.Unmarshal([]byte(value), &blkgrps)
	return blkgrps, nil
}

// AllocateInodeID
func (ns *nameSpace) AllocateInodeID() (int64, error) {
	id, err := RedisClient.Incr(ns.VolID + "-InodeID").Result()
	if err != nil {
		time.Sleep(time.Second * 2)
		id, err = RedisClient.Incr(ns.VolID + "-InodeID").Result()
		if err != nil {
			logger.Error("AllocateInodeID Incr failed err :%v\n", err)
			return -1, err
		}
	}

	return id, nil
}

// AllocateChunkID
func (ns *nameSpace) AllocateChunkID() (int64, error) {
	id, err := RedisClient.Incr(ns.VolID + "-ChunkID").Result()
	if err != nil {
		time.Sleep(time.Second * 2)
		id, err = RedisClient.Incr(ns.VolID + "-ChunkID").Result()
		if err != nil {
			logger.Error("AllocateChunkID Incr failed err :%v\n", err)
			return -1, err
		}
	}

	return id, nil
}

// InodeDBGet
func (ns *nameSpace) InodeDBGet(k string) (bool, *mp.InodeInfo) {
	value, err := RedisClient.HGet(ns.VolID, "InodeDB/"+k).Result()
	if err != nil {
		if err.Error() != "redis: nil" {
			time.Sleep(time.Second * 2)
			value, err = RedisClient.HGet(ns.VolID, "InodeDB/"+k).Result()
			if err != nil {
				logger.Error("InodeDBGet vol:%v,key:%v,err:%v\n", ns.VolID, "InodeDB/"+k, err)
				return false, nil
			}
		} else {
			return false, nil
		}
	}

	inodeInfo := mp.InodeInfo{}
	err = json.Unmarshal([]byte(value), &inodeInfo)
	if err != nil {
		return false, nil
	}
	return true, &inodeInfo
}

// InodeDBSet
func (ns *nameSpace) InodeDBSet(k string, v *mp.InodeInfo) error {
	val, _ := json.Marshal(v)
	err := RedisClient.HSet(ns.VolID, "InodeDB/"+k, val).Err()
	if err != nil {
		time.Sleep(time.Second * 2)
		err := RedisClient.HSet(ns.VolID, "InodeDB/"+k, val).Err()
		if err != nil {
			logger.Error("InodeDBSet vol:%v,key:%v,err:%v\n", ns.VolID, "InodeDB/"+k, err)
			return err
		}
	}
	return nil

}

// InodeDBDelete
func (ns *nameSpace) InodeDBDelete(k string) error {
	err := RedisClient.HDel(ns.VolID, "InodeDB/"+k).Err()
	if err != nil {
		time.Sleep(time.Second * 2)
		err = RedisClient.HDel(ns.VolID, "InodeDB/"+k).Err()
		if err != nil {
			logger.Error("InodeDBDelete vol:%v,key:%v,err:%v\n", ns.VolID, "InodeDB/"+k, err)
			return err
		}
	}
	return nil
}

// BlockGroupDBGet
func (ns *nameSpace) BlockGroupDBGet(k int32) (bool, *vp.BlockGroup) {
	value, err := RedisClient.HGet(ns.VolID, "BGDB/"+strconv.Itoa(int(k))).Result()
	if err != nil {
		if err.Error() != "redis: nil" {
			time.Sleep(time.Second * 2)
			value, err = RedisClient.HGet(ns.VolID, "BGDB/"+strconv.Itoa(int(k))).Result()
			if err != nil {
				logger.Error("BlockGroupDBGet vol:%v,key:%v,err:%v\n", ns.VolID, "BGDB/"+strconv.Itoa(int(k)), err)
				return false, nil
			}
		} else {
			return false, nil
		}
	}

	blockGroup := vp.BlockGroup{}
	err = json.Unmarshal([]byte(value), &blockGroup)
	if err != nil {
		return false, nil
	}
	return true, &blockGroup

}

// BlockGroupDBSet
func (ns *nameSpace) BlockGroupDBSet(k int32, v *vp.BlockGroup) error {
	val, _ := json.Marshal(v)
	err := RedisClient.HSet(ns.VolID, "BGDB/"+strconv.Itoa(int(k)), val).Err()
	if err != nil {
		time.Sleep(time.Second * 2)
		err = RedisClient.HSet(ns.VolID, "BGDB/"+strconv.Itoa(int(k)), val).Err()
		if err != nil {
			logger.Error("BlockGroupDBSet vol:%v,key:%v,err=%v\n", ns.VolID, "BGDB/"+strconv.Itoa(int(k)), err)
			return err
		}
	}
	return nil
}

// BlockGroupDBDelete
func (ns *nameSpace) BlockGroupDBDelete(k int32) {
	err := RedisClient.HDel(ns.VolID, "BGDB/"+strconv.Itoa(int(k))).Err()
	if err != nil {
		time.Sleep(time.Second * 2)
		err = RedisClient.HDel(ns.VolID, "BGDB/"+strconv.Itoa(int(k))).Err()
		if err != nil {
			logger.Error("InodeDBDelete vol:%v,key:%v,err:%v\n", ns.VolID, "BGDB/"+strconv.Itoa(int(k)), err)
		}
	}

}

// ChunkDBGet
func (ns *nameSpace) ChunkDBGet(k int64) (bool, *mp.ChunkInfo) {
	value, err := RedisClient.HGet(ns.VolID, "ChunkDB/"+strconv.FormatInt(k, 10)).Result()
	if err != nil {
		if err.Error() != "redis: nil" {
			time.Sleep(time.Second * 2)
			value, err = RedisClient.HGet(ns.VolID, "ChunkDB/"+strconv.FormatInt(k, 10)).Result()
			if err != nil {
				logger.Error("ChunkDBGet vol:%v,key:%v,err:%v\n", ns.VolID, "ChunkDB/"+strconv.FormatInt(k, 10), err)
				return false, nil
			}
		} else {
			return false, nil
		}
	}
	chunkInfo := mp.ChunkInfo{}
	err = json.Unmarshal([]byte(value), &chunkInfo)
	if err != nil {
		return false, nil
	}
	return true, &chunkInfo

}

// ChunkDBSet
func (ns *nameSpace) ChunkDBSet(k int64, v *mp.ChunkInfo) error {
	val, _ := json.Marshal(v)
	err := RedisClient.HSet(ns.VolID, "ChunkDB/"+strconv.FormatInt(k, 10), val).Err()
	if err != nil {
		time.Sleep(time.Second * 2)
		err = RedisClient.HSet(ns.VolID, "ChunkDB/"+strconv.FormatInt(k, 10), val).Err()
		if err != nil {
			logger.Error("ChunkDBSet vol:%v,key:%v,err:%v\n", ns.VolID, "ChunkDB/"+strconv.FormatInt(k, 10), err)
			return err
		}

	}
	return nil
}

// ChunkDBDelete
func (ns *nameSpace) ChunkDBDelete(k int64) {
	err := RedisClient.HDel(ns.VolID, "ChunkDB/"+strconv.FormatInt(k, 10)).Err()
	if err != nil {
		time.Sleep(time.Second * 2)
		err = RedisClient.HDel(ns.VolID, "ChunkDB/"+strconv.FormatInt(k, 10)).Err()
		if err != nil {
			logger.Error("ChunkDBDelete vol:%v,key:%v,err:%v\n", ns.VolID, "ChunkDB/"+strconv.FormatInt(k, 10), err)
		}
	}

}
