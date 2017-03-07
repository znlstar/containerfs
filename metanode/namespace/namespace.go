package namespace

import (
	"bytes"
	"fmt"
	mp "ipd.org/containerfs/proto/mp"
	vp "ipd.org/containerfs/proto/vp"

	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"ipd.org/containerfs/utils"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	volMgrAddress   = "10.8.65.94:10001"
	mataNodeAddress = "10.8.65.94:10002"
)

type nameSpace struct {
	InodeDB      map[string]*mp.InodeInfo
	Mutex        sync.RWMutex
	BlockBroupDB map[int32]*vp.BlockGroup
	BGMutex      sync.Mutex
	ChunkDB      map[int64]*mp.ChunkInfo
	CMutex       sync.RWMutex
	BaseInodeID  *utils.AutoInc
	BaseChunkID  *utils.AutoInc
}

var AllNameSpace map[string]*nameSpace
var gMutex sync.RWMutex

func CreateGNameSpace() {
	gMutex.Lock()
	AllNameSpace = make(map[string]*nameSpace)
	gMutex.Unlock()
}
func CreateNameSpace(UUID string, inodenum int64, chunknum int64) int32 {
	nameSpace := nameSpace{}
	nameSpace.InodeDB = make(map[string]*mp.InodeInfo)
	nameSpace.BlockBroupDB = make(map[int32]*vp.BlockGroup)
	nameSpace.ChunkDB = make(map[int64]*mp.ChunkInfo)
	nameSpace.BaseInodeID = utils.New(inodenum, 1)
	nameSpace.BaseChunkID = utils.New(chunknum, 1)

	//fmt.Println("CreateNameSpace in namespace ...")
	ret, tmpBlockGroups := nameSpace.GetVolInfo(UUID)
	if ret != 0 {
		return ret
	}
	for _, v := range tmpBlockGroups {
		nameSpace.BGMutex.Lock()
		v.FreeCnt = 320
		nameSpace.BlockBroupDB[v.BlockGroupID] = v
		nameSpace.BGMutex.Unlock()
	}
	fmt.Println(nameSpace.BlockBroupDB)
	gMutex.Lock()
	AllNameSpace[UUID] = &nameSpace
	gMutex.Unlock()

	tmpInodeInfo := mp.InodeInfo{
		InodeID: 0, Name: "/",
		AccessTime: time.Now().Unix(),
		ModifiTime: time.Now().Unix(),
		InodeType:  false}
	nameSpace.Set("0", &tmpInodeInfo)

	return 0
}

func GetNameSpace(UUID string) (int32, *nameSpace) {
	gMutex.RLock()
	defer gMutex.RUnlock()
	if v, ok := AllNameSpace[UUID]; ok {
		return 0, v
	} else {
		return -1, nil
	}
}

func (ns *nameSpace) GetVolInfo(name string) (int32, []*vp.BlockGroup) {
	//fmt.Println("GetVolInfo ... ")
	conn, err := grpc.Dial(volMgrAddress, grpc.WithInsecure())
	if err != nil {
		fmt.Printf("did not connect: %v", err)
	}
	defer conn.Close()
	vc := vp.NewVolMgrClient(conn)
	pGetVolInfoReq := &vp.GetVolInfoReq{UUID: name}
	pGetVolInfoAck, _ := vc.GetVolInfo(context.Background(), pGetVolInfoReq)
	if pGetVolInfoAck.Ret != 0 {
		return pGetVolInfoAck.Ret, nil
	}
	return 0, pGetVolInfoAck.VolInfo.BlockGroups
}

func (ns *nameSpace) CreateDir(path string) int32 {
	var ret int32
	ret = 0
	keys := ns.GetAllKeyByFullPath(path)
	keysNum := len(keys)
	//fmt.Println(keys)
	var pParentInodeInfo *mp.InodeInfo
	var ok bool
	if ok, pParentInodeInfo = ns.Get(keys[keysNum-2]); !ok {
		ret = 2 /*ENOENT*/
		return ret
	}
	if pParentInodeInfo.InodeType == true {
		ret = 1 /*EPERM*/
		return ret
	}
	if ok, _ = ns.Get(keys[keysNum-1]); ok {
		ret = 17 /*ENOENT*/
		return ret
	}
	/*update inode info*/
	inodeID := ns.AllocateInodeID()
	name := utils.GetSelfName(path)
	tmpInodeInfo := mp.InodeInfo{
		ParentInodeID: pParentInodeInfo.InodeID,
		InodeID:       inodeID,
		Name:          name,
		AccessTime:    time.Now().Unix(),
		ModifiTime:    time.Now().Unix(),
		InodeType:     false}

	ns.Set(strconv.FormatInt(inodeID, 10), &tmpInodeInfo)
	tmpKey := strconv.FormatInt(pParentInodeInfo.InodeID, 10) + "-" + name
	ns.Set(tmpKey, &tmpInodeInfo)
	/*update patent inode info*/
	pParentInodeInfo.ChildrenInodeIDs = append(pParentInodeInfo.ChildrenInodeIDs, inodeID)
	parentName := utils.GetParentName(path)
	if parentName == "/" {
		ns.Set("0", pParentInodeInfo)
	} else {
		ns.Set(strconv.FormatInt(pParentInodeInfo.InodeID, 10), pParentInodeInfo)
		tmpKey = strconv.FormatInt(pParentInodeInfo.ParentInodeID, 10) + "-" + utils.GetParentName(path)
		ns.Set(tmpKey, pParentInodeInfo)
	}
	return ret
}

func (ns *nameSpace) Stat(path string) (*mp.InodeInfo, int32) {
	var ret int32
	ret = 0
	var ok bool
	var pInodeInfo *mp.InodeInfo

	if path == "/" {
		_, pInodeInfo = ns.Get("0")
		//		ack = mp.StatAck{Ret: 0, InodeInfo: pInodeInfo}
		return pInodeInfo, 0
	}
	keys := ns.GetAllKeyByFullPath(path)
	keysNum := len(keys)

	if ok, pInodeInfo = ns.Get(keys[keysNum-1]); !ok {
		ret = 2 /*ENOENT*/
		return nil, ret
	}
	return pInodeInfo, 0
}

func (ns *nameSpace) List(path string) ([]*mp.InodeInfo, int32) {
	var ret int32
	ret = 0

	var pInodeInfo *mp.InodeInfo
	var ok bool
	if path == "/" {
		_, pInodeInfo = ns.Get("0")
	} else {
		keys := ns.GetAllKeyByFullPath(path)
		keysNum := len(keys)

		if ok, pInodeInfo = ns.Get(keys[keysNum-1]); !ok {
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
		_, pTmpInodeInfo = ns.Get(strconv.FormatInt(value, 10))
		tmpInodeInfos = append(tmpInodeInfos, pTmpInodeInfo)
	}
	return tmpInodeInfos, ret
}

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

	if ok, pInodeInfo = ns.Get(keys[keysNum-1]); !ok {
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
	_, pTmpParentInodeInfo = ns.Get(keys[keysNum-2])
	for index, value := range pTmpParentInodeInfo.ChildrenInodeIDs {
		if value == pInodeInfo.InodeID {
			//fmt.Println("find the child index:")
			pTmpParentInodeInfo.ChildrenInodeIDs = append(pTmpParentInodeInfo.ChildrenInodeIDs[:index], pTmpParentInodeInfo.ChildrenInodeIDs[index+1:]...)
			break
		}
	}
	ns.Set(keys[keysNum-2], pTmpParentInodeInfo)
	ns.Set(strconv.FormatInt(pTmpParentInodeInfo.ParentInodeID, 10)+"-"+utils.GetParentName(path), pTmpParentInodeInfo)

	/*delete inode info*/
	ns.Delete(strconv.FormatInt(pInodeInfo.InodeID, 10))
	ns.Delete(strconv.FormatInt(pInodeInfo.ParentInodeID, 10) + "-" + utils.GetSelfName(path))

	return ret
}

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
	if ok, pInodeInfo = ns.Get(key1s[key1sNum-1]); !ok {
		ret = 17 /*ENOENT*/
		return ret
	}

	key2s := ns.GetAllKeyByFullPath(path2)
	key2sNum := len(key2s)

	var pParentInodeInfo2 *mp.InodeInfo

	if ok, pParentInodeInfo2 = ns.Get(key2s[key2sNum-2]); !ok {
		ret = 2 /*ENOENT*/
		return ret
	}
	if pParentInodeInfo2.InodeType == true {
		ret = 1 /*EPERM*/
		return ret
	}
	if ok, _ = ns.Get(key2s[key2sNum-1]); ok {
		ret = 17 /*ENOENT*/
		return ret
	}

	/*add a new parentID + name key  */
	name := utils.GetSelfName(path2)
	tmpInodeInfo := mp.InodeInfo{
		ParentInodeID:    pParentInodeInfo2.InodeID,
		InodeID:          pInodeInfo.InodeID,
		Name:             name,
		AccessTime:       pInodeInfo.AccessTime,
		ModifiTime:       pInodeInfo.ModifiTime,
		InodeType:        pInodeInfo.InodeType,
		ChildrenInodeIDs: pInodeInfo.ChildrenInodeIDs}

	tmpKey := strconv.FormatInt(pParentInodeInfo2.InodeID, 10) + "-" + name
	ns.Set(tmpKey, &tmpInodeInfo)

	/*modfiy inode key */
	ns.Set(strconv.FormatInt(pInodeInfo.InodeID, 10), &tmpInodeInfo)

	/*modify parents inodeinfo if they are not same*/
	if key2s[key2sNum-2] != key1s[key1sNum-2] {
		/*update patent1 inode info*/
		var pTmpParentInodeInfo1 *mp.InodeInfo
		_, pTmpParentInodeInfo1 = ns.Get(key1s[key1sNum-2])
		for index, value := range pTmpParentInodeInfo1.ChildrenInodeIDs {
			if value == pInodeInfo.InodeID {
				//fmt.Println("find the child index:")
				pTmpParentInodeInfo1.ChildrenInodeIDs = append(pTmpParentInodeInfo1.ChildrenInodeIDs[:index], pTmpParentInodeInfo1.ChildrenInodeIDs[index+1:]...)
				break
			}
		}
		ns.Set(strconv.FormatInt(pTmpParentInodeInfo1.InodeID, 10), pTmpParentInodeInfo1)
		ns.Set(strconv.FormatInt(pTmpParentInodeInfo1.ParentInodeID, 10)+"-"+utils.GetParentName(path1), pTmpParentInodeInfo1)

		/*update patent2 inode info*/
		pParentInodeInfo2.ChildrenInodeIDs = append(pParentInodeInfo2.ChildrenInodeIDs, pInodeInfo.InodeID)
		parent2Name := utils.GetParentName(path2)
		if parent2Name == "/" {
			ns.Set("0", pParentInodeInfo2)
		} else {
			ns.Set(strconv.FormatInt(pParentInodeInfo2.InodeID, 10), pParentInodeInfo2)
			tmpKey2 := strconv.FormatInt(pParentInodeInfo2.ParentInodeID, 10) + "-" + utils.GetParentName(path2)
			ns.Set(tmpKey2, pParentInodeInfo2)
		}
	}
	/*delete old parentID + name key*/
	ns.Delete(strconv.FormatInt(pInodeInfo.ParentInodeID, 10) + "-" + utils.GetSelfName(path1))

	return ret
}
func (ns *nameSpace) CreateFile(path string) int32 {

	var ret int32
	ret = 0

	keys := ns.GetAllKeyByFullPath(path)
	keysNum := len(keys)

	//fmt.Println(keys)
	var ok bool
	var pParentInodeInfo *mp.InodeInfo
	if ok, pParentInodeInfo = ns.Get(keys[keysNum-2]); !ok {
		ret = 2 /*ENOENT*/
		return ret
	}

	if pParentInodeInfo.InodeType == true {
		ret = 1 /*EPERM*/
		return ret
	}

	if ok, _ = ns.Get(keys[keysNum-1]); ok {
		ret = 17 /*ENOENT*/
		return ret
	}

	/*update inode info*/
	inodeID := ns.AllocateInodeID()
	name := utils.GetSelfName(path)
	tmpInodeInfo := mp.InodeInfo{
		ParentInodeID: pParentInodeInfo.InodeID,
		InodeID:       inodeID,
		Name:          name,
		AccessTime:    time.Now().Unix(),
		ModifiTime:    time.Now().Unix(),
		InodeType:     true}

	ns.Set(strconv.FormatInt(inodeID, 10), &tmpInodeInfo)
	tmpKey := strconv.FormatInt(pParentInodeInfo.InodeID, 10) + "-" + name
	ns.Set(tmpKey, &tmpInodeInfo)

	/*update patent inode info*/
	pParentInodeInfo.ChildrenInodeIDs = append(pParentInodeInfo.ChildrenInodeIDs, inodeID)

	parentName := utils.GetParentName(path)
	if parentName == "/" {
		ns.Set("0", pParentInodeInfo)
	} else {
		ns.Set(strconv.FormatInt(pParentInodeInfo.InodeID, 10), pParentInodeInfo)
		tmpKey = strconv.FormatInt(pParentInodeInfo.ParentInodeID, 10) + "-" + utils.GetParentName(path)
		ns.Set(tmpKey, pParentInodeInfo)
	}

	return ret
}

func (ns *nameSpace) DeleteFile(path string) int32 {

	fmt.Println("DeleteFile in namespace ...")

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

	if ok, pInodeInfo = ns.Get(keys[keysNum-1]); !ok {
		ret = 0 /*ENOENT*/
		return ret
	}
	if pInodeInfo.InodeType == false {
		ret = 1 /*EPERM*/
		return ret
	}

	/*update patent inode info*/
	var pTmpParentInodeInfo *mp.InodeInfo
	_, pTmpParentInodeInfo = ns.Get(keys[keysNum-2])
	for index, value := range pTmpParentInodeInfo.ChildrenInodeIDs {
		if value == pInodeInfo.InodeID {
			//fmt.Println("find the child index:")
			pTmpParentInodeInfo.ChildrenInodeIDs = append(pTmpParentInodeInfo.ChildrenInodeIDs[:index], pTmpParentInodeInfo.ChildrenInodeIDs[index+1:]...)
			break
		}
	}
	ns.Set(keys[keysNum-2], pTmpParentInodeInfo)
	ns.Set(strconv.FormatInt(pTmpParentInodeInfo.ParentInodeID, 10)+"-"+utils.GetParentName(path), pTmpParentInodeInfo)

	/*delete chunk info*/
	for _, value := range pInodeInfo.ChunkIDs {

		ns.CMutex.Lock()
		chunkInfo, ok := ns.ChunkDB[value]
		ns.CMutex.Unlock()
		if !ok {
			continue
		}
		/*release bg cnt*/
		ns.ReleaseBlockGroup(chunkInfo.BlockGroup.BlockGroupID)
		ns.CMutex.Lock()
		delete(ns.ChunkDB, value)
		ns.CMutex.Unlock()
	}

	/*delete inode info*/
	ns.Delete(strconv.FormatInt(pInodeInfo.InodeID, 10))
	ns.Delete(strconv.FormatInt(pInodeInfo.ParentInodeID, 10) + "-" + utils.GetSelfName(path))

	fmt.Println("DeleteFile in namespace end ...")

	return ret
}
func (ns *nameSpace) AllocateChunk(path string) (int32, *mp.ChunkInfo) {
	//fmt.Println("AllocateChunk in ...")
	var ret int32

	keys := ns.GetAllKeyByFullPath(path)
	keysNum := len(keys)

	if ok, _ := ns.Get(keys[keysNum-1]); !ok {
		ret = 2 /*ENOENT*/
		return -1, nil
	}

	var chunkInfo = mp.ChunkInfo{}
	ret, _, blockGroup := ns.ChooseBlockGroup()
	if ret != 0 {
		return -1, nil
	}
	chunkInfo.BlockGroup = ns.blockGroupVp2Mp(blockGroup)
	chunkInfo.ChunkSize = 0
	chunkInfo.ChunkID = ns.AllocateChunkID()
	return 0, &chunkInfo

}

func (ns *nameSpace) GetFileChunks(path string) (int32, []*mp.ChunkInfo) {
	var ret int32
	var ok bool
	keys := ns.GetAllKeyByFullPath(path)
	keysNum := len(keys)

	var pTmpInodeInfo *mp.InodeInfo
	if ok, pTmpInodeInfo = ns.Get(keys[keysNum-1]); !ok {
		ret = 2 /*ENOENT*/
		return ret, nil
	}
	chunkInfos := make([]*mp.ChunkInfo, 0)
	ns.CMutex.RLock()
	for i := range pTmpInodeInfo.ChunkIDs {
		chunkID := pTmpInodeInfo.ChunkIDs[i]
		chunkInfos = append(chunkInfos, ns.ChunkDB[chunkID])
	}
	ns.CMutex.RUnlock()
	//fmt.Println("getfilechunks in ... ")
	//fmt.Println(chunkInfos)
	return 0, chunkInfos

}

func (ns *nameSpace) SyncChunk(path string, chunkinfo *mp.ChunkInfo) int32 {
	var ret int32
	var ok bool
	keys := ns.GetAllKeyByFullPath(path)
	keysNum := len(keys)

	var pTmpInodeInfo *mp.InodeInfo
	if ok, pTmpInodeInfo = ns.Get(keys[keysNum-1]); !ok {
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
			ns.CMutex.Lock()
			lastChunkInfo = ns.ChunkDB[chunkinfo.ChunkID]
			ns.CMutex.Unlock()
			pTmpInodeInfo.FileSize = pTmpInodeInfo.FileSize - int64(lastChunkInfo.ChunkSize) + int64(chunkinfo.ChunkSize)
		} else {
			pTmpInodeInfo.ChunkIDs = append(pTmpInodeInfo.ChunkIDs, chunkinfo.ChunkID)
			pTmpInodeInfo.FileSize += int64(chunkinfo.ChunkSize)
		}
	} else {
		pTmpInodeInfo.ChunkIDs = append(pTmpInodeInfo.ChunkIDs, chunkinfo.ChunkID)
		pTmpInodeInfo.FileSize += int64(chunkinfo.ChunkSize)
	}

	ns.Set(strconv.FormatInt(pTmpInodeInfo.InodeID, 10), pTmpInodeInfo)
	ns.Set(keys[keysNum-1], pTmpInodeInfo)

	ns.CMutex.Lock()
	ns.ChunkDB[chunkinfo.ChunkID] = chunkinfo
	ns.CMutex.Unlock()
	//fmt.Println("update chunkinfo to chunkDB ... ")
	//fmt.Println(ns.ChunkDB[chunkinfo.ChunkID])
	return 0

}

func (ns *nameSpace) blockGroupVp2Mp(in *vp.BlockGroup) *mp.BlockGroup {
	var mpBlockGroup = mp.BlockGroup{}

	mpBlockInfos := make([]*mp.BlockInfo, 1)

	mpBlockGroup.BlockGroupID = in.BlockGroupID
	mpBlockGroup.FreeCnt = in.FreeCnt
	mpBlockGroup.Status = in.Status
	//fmt.Println("blockGroupVp2Mp")
	//fmt.Println(in.BlockInfos)

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

func (ns *nameSpace) ChooseBlockGroup() (int32, int32, *vp.BlockGroup) {
	//fmt.Println("ChooseBlockGroup in ...")
	ns.BGMutex.Lock()
	defer ns.BGMutex.Unlock()
	var blockGroupID int32
	var blockGroup *vp.BlockGroup
	flag := false
	// find the using blockgroup
	for k, v := range ns.BlockBroupDB {
		if v.Status == 1 {
			blockGroupID = v.BlockGroupID
			v.FreeCnt = v.FreeCnt - 1
			if v.FreeCnt <= 0 {
				v.Status = 2
			}
			ns.BlockBroupDB[k] = v
			blockGroup = v
			fmt.Println("find a using blockgroup !!!")
			//fmt.Println(blockGroup)
			flag = true
			break
		}
	}
	// find the free blockgroup
	if flag == false {
		for k, v := range ns.BlockBroupDB {
			if v.Status == 0 {
				blockGroupID = v.BlockGroupID
				v.FreeCnt = v.FreeCnt - 1
				if v.FreeCnt == 0 {
					v.Status = 2
				} else {
					v.Status = 1
				}
				ns.BlockBroupDB[k] = v
				blockGroup = v
				fmt.Println("find a free blockgroup !!!")
				//fmt.Println(blockGroup)
				flag = true
				break
			}
		}
	}
	if flag {

		return 0, blockGroupID, blockGroup
	} else {
		return 1, -1, nil
	}

}

func (ns *nameSpace) ReleaseBlockGroup(blockGroupID int32) {
	fmt.Println("ReleaseBlockGroup in ...")
	ns.BGMutex.Lock()
	defer ns.BGMutex.Unlock()

	blockGroup := ns.BlockBroupDB[blockGroupID]
	blockGroup.FreeCnt += 1
	if blockGroup.FreeCnt > 0 {
		blockGroup.Status = 1
	}
	if blockGroup.FreeCnt >= 0 {
		blockGroup.Status = 0
	}
	ns.BlockBroupDB[blockGroupID] = blockGroup

}
func (ns *nameSpace) GetAllKeyByFullPath(in string) (keys []string) {

	tmp := strings.Split(in, "/")
	//fmt.Println(tmp)
	keys = make([]string, 1)

	for i, v := range tmp {
		if i == 0 {
			keys[i] = "0"
		} else {
			if ok, pInodeInfo := ns.Get(keys[i-1]); ok {
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

func (ns *nameSpace) AllocateInodeID() int64 {
	return ns.BaseInodeID.Id()
}

func (ns *nameSpace) AllocateChunkID() int64 {
	return ns.BaseChunkID.Id()
}

func (ns *nameSpace) Get(k string) (bool, *mp.InodeInfo) {
	ns.Mutex.RLock()
	defer ns.Mutex.RUnlock()
	if v, ok := ns.InodeDB[k]; ok {
		return true, v
	} else {
		return false, nil
	}
}

func (ns *nameSpace) Set(k string, v *mp.InodeInfo) {
	ns.Mutex.Lock()
	defer ns.Mutex.Unlock()
	ns.InodeDB[k] = v
}

func (ns *nameSpace) Delete(k string) {
	ns.Mutex.Lock()
	defer ns.Mutex.Unlock()
	delete(ns.InodeDB, k)
}
