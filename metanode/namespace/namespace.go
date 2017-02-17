package namespace

import (
	"bytes"
	//"fmt"
	"ipd.org/containerfs/metanode/protobuf"
	mutils "ipd.org/containerfs/metanode/utils"
	"ipd.org/containerfs/utils"
	"strconv"
	"strings"
	"sync"
	"time"
)

type nameSpace struct {
	InodeDB     map[string]*protobuf.InodeInfo
	BaseInodeID *utils.AutoInc
	//BlockBroupDB map[int32]string
	//ChunkDB      map[int64]string
	Mutex sync.RWMutex
}

var AllNameSpace map[string]*nameSpace
var gMutex sync.RWMutex

func CreateGNameSpace() {
	//fmt.Println("CreateGNameSpace ... ")
	gMutex.Lock()
	AllNameSpace = make(map[string]*nameSpace)
	gMutex.Unlock()
}
func CreateNameSpace(UUID string, inodenum int64) bool {
	//fmt.Println("CreateNameSpace ... in final")

	nameSpace := nameSpace{}
	var inodeDB = make(map[string]*protobuf.InodeInfo)
	//var blockGroupDB = make(map[int32]string)
	//var chunkDB = make(map[int64]string)
	nameSpace.InodeDB = inodeDB
	//nameSpace.BlockBroupDB = blockGroupDB
	nameSpace.BaseInodeID = utils.New(inodenum, 1)
	//nameSpace.ChunkDB = chunkDB
	gMutex.Lock()
	AllNameSpace[UUID] = &nameSpace
	gMutex.Unlock()
	return true
}

func GetNameSpace(UUID string) *nameSpace {
	gMutex.RLock()
	defer gMutex.RUnlock()
	return AllNameSpace[UUID]
}

func (ns *nameSpace) CreateDir(path string) int32 {
	var ret int32
	ret = 0
	keys := ns.GetAllKeyByFullPath(path)
	keysNum := len(keys)
	//fmt.Println(keys)
	var pParentInodeInfo *protobuf.InodeInfo
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
	inodeID := ns.AllocateID()
	name := mutils.GetSelfName(path)
	tmpInodeInfo := protobuf.InodeInfo{ParentInodeID: pParentInodeInfo.InodeID, InodeID: inodeID, Name: name, AccessTime: time.Now().Unix(), ModifiTime: time.Now().Unix(), InodeType: false}
	ns.Set(strconv.FormatInt(inodeID, 10), &tmpInodeInfo)
	tmpKey := strconv.FormatInt(pParentInodeInfo.InodeID, 10) + "-" + name
	ns.Set(tmpKey, &tmpInodeInfo)
	/*update patent inode info*/
	pParentInodeInfo.ChildrenInodeIDs = append(pParentInodeInfo.ChildrenInodeIDs, inodeID)
	parentName := mutils.GetParentName(path)
	if parentName == "/" {
		ns.Set("0", pParentInodeInfo)
	} else {
		ns.Set(strconv.FormatInt(pParentInodeInfo.InodeID, 10), pParentInodeInfo)
		tmpKey = strconv.FormatInt(pParentInodeInfo.ParentInodeID, 10) + "-" + mutils.GetParentName(path)
		ns.Set(tmpKey, pParentInodeInfo)
	}
	return ret
}

func (ns *nameSpace) Stat(path string) (*protobuf.InodeInfo, int32) {
	var ret int32
	ret = 0
	var ok bool
	var pInodeInfo *protobuf.InodeInfo

	if path == "/" {
		_, pInodeInfo = ns.Get("0")
		//		ack = protobuf.StatAck{Ret: 0, InodeInfo: pInodeInfo}
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

func (ns *nameSpace) List(path string) ([]*protobuf.InodeInfo, int32) {
	var ret int32
	ret = 0

	var pInodeInfo *protobuf.InodeInfo
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
	var tmpInodeInfos []*protobuf.InodeInfo
	if pInodeInfo.InodeType == true {
		tmpInodeInfos = append(tmpInodeInfos, pInodeInfo)
		return tmpInodeInfos, ret
	}

	var pTmpInodeInfo *protobuf.InodeInfo
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
	var pInodeInfo *protobuf.InodeInfo

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
	var pTmpParentInodeInfo *protobuf.InodeInfo
	_, pTmpParentInodeInfo = ns.Get(keys[keysNum-2])
	for index, value := range pTmpParentInodeInfo.ChildrenInodeIDs {
		if value == pInodeInfo.InodeID {
			//fmt.Println("find the child index:")
			pTmpParentInodeInfo.ChildrenInodeIDs = append(pTmpParentInodeInfo.ChildrenInodeIDs[:index], pTmpParentInodeInfo.ChildrenInodeIDs[index+1:]...)
			break
		}
	}
	ns.Set(keys[keysNum-2], pTmpParentInodeInfo)
	ns.Set(strconv.FormatInt(pTmpParentInodeInfo.ParentInodeID, 10)+"-"+mutils.GetParentName(path), pTmpParentInodeInfo)

	/*delete inode info*/
	ns.Delete(strconv.FormatInt(pInodeInfo.InodeID, 10))
	ns.Delete(strconv.FormatInt(pInodeInfo.ParentInodeID, 10) + "-" + mutils.GetSelfName(path))

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
	var pInodeInfo *protobuf.InodeInfo
	var ok bool
	if ok, pInodeInfo = ns.Get(key1s[key1sNum-1]); !ok {
		ret = 17 /*ENOENT*/
		return ret
	}

	key2s := ns.GetAllKeyByFullPath(path2)
	key2sNum := len(key2s)

	var pParentInodeInfo2 *protobuf.InodeInfo

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
	name := mutils.GetSelfName(path2)
	tmpInodeInfo := protobuf.InodeInfo{ParentInodeID: pParentInodeInfo2.InodeID, InodeID: pInodeInfo.InodeID, Name: name, AccessTime: pInodeInfo.AccessTime, ModifiTime: pInodeInfo.ModifiTime, InodeType: pInodeInfo.InodeType, ChildrenInodeIDs: pInodeInfo.ChildrenInodeIDs}
	tmpKey := strconv.FormatInt(pParentInodeInfo2.InodeID, 10) + "-" + name
	ns.Set(tmpKey, &tmpInodeInfo)

	/*keep inode key */

	/*update patent1 inode info*/
	var pTmpParentInodeInfo1 *protobuf.InodeInfo
	_, pTmpParentInodeInfo1 = ns.Get(key1s[key1sNum-2])
	for index, value := range pTmpParentInodeInfo1.ChildrenInodeIDs {
		if value == pInodeInfo.InodeID {
			//fmt.Println("find the child index:")
			pTmpParentInodeInfo1.ChildrenInodeIDs = append(pTmpParentInodeInfo1.ChildrenInodeIDs[:index], pTmpParentInodeInfo1.ChildrenInodeIDs[index+1:]...)
			break
		}
	}
	ns.Set(key1s[key1sNum-2], pTmpParentInodeInfo1)
	ns.Set(strconv.FormatInt(pTmpParentInodeInfo1.ParentInodeID, 10)+"-"+mutils.GetParentName(path1), pTmpParentInodeInfo1)

	/*update patent2 inode info*/
	pParentInodeInfo2.ChildrenInodeIDs = append(pParentInodeInfo2.ChildrenInodeIDs, pInodeInfo.InodeID)
	parent2Name := mutils.GetParentName(path2)
	if parent2Name == "/" {
		ns.Set("0", pParentInodeInfo2)
	} else {
		ns.Set(strconv.FormatInt(pParentInodeInfo2.InodeID, 10), pParentInodeInfo2)
		tmpKey2 := strconv.FormatInt(pParentInodeInfo2.ParentInodeID, 10) + "-" + mutils.GetParentName(path2)
		ns.Set(tmpKey2, pParentInodeInfo2)
	}

	/*delete old parentID + name key*/
	ns.Delete(strconv.FormatInt(pInodeInfo.ParentInodeID, 10) + "-" + mutils.GetSelfName(path1))

	return ret
}
func (ns *nameSpace) CreateFile(path string) int32 {

	var ret int32
	ret = 0

	keys := ns.GetAllKeyByFullPath(path)
	keysNum := len(keys)

	//fmt.Println(keys)
	var ok bool
	var pParentInodeInfo *protobuf.InodeInfo
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
	inodeID := ns.AllocateID()
	name := mutils.GetSelfName(path)
	tmpInodeInfo := protobuf.InodeInfo{ParentInodeID: pParentInodeInfo.InodeID, InodeID: inodeID, Name: name, AccessTime: time.Now().Unix(), ModifiTime: time.Now().Unix(), InodeType: true}
	ns.Set(strconv.FormatInt(inodeID, 10), &tmpInodeInfo)
	tmpKey := strconv.FormatInt(pParentInodeInfo.InodeID, 10) + "-" + name
	ns.Set(tmpKey, &tmpInodeInfo)

	/*update patent inode info*/
	pParentInodeInfo.ChildrenInodeIDs = append(pParentInodeInfo.ChildrenInodeIDs, inodeID)

	parentName := mutils.GetParentName(path)
	if parentName == "/" {
		ns.Set("0", pParentInodeInfo)
	} else {
		ns.Set(strconv.FormatInt(pParentInodeInfo.InodeID, 10), pParentInodeInfo)
		tmpKey = strconv.FormatInt(pParentInodeInfo.ParentInodeID, 10) + "-" + mutils.GetParentName(path)
		ns.Set(tmpKey, pParentInodeInfo)
	}

	return ret
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

func (ns *nameSpace) AllocateID() int64 {
	return ns.BaseInodeID.Id()
}

func (ns *nameSpace) Get(k string) (bool, *protobuf.InodeInfo) {
	ns.Mutex.RLock()
	defer ns.Mutex.RUnlock()
	if v, ok := ns.InodeDB[k]; ok {
		return true, v
	} else {
		return false, nil
	}
}

func (ns *nameSpace) Set(k string, v *protobuf.InodeInfo) {
	ns.Mutex.Lock()
	defer ns.Mutex.Unlock()
	ns.InodeDB[k] = v
}

func (ns *nameSpace) Delete(k string) {
	ns.Mutex.Lock()
	defer ns.Mutex.Unlock()
	delete(ns.InodeDB, k)
}
