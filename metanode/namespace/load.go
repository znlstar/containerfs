package namespace

import (
	"encoding/json"
	"fmt"
	mp "github.com/ipdcode/containerfs/proto/mp"
	vp "github.com/ipdcode/containerfs/proto/vp"
	"github.com/ipdcode/containerfs/utils"
	"strconv"
	"time"
)

func LoadVolMeta(volID string) {
	ret, nameSpace := GetNameSpace(volID)
	if ret != 0 {
		return
	}

	key := nameSpace.MakeEtcdWatchPreKey("ChunkBaseID", volID)
	key += "ChunkBaseIDKey"
	time.Sleep(time.Millisecond * 100)
	resp, err := EtcdClient.Get(key, false)
	if err != nil {
		fmt.Printf("err:%v\n", err)
	}

	baseChunkID, _ := strconv.ParseInt(string(resp.Kvs[0].Value), 10, 64)
	nameSpace.BaseChunkID = utils.New(baseChunkID+1, 1)
	//nameSpace.CreateChunkBaseIDEtcdWatcher(resp.Header.Revision, nameSpace.VolID)

	key = nameSpace.MakeEtcdWatchPreKey("InodeBaseID", volID)
	key += "InodeBaseIDKey"
	resp, err = EtcdClient.Get(key, false)
	if err != nil {
		fmt.Printf("err:%v\n", err)
	}
	baseInodeID, _ := strconv.ParseInt(string(resp.Kvs[0].Value), 10, 64)
	nameSpace.BaseInodeID = utils.New(baseInodeID+1, 1)
	//nameSpace.CreateInodeBaseIDEtcdWatcher(resp.Header.Revision, nameSpace.VolID)

	preKey := nameSpace.MakeEtcdWatchPreKey("ChunkDB", volID)
	resp, err = EtcdClient.Get(preKey, true)
	if err != nil {
		fmt.Printf("err:%v\n", err)
	}

	for _, ev := range resp.Kvs {
		fmt.Printf("%s : %s\n", ev.Key, ev.Value)
		chunkKey := ev.Key[len(preKey):]
		chunkInfo := mp.ChunkInfo{}

		err := json.Unmarshal([]byte(ev.Value), &chunkInfo)
		if err != nil {
			fmt.Println("Unmarshal faild")
		}

		fmt.Println("createInodeDBEtcdWatcher: chunkInfo")
		fmt.Println(chunkInfo)
		chunkID, _ := strconv.ParseInt(string(chunkKey), 10, 64)
		nameSpace.ChunkDBSet(chunkID, &chunkInfo)
	}
	//nameSpace.CreateChunkDBEtcdWatcher(resp.Header.Revision, nameSpace.VolID)

	preKey = nameSpace.MakeEtcdWatchPreKey("BGDB", volID)
	resp, err = EtcdClient.Get(preKey, true)
	if err != nil {
		fmt.Printf("err:%v\n", err)
	}
	for _, ev := range resp.Kvs {
		fmt.Printf("%s : %s\n", ev.Key, ev.Value)
		bgKey := ev.Key[len(preKey):]
		bgID, _ := strconv.Atoi(string(bgKey))

		bgInfo := vp.BlockGroup{}
		err = json.Unmarshal([]byte(ev.Value), &bgInfo)
		if err != nil {
			fmt.Println("Unmarshal faild")
		}
		nameSpace.BlockGroupDBSet(int32(bgID), &bgInfo)
	}
	//nameSpace.CreateBGDBEtcdWatcher(resp.Header.Revision, nameSpace.VolID)

	preKey = nameSpace.MakeEtcdWatchPreKey("InodeDB", volID)
	resp, err = EtcdClient.Get(preKey, true)
	if err != nil {
		fmt.Printf("err:%v\n", err)
	}

	for _, ev := range resp.Kvs {
		fmt.Printf("%s : %s\n", ev.Key, ev.Value)
		inodeKey := ev.Key[len(preKey):]
		inodeID, _ := strconv.Atoi(string(inodeKey))

		inodeInfo := mp.InodeInfo{}
		err = json.Unmarshal([]byte(ev.Value), &inodeInfo)
		if err != nil {
			fmt.Println("Unmarshal faild")
		}
		nameSpace.InodeDBSet(strconv.FormatInt(int64(inodeID), 10), &inodeInfo)
		nameSpace.InodeDBSet(strconv.FormatInt(inodeInfo.ParentInodeID, 10)+"-"+inodeInfo.Name, &inodeInfo)

	}
	//nameSpace.CreateInodeDBEtcdWatcher(resp.Header.Revision, nameSpace.VolID)
}

func LoadNewVolMeta(volID string) {
	//ret, nameSpace := GetNameSpace(volID)
	//if ret != 0 {
	//	return
	//}

	//nameSpace.CreateChunkBaseIDEtcdWatcher(resp.Header.Revision, nameSpace.VolID)
	//nameSpace.CreateInodeBaseIDEtcdWatcher(resp.Header.Revision, nameSpace.VolID)
	//nameSpace.CreateChunkDBEtcdWatcher(resp.Header.Revision, nameSpace.VolID)
	//nameSpace.CreateBGDBEtcdWatcher(resp.Header.Revision, nameSpace.VolID)
	//nameSpace.CreateInodeDBEtcdWatcher(resp.Header.Revision, nameSpace.VolID)
}
