package raftopt

import (
	"errors"
	"fmt"
	pbproto "github.com/golang/protobuf/proto"
	"github.com/tigcode/containerfs/logger"
	"github.com/tigcode/containerfs/metanode/raftopt/BTree"
	"github.com/tigcode/containerfs/proto/kvp"
	"github.com/tigcode/raft"
	"github.com/tigcode/raft/proto"
	"io"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
)

var errNotExists = errors.New("Key not exists")

const (
	// OPT_ALLOCATE_INODEID ...
	OPT_ALLOCATE_INODEID = 1
	// OPT_ALLOCATE_CHUNKID ...
	OPT_ALLOCATE_CHUNKID = 2
	// OPT_SET_DENTRY ...
	OPT_SET_DENTRY = 3
	// OPT_DEL_DENTRY ...
	OPT_DEL_DENTRY = 4
	// OPT_SET_INODE ...
	OPT_SET_INODE = 5
	// OPT_DEL_INODE ...
	OPT_DEL_INODE = 6
	// OPT_SET_BG ...
	OPT_SET_BG = 7
	// OPT_DEL_BG ...
	OPT_DEL_BG = 8

	// for Cluster
	OPT_ALLOCATE_RGID    = 9
	OPT_ALLOCATE_BLOCKID = 10
	OPT_ALLOCATE_BGID    = 11
	OPT_SET_DATANODE     = 12
	OPT_DEL_DATANODE     = 13
	OPT_SET_BLOCK        = 14
	OPT_DEL_BLOCK        = 15
	OPT_SET_BGP          = 16
	OPT_DEL_BGP          = 17
	OPT_SET_VOL          = 18
	OPT_DEL_VOL          = 19

	// OPT_APPLIED ...
	OPT_APPLIED = 20
)

//KvStateMachine ...
type KvStateMachine struct {
	id      uint64
	applied uint64
	raft    *raft.RaftServer

	dentryItem btree.DentryKV
	inodeItem  btree.InodeKV
	bgItem     btree.BGKV

	dentryData     *btree.BTree
	inodeData      *btree.BTree
	blockGroupData *btree.BTree

	chunkIDLocker sync.Mutex
	chunkID       uint64

	inodeIDLocker sync.Mutex
	inodeID       uint64

	rgIDLocker sync.Mutex
	rgID       uint64

	blockIDLocker sync.Mutex
	blockID       uint64

	bgIDLocker sync.Mutex
	bgID       uint64

	dataNodeItem btree.DataNodeKV
	dataNodeData *btree.BTree

	blockItem btree.BlockKV
	blockData *btree.BTree

	bgpItem btree.BGPKV
	bgpData *btree.BTree

	volItem btree.VOLKV
	volData *btree.BTree
}

func newKvStatemachine(id uint64, raft *raft.RaftServer) *KvStateMachine {
	return &KvStateMachine{
		id:             id,
		raft:           raft,
		dentryData:     btree.New(32),
		inodeData:      btree.New(32),
		blockGroupData: btree.New(8),
		dataNodeData:   btree.New(8),
		blockData:      btree.New(8),
		bgpData:        btree.New(8),
		volData:        btree.New(8),
		rgID:           1,
	}
}

//Apply ...
func (ms *KvStateMachine) Apply(data []byte, index uint64) (interface{}, error) {

	kv := &kvp.Kv{}
	err := pbproto.Unmarshal(data, kv)
	if err != nil {
		return nil, err
	}

	switch kv.Opt {
	case OPT_ALLOCATE_INODEID: // allockInodeID
		atomic.AddUint64(&ms.inodeID, 1)
	case OPT_ALLOCATE_CHUNKID: // allockChunkID
		atomic.AddUint64(&ms.chunkID, 1)
	case OPT_SET_DENTRY: // set dentryData
		ms.dentryItem.K = kv.K
		ms.dentryItem.V = kv.V
		ms.dentryData.ReplaceOrInsert(ms.dentryItem)
	case OPT_DEL_DENTRY: // del dentryData
		ms.dentryItem.K = kv.K
		ms.dentryData.Delete(ms.dentryItem)
	case OPT_SET_INODE: // set inodeData
		key, _ := strconv.ParseUint(kv.K, 10, 64)
		ms.inodeItem.K = key
		ms.inodeItem.V = kv.V
		ms.inodeData.ReplaceOrInsert(ms.inodeItem)
	case OPT_DEL_INODE: // del inodeData
		key, _ := strconv.ParseUint(kv.K, 10, 64)
		ms.inodeItem.K = key
		ms.inodeData.Delete(ms.inodeItem)
	case OPT_SET_BG: // set OPT_SET_BG
		key, _ := strconv.ParseUint(kv.K, 10, 64)
		ms.bgItem.K = key
		ms.bgItem.V = kv.V
		ms.blockGroupData.ReplaceOrInsert(ms.bgItem)

	case OPT_ALLOCATE_RGID:
		atomic.AddUint64(&ms.rgID, 1)
	case OPT_ALLOCATE_BLOCKID: // allockChunkID
		atomic.AddUint64(&ms.blockID, 1)
	case OPT_ALLOCATE_BGID: // allockChunkID
		atomic.AddUint64(&ms.bgID, 1)
	case OPT_SET_DATANODE: // set OPT_SET_BLOCK
		ms.dataNodeItem.K = kv.K
		ms.dataNodeItem.V = kv.V
		ms.dataNodeData.ReplaceOrInsert(ms.dataNodeItem)
	case OPT_DEL_DATANODE: // del OPT_DEL_BLOCK
		ms.dataNodeItem.K = kv.K
		ms.dataNodeData.Delete(ms.dataNodeItem)

	case OPT_SET_BLOCK: // set OPT_SET_BLOCK
		ms.blockItem.K = kv.K
		ms.blockItem.V = kv.V
		ms.blockData.ReplaceOrInsert(ms.blockItem)
	case OPT_DEL_BLOCK: // del OPT_DEL_BLOCK
		ms.blockItem.K = kv.K
		ms.blockData.Delete(ms.blockItem)

	case OPT_SET_BGP: // set OPT_SET_BGP
		ms.bgpItem.K = kv.K
		ms.bgpItem.V = kv.V
		ms.bgpData.ReplaceOrInsert(ms.bgpItem)
	case OPT_DEL_BGP: // del OPT_DEL_BGP
		ms.bgpItem.K = kv.K
		ms.bgpData.Delete(ms.bgpItem)

	case OPT_SET_VOL: // set OPT_SET_VOL
		ms.volItem.K = kv.K
		ms.volItem.V = kv.V
		ms.volData.ReplaceOrInsert(ms.volItem)
	case OPT_DEL_VOL: // del OPT_DEL_VOL
		ms.volItem.K = kv.K
		ms.volData.Delete(ms.volItem)
	}

	ms.applied = index
	return nil, nil
}

//DentryGet ...
func (ms *KvStateMachine) DentryGet(raftGroupID uint64, key string) ([]byte, error) {
	if !ms.raft.IsLeader(raftGroupID) {
		return nil, errors.New("not leader")
	}

	var item btree.DentryKV
	item.K = key
	newItem := ms.dentryData.Get(item)

	if newItem != nil {
		return newItem.(btree.DentryKV).V, nil
	}
	return []byte{}, errNotExists

}

/*
//DentryGetAll ...
func (ms *KvStateMachine) DentryGetAll(raftGroupID uint64) (*map[string][]byte, error) {
	if !ms.raft.IsLeader(raftGroupID) {
		return nil, errors.New("not leader")
	}
	return &ms.dentryData, nil
}
*/

func (ms *KvStateMachine) DentryGetRange(raftGroupID uint64, minKey string, maxKey string) ([]btree.DentryKV, error) {
	if !ms.raft.IsLeader(raftGroupID) {
		return nil, errors.New("not leader")
	}

	var v []btree.DentryKV

	var itemMin btree.DentryKV
	itemMin.K = minKey

	var itemMax btree.DentryKV
	itemMax.K = maxKey

	ms.dentryData.AscendRange(itemMin, itemMax, func(a btree.Item) bool {
		v = append(v, a.(btree.DentryKV))
		return true
	})

	return v, nil
}

//DentrySet ...
func (ms *KvStateMachine) DentrySet(raftGroupID uint64, key string, value []byte) error {
	if !ms.raft.IsLeader(raftGroupID) {
		return errors.New("not leader")
	}
	var data []byte
	var err error

	kv := &kvp.Kv{Opt: OPT_SET_DENTRY, K: key, V: value}

	if data, err = pbproto.Marshal(kv); err != nil {
		return err
	}
	resp := ms.raft.Submit(raftGroupID, data)
	_, err = resp.Response()
	if err != nil {
		return fmt.Errorf("Put error[%v]", err)
	}
	return nil

}

//DentryDel ...
func (ms *KvStateMachine) DentryDel(raftGroupID uint64, key string) error {
	if !ms.raft.IsLeader(raftGroupID) {
		return errors.New("not leader")
	}
	var data []byte
	var err error

	kv := &kvp.Kv{Opt: OPT_DEL_DENTRY, K: key}

	if data, err = pbproto.Marshal(kv); err != nil {
		return err
	}
	resp := ms.raft.Submit(raftGroupID, data)
	_, err = resp.Response()
	if err != nil {
		return fmt.Errorf("Del error[%v]", err)
	}
	return nil

}

//InodeGet ...
func (ms *KvStateMachine) InodeGet(raftGroupID uint64, key uint64) ([]byte, error) {

	if !ms.raft.IsLeader(raftGroupID) {
		return nil, errors.New("not leader")
	}

	var item btree.InodeKV
	item.K = key
	newItem := ms.inodeData.Get(item)

	if newItem != nil {
		return newItem.(btree.InodeKV).V, nil
	}
	return []byte{}, errNotExists

}

//InodeSet ...
func (ms *KvStateMachine) InodeSet(raftGroupID uint64, key uint64, value []byte) error {
	if !ms.raft.IsLeader(raftGroupID) {
		return errors.New("not leader")
	}

	var data []byte
	var err error

	kv := &kvp.Kv{Opt: OPT_SET_INODE, K: strconv.FormatUint(key, 10), V: value}

	if data, err = pbproto.Marshal(kv); err != nil {
		return err
	}
	resp := ms.raft.Submit(raftGroupID, data)
	_, err = resp.Response()
	if err != nil {
		return fmt.Errorf("Put error[%v]", err)
	}
	return nil

}

//InodeDel ...
func (ms *KvStateMachine) InodeDel(raftGroupID uint64, key uint64) error {
	if !ms.raft.IsLeader(raftGroupID) {
		return errors.New("not leader")
	}

	var data []byte
	var err error

	kv := &kvp.Kv{Opt: OPT_DEL_INODE, K: strconv.FormatUint(key, 10)}

	if data, err = pbproto.Marshal(kv); err != nil {
		return err
	}
	resp := ms.raft.Submit(raftGroupID, data)
	_, err = resp.Response()
	if err != nil {
		return fmt.Errorf("Del error[%v]", err)
	}
	return nil

}

//BGGet ...
func (ms *KvStateMachine) BGGet(raftGroupID uint64, key uint64) ([]byte, error) {
	if !ms.raft.IsLeader(raftGroupID) {
		return nil, errors.New("not leader")
	}

	var item btree.BGKV
	item.K = key
	newItem := ms.blockGroupData.Get(item)

	if newItem != nil {
		return newItem.(btree.BGKV).V, nil
	}
	return []byte{}, errNotExists

}

//BGSet ...
func (ms *KvStateMachine) BGSet(raftGroupID uint64, key uint64, value []byte) error {
	if !ms.raft.IsLeader(raftGroupID) {
		return errors.New("not leader")
	}

	var data []byte
	var err error

	kv := &kvp.Kv{Opt: OPT_SET_BG, K: strconv.FormatUint(key, 10), V: value}

	if data, err = pbproto.Marshal(kv); err != nil {
		return err
	}
	resp := ms.raft.Submit(raftGroupID, data)
	_, err = resp.Response()
	if err != nil {
		return fmt.Errorf("Put error[%v]", err)
	}
	return nil

}

//BGGetAll ...
func (ms *KvStateMachine) BGGetAll(raftGroupID uint64) ([]btree.BGKV, error) {
	if !ms.raft.IsLeader(raftGroupID) {
		return nil, errors.New("not leader")
	}

	var v []btree.BGKV

	ms.blockGroupData.Ascend(func(a btree.Item) bool {
		v = append(v, a.(btree.BGKV))
		return true
	})

	return v, nil
}

//ChunkIDGET ...
func (ms *KvStateMachine) ChunkIDGET(raftGroupID uint64) (uint64, error) {
	if !ms.raft.IsLeader(raftGroupID) {
		return 0, errors.New("not leader")
	}

	var data []byte
	var err error

	kv := &kvp.Kv{Opt: OPT_ALLOCATE_CHUNKID}

	if data, err = pbproto.Marshal(kv); err != nil {
		return 0, err
	}

	ms.chunkIDLocker.Lock()
	resp := ms.raft.Submit(raftGroupID, data)
	_, err = resp.Response()
	if err != nil {
		ms.chunkIDLocker.Unlock()
		return 0, fmt.Errorf("Put error[%v]", err)
	}
	chunkID := ms.chunkID
	ms.chunkIDLocker.Unlock()

	return chunkID, nil
}

//InodeIDGET ...
func (ms *KvStateMachine) InodeIDGET(raftGroupID uint64) (uint64, error) {
	if !ms.raft.IsLeader(raftGroupID) {
		return 0, errors.New("not leader")
	}

	var data []byte
	var err error

	kv := &kvp.Kv{Opt: OPT_ALLOCATE_INODEID}

	if data, err = pbproto.Marshal(kv); err != nil {
		return 0, err
	}

	ms.inodeIDLocker.Lock()
	resp := ms.raft.Submit(raftGroupID, data)
	_, err = resp.Response()
	if err != nil {
		ms.inodeIDLocker.Unlock()
		return 0, fmt.Errorf("Put error[%v]", err)
	}
	inodeID := ms.inodeID
	ms.inodeIDLocker.Unlock()

	return inodeID, nil
}

func (ms *KvStateMachine) DataNodeGetAll(raftGroupID uint64) ([]btree.DataNodeKV, error) {
	if !ms.raft.IsLeader(raftGroupID) {
		return nil, errors.New("not leader")
	}
	var v []btree.DataNodeKV

	ms.dataNodeData.Ascend(func(a btree.Item) bool {
		v = append(v, a.(btree.DataNodeKV))
		return true
	})

	return v, nil
}

// cluster ...
func (ms *KvStateMachine) DataNodeGetRange(raftGroupID uint64, minKey string) ([]btree.DataNodeKV, error) {
	if !ms.raft.IsLeader(raftGroupID) {
		return nil, errors.New("not leader")
	}

	var v []btree.DataNodeKV

	var itemMin btree.DataNodeKV
	itemMin.K = minKey

	ms.dataNodeData.AscendGreaterOrEqual(itemMin, func(a btree.Item) bool {
		tmp := strings.Split(a.(btree.DataNodeKV).K, ":")
		if tmp == nil {
			return false
		}
		if len(tmp) < 2 {
			return false
		}
		if tmp[0] != itemMin.K {
			return false
		}
		v = append(v, a.(btree.DataNodeKV))
		return true
	})

	return v, nil
}

func (ms *KvStateMachine) DataNodeGet(raftGroupID uint64, key string) ([]byte, error) {
	var item btree.DataNodeKV
	item.K = key
	newItem := ms.dataNodeData.Get(item)

	if newItem != nil {
		return newItem.(btree.DataNodeKV).V, nil
	}
	return []byte{}, errNotExists

}

func (ms *KvStateMachine) DataNodeSet(raftGroupID uint64, key string, value []byte) error {
	if !ms.raft.IsLeader(raftGroupID) {
		return errors.New("not leader")
	}
	var data []byte
	var err error

	kv := &kvp.Kv{Opt: OPT_SET_DATANODE, K: key, V: value}

	if data, err = pbproto.Marshal(kv); err != nil {
		return err
	}
	resp := ms.raft.Submit(raftGroupID, data)
	_, err = resp.Response()
	if err != nil {
		return fmt.Errorf("Put error[%v]", err)
	}
	return nil

}

//DentryDel ...
func (ms *KvStateMachine) DelDataNode(raftGroupID uint64, key string) error {
	if !ms.raft.IsLeader(raftGroupID) {
		return errors.New("not leader")
	}
	var data []byte
	var err error

	kv := &kvp.Kv{Opt: OPT_DEL_DATANODE, K: key}

	if data, err = pbproto.Marshal(kv); err != nil {
		return err
	}
	resp := ms.raft.Submit(raftGroupID, data)
	_, err = resp.Response()
	if err != nil {
		return fmt.Errorf("Del error[%v]", err)
	}
	return nil

}

func (ms *KvStateMachine) RGIDGET(raftGroupID uint64) (uint64, error) {
	if !ms.raft.IsLeader(raftGroupID) {
		return 0, errors.New("not leader")
	}

	var data []byte
	var err error

	kv := &kvp.Kv{Opt: OPT_ALLOCATE_RGID}

	if data, err = pbproto.Marshal(kv); err != nil {
		return 0, err
	}

	ms.rgIDLocker.Lock()
	resp := ms.raft.Submit(raftGroupID, data)
	_, err = resp.Response()
	if err != nil {
		ms.rgIDLocker.Unlock()
		return 0, fmt.Errorf("Put error[%v]", err)
	}
	rgID := ms.rgID
	ms.rgIDLocker.Unlock()

	return rgID, nil
}

//blockIDGET ...
func (ms *KvStateMachine) BlockIDGET(raftGroupID uint64) (uint64, error) {
	if !ms.raft.IsLeader(raftGroupID) {
		return 0, errors.New("not leader")
	}

	var data []byte
	var err error

	kv := &kvp.Kv{Opt: OPT_ALLOCATE_BLOCKID}

	if data, err = pbproto.Marshal(kv); err != nil {
		return 0, err
	}

	ms.blockIDLocker.Lock()
	resp := ms.raft.Submit(raftGroupID, data)
	_, err = resp.Response()
	if err != nil {
		ms.blockIDLocker.Unlock()
		return 0, fmt.Errorf("Put error[%v]", err)
	}
	blockID := ms.blockID
	ms.blockIDLocker.Unlock()

	return blockID, nil
}

//blockgroupIDGET ...
func (ms *KvStateMachine) BGIDGET(raftGroupID uint64) (uint64, error) {
	if !ms.raft.IsLeader(raftGroupID) {
		return 0, errors.New("not leader")
	}

	var data []byte
	var err error

	kv := &kvp.Kv{Opt: OPT_ALLOCATE_BGID}

	if data, err = pbproto.Marshal(kv); err != nil {
		return 0, err
	}

	ms.bgIDLocker.Lock()
	resp := ms.raft.Submit(raftGroupID, data)
	_, err = resp.Response()
	if err != nil {
		ms.bgIDLocker.Unlock()
		return 0, fmt.Errorf("Put error[%v]", err)
	}
	bgID := ms.bgID
	ms.bgIDLocker.Unlock()

	return bgID, nil
}

func (ms *KvStateMachine) BlockGetRange(raftGroupID uint64, minKey string) ([]btree.BlockKV, error) {
	var v []btree.BlockKV

	var itemMin btree.BlockKV
	itemMin.K = minKey

	ms.blockData.AscendGreaterOrEqual(itemMin, func(a btree.Item) bool {
		tmp := strings.Split(a.(btree.BlockKV).K, "-")
		if tmp == nil {
			return false
		}
		if len(tmp) < 2 {
			return false
		}
		if tmp[0] != itemMin.K {
			return false
		}
		v = append(v, a.(btree.BlockKV))
		return true
	})

	return v, nil
}

func (ms *KvStateMachine) BlockSet(raftGroupID uint64, key string, value []byte) error {
	if !ms.raft.IsLeader(raftGroupID) {
		return errors.New("not leader")
	}
	var data []byte
	var err error

	kv := &kvp.Kv{Opt: OPT_SET_BLOCK, K: key, V: value}

	if data, err = pbproto.Marshal(kv); err != nil {
		return err
	}
	resp := ms.raft.Submit(raftGroupID, data)
	_, err = resp.Response()
	if err != nil {
		return fmt.Errorf("Put error[%v]", err)
	}
	return nil

}

//DentryDel ...
func (ms *KvStateMachine) BlockDel(raftGroupID uint64, key string) error {
	if !ms.raft.IsLeader(raftGroupID) {
		return errors.New("not leader")
	}
	var data []byte
	var err error

	kv := &kvp.Kv{Opt: OPT_DEL_BLOCK, K: key}

	if data, err = pbproto.Marshal(kv); err != nil {
		return err
	}
	resp := ms.raft.Submit(raftGroupID, data)
	_, err = resp.Response()
	if err != nil {
		return fmt.Errorf("Del error[%v]", err)
	}
	return nil

}

//BGGet ...
func (ms *KvStateMachine) BGPGet(raftGroupID uint64, key string) ([]byte, error) {
	var item btree.BGPKV
	item.K = key
	newItem := ms.bgpData.Get(item)

	if newItem != nil {
		return newItem.(btree.BGPKV).V, nil
	}
	return []byte{}, errNotExists

}

func (ms *KvStateMachine) BGPGetRange(raftGroupID uint64, minKey string) ([]btree.BGPKV, error) {
	var v []btree.BGPKV

	var itemMin btree.BGPKV
	itemMin.K = minKey

	ms.bgpData.AscendGreaterOrEqual(itemMin, func(a btree.Item) bool {
		tmp := strings.Split(a.(btree.BGPKV).K, "-")
		if tmp == nil {
			return false
		}
		if len(tmp) < 2 {
			return false
		}
		if tmp[0] != itemMin.K {
			return false
		}
		v = append(v, a.(btree.BGPKV))
		return true
	})

	return v, nil
}

//BGSet ...
func (ms *KvStateMachine) BGPSet(raftGroupID uint64, key string, value []byte) error {
	if !ms.raft.IsLeader(raftGroupID) {
		return errors.New("not leader")
	}

	var data []byte
	var err error

	kv := &kvp.Kv{Opt: OPT_SET_BGP, K: key, V: value}

	if data, err = pbproto.Marshal(kv); err != nil {
		return err
	}
	resp := ms.raft.Submit(raftGroupID, data)
	_, err = resp.Response()
	if err != nil {
		return fmt.Errorf("Put error[%v]", err)
	}
	return nil

}

func (ms *KvStateMachine) BGPDel(raftGroupID uint64, key string) error {
	if !ms.raft.IsLeader(raftGroupID) {
		return errors.New("not leader")
	}
	var data []byte
	var err error

	kv := &kvp.Kv{Opt: OPT_DEL_BGP, K: key}

	if data, err = pbproto.Marshal(kv); err != nil {
		return err
	}
	resp := ms.raft.Submit(raftGroupID, data)
	_, err = resp.Response()
	if err != nil {
		return fmt.Errorf("Del error[%v]", err)
	}
	return nil

}

func (ms *KvStateMachine) VOLGet(raftGroupID uint64, key string) ([]byte, error) {
	var item btree.VOLKV
	item.K = key
	newItem := ms.volData.Get(item)

	if newItem != nil {
		return newItem.(btree.VOLKV).V, nil
	}
	return []byte{}, errNotExists

}

func (ms *KvStateMachine) VolsGetAll(raftGroupID uint64) ([]btree.VOLKV, error) {
	var v []btree.VOLKV

	ms.volData.Ascend(func(a btree.Item) bool {
		v = append(v, a.(btree.VOLKV))
		return true
	})

	return v, nil
}

func (ms *KvStateMachine) VOLSet(raftGroupID uint64, key string, value []byte) error {
	if !ms.raft.IsLeader(raftGroupID) {
		return errors.New("not leader")
	}

	var data []byte
	var err error

	kv := &kvp.Kv{Opt: OPT_SET_VOL, K: key, V: value}

	if data, err = pbproto.Marshal(kv); err != nil {
		return err
	}
	resp := ms.raft.Submit(raftGroupID, data)
	_, err = resp.Response()
	if err != nil {
		return fmt.Errorf("Put error[%v]", err)
	}
	return nil

}

func (ms *KvStateMachine) VOLDel(raftGroupID uint64, key string) error {
	if !ms.raft.IsLeader(raftGroupID) {
		return errors.New("not leader")
	}
	var data []byte
	var err error

	kv := &kvp.Kv{Opt: OPT_DEL_VOL, K: key}

	if data, err = pbproto.Marshal(kv); err != nil {
		return err
	}
	resp := ms.raft.Submit(raftGroupID, data)
	_, err = resp.Response()
	if err != nil {
		return fmt.Errorf("Del error[%v]", err)
	}
	return nil

}

//AddNode ...
func (ms *KvStateMachine) AddNode(peer proto.Peer) error {
	resp := ms.raft.ChangeMember(1, proto.ConfAddNode, peer, nil)
	_, err := resp.Response()
	if err != nil {
		return errors.New("AddNode error")
	}
	return nil
}

//RemoveNode ...
func (ms *KvStateMachine) RemoveNode(peer proto.Peer) error {
	resp := ms.raft.ChangeMember(1, proto.ConfRemoveNode, peer, nil)
	_, err := resp.Response()
	if err != nil {
		return errors.New("RemoveNode error")
	}
	return nil
}

//ApplyMemberChange ...
func (ms *KvStateMachine) ApplyMemberChange(confChange *proto.ConfChange, index uint64) (interface{}, error) {
	return nil, nil
}

//HandleLeaderChange ...
func (ms *KvStateMachine) HandleLeaderChange(leader uint64) {
}

//HandleFatalEvent ...
func (ms *KvStateMachine) HandleFatalEvent(err *raft.FatalError) {
	panic(err.Err)
}

func (ms *KvStateMachine) setApplied(index uint64) {
	ms.applied = index
}

//Snapshot ...
func (ms *KvStateMachine) Snapshot() (proto.Snapshot, error) {

	return &kvSnapshot{
		applied:       ms.applied,
		chunkID:       ms.chunkID,
		inodeID:       ms.inodeID,
		curDentryItem: ms.dentryData.Min(),
		curInodeItem:  ms.inodeData.Min(),
		curBgItem:     ms.blockGroupData.Min(),
		bgTree:        ms.blockGroupData,
		dentryTree:    ms.dentryData,
		inodeTree:     ms.inodeData,
	}, nil

}

//ApplySnapshot ...
func (ms *KvStateMachine) ApplySnapshot(peers []proto.Peer, iter proto.SnapIterator) error {

	var (
		err      error
		data     []byte
		uint64ID uint64
	)

	kv := &kvp.Kv{}

	for err == nil {
		if data, err = iter.Next(); len(data) > 0 {

			err := pbproto.Unmarshal(data, kv)
			if err != nil {
				return err
			}

			logger.Debug("---------- ApplySnapshot : %v", kv)

			switch kv.Opt {
			case OPT_APPLIED:
				uint64ID, _ = strconv.ParseUint(kv.K, 10, 64)
				ms.applied = uint64ID
			case OPT_ALLOCATE_INODEID:
				uint64ID, _ = strconv.ParseUint(kv.K, 10, 64)
				ms.inodeID = uint64ID
			case OPT_ALLOCATE_CHUNKID:
				uint64ID, _ = strconv.ParseUint(kv.K, 10, 64)
				ms.chunkID = uint64ID
			case OPT_SET_DENTRY: // set dentryData
				ms.dentryItem.K = kv.K
				ms.dentryItem.V = kv.V
				ms.dentryData.ReplaceOrInsert(ms.dentryItem)
			case OPT_SET_INODE: // set inodeData
				uint64ID, _ = strconv.ParseUint(kv.K, 10, 64)
				ms.inodeItem.K = uint64ID
				ms.inodeItem.V = kv.V
				ms.inodeData.ReplaceOrInsert(ms.inodeItem)
			case OPT_SET_BG:
				uint64ID, _ = strconv.ParseUint(kv.K, 10, 64)
				ms.bgItem.K = uint64ID
				ms.bgItem.V = kv.V
				ms.blockGroupData.ReplaceOrInsert(ms.bgItem)
			}

		}
	}
	if err != nil && err != io.EOF {
		return err
	}

	return nil
}

type kvSnapshot struct {
	applied     uint64
	appliedFlag bool

	chunkID     uint64
	chunkIDFlag bool

	inodeID     uint64
	inodeIDFlag bool

	curBtree string

	curBgItem     btree.Item
	curInodeItem  btree.Item
	curDentryItem btree.Item

	dentryTree *btree.BTree
	inodeTree  *btree.BTree
	bgTree     *btree.BTree
}

func tmplog(v string) {
	logger.Debug("Next:", v)
}

//Next ...
func (s *kvSnapshot) Next() ([]byte, error) {

	if s.appliedFlag && s.chunkIDFlag && s.inodeIDFlag && s.curBtree == "done" {
		return nil, io.EOF
	}

	var data []byte
	defer tmplog(string(data))

	var err error
	kv := &kvp.Kv{}

	if !s.appliedFlag {

		kv.Opt = OPT_APPLIED
		kv.K = strconv.FormatUint(s.applied, 10)

		if data, err = pbproto.Marshal(kv); err != nil {
			return nil, err
		}

		s.appliedFlag = true
		return data, nil
	}
	if !s.chunkIDFlag {

		kv.Opt = OPT_ALLOCATE_CHUNKID
		kv.K = strconv.FormatUint(s.chunkID, 10)

		if data, err = pbproto.Marshal(kv); err != nil {
			return nil, err
		}

		s.chunkIDFlag = true
		return data, nil
	}
	if !s.inodeIDFlag {

		kv.Opt = OPT_ALLOCATE_INODEID
		kv.K = strconv.FormatUint(s.inodeID, 10)

		if data, err = pbproto.Marshal(kv); err != nil {
			return nil, err
		}

		s.inodeIDFlag = true
		return data, nil
	}
	if s.curBtree == "" {
		s.curBtree = "bg"
	}

	if s.curBtree == "bg" {

		kv.Opt = OPT_SET_BG

		s.bgTree.AscendGreaterOrEqual(s.curBgItem, func(a btree.Item) bool {
			if a.(btree.BGKV).K > s.curBgItem.(btree.BGKV).K {
				kv.K = strconv.Itoa(int(a.(btree.BGKV).K))
				kv.V = a.(btree.BGKV).V
				s.curBgItem = a
				return false
			}
			kv.K = strconv.Itoa(int(a.(btree.BGKV).K))
			kv.V = a.(btree.BGKV).V
			return true
		})

		if s.curBgItem.(btree.BGKV).K == s.bgTree.Max().(btree.BGKV).K {
			s.curBtree = "inode"
		}

		if data, err = pbproto.Marshal(kv); err != nil {
			return nil, err
		}

		return data, err
	}

	if s.curBtree == "inode" {

		kv.Opt = OPT_SET_INODE

		s.inodeTree.AscendGreaterOrEqual(s.curInodeItem, func(a btree.Item) bool {
			if a.(btree.InodeKV).K > s.curInodeItem.(btree.InodeKV).K {
				kv.K = strconv.FormatUint(a.(btree.InodeKV).K, 10)
				kv.V = a.(btree.InodeKV).V
				s.curInodeItem = a
				return false
			}
			kv.K = strconv.FormatUint(a.(btree.InodeKV).K, 10)
			kv.V = a.(btree.InodeKV).V
			return true
		})

		if s.curInodeItem.(btree.InodeKV).K == s.inodeTree.Max().(btree.InodeKV).K {
			s.curBtree = "dentry"
		}

		if data, err = pbproto.Marshal(kv); err != nil {
			return nil, err
		}

		return data, err
	}
	if s.curBtree == "dentry" {
		kv.Opt = OPT_SET_DENTRY

		s.dentryTree.AscendGreaterOrEqual(s.curDentryItem, func(a btree.Item) bool {
			if a.(btree.DentryKV).K > s.curDentryItem.(btree.DentryKV).K {
				kv.K = a.(btree.DentryKV).K
				kv.V = a.(btree.DentryKV).V
				s.curDentryItem = a
				return false
			}
			kv.K = a.(btree.DentryKV).K
			kv.V = a.(btree.DentryKV).V
			return true
		})

		if s.curDentryItem.(btree.DentryKV).K == s.dentryTree.Max().(btree.DentryKV).K {
			s.curBtree = "done"
		}

		if data, err = pbproto.Marshal(kv); err != nil {
			return nil, err
		}

		return data, err

	}
	return nil, io.ErrUnexpectedEOF
}

//ApplyIndex ...
func (s *kvSnapshot) ApplyIndex() uint64 {
	return s.applied
}

//Close ...
func (s *kvSnapshot) Close() {
	return
}
