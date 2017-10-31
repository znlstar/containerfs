package raftopt

import (
	"errors"
	"fmt"
	"github.com/ipdcode/containerfs/logger"
	"io"
	"sync"

	pbproto "github.com/golang/protobuf/proto"
	btree "github.com/ipdcode/containerfs/metanode/raftopt/BTree"
	kvp "github.com/ipdcode/containerfs/proto/kvp"
	"github.com/ipdcode/raft"
	"github.com/ipdcode/raft/proto"
	"strconv"

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
	// OPT_APPLIED ...
	OPT_APPLIED = 9
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
}

func newKvStatemachine(id uint64, raft *raft.RaftServer) *KvStateMachine {
	return &KvStateMachine{
		id:             id,
		raft:           raft,
		dentryData:     btree.New(32),
		inodeData:      btree.New(32),
		blockGroupData: btree.New(32),
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
		key, _ := strconv.Atoi(kv.K)
		ms.bgItem.K = uint32(key)
		ms.bgItem.V = kv.V
		ms.blockGroupData.ReplaceOrInsert(ms.bgItem)
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
func (ms *KvStateMachine) BGGet(raftGroupID uint64, key uint32) ([]byte, error) {
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
func (ms *KvStateMachine) BGSet(raftGroupID uint64, key uint32, value []byte) error {
	if !ms.raft.IsLeader(raftGroupID) {
		return errors.New("not leader")
	}

	var data []byte
	var err error

	kv := &kvp.Kv{Opt: OPT_SET_BG, K: strconv.Itoa(int(key)), V: value}

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
		intID    int
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
				intID, _ = strconv.Atoi(kv.K)
				ms.bgItem.K = uint32(intID)
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
