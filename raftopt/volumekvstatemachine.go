package raftopt

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	pbproto "github.com/golang/protobuf/proto"
	"github.com/tiglabs/containerfs/logger"
	log "github.com/tiglabs/containerfs/logger"
	kvp "github.com/tiglabs/containerfs/proto/kvp"
	com "github.com/tiglabs/containerfs/raftopt/common"
	btree "github.com/tiglabs/containerfs/raftopt/common/BTree"
	"github.com/tiglabs/raft"
	"github.com/tiglabs/raft/proto"
	"github.com/tiglabs/raft/storage/wal"
)

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
)

//VolumeKvStateMachine ...
type VolumeKvStateMachine struct {
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

func newVolumeKvStateMachine(id uint64, raft *raft.RaftServer) *VolumeKvStateMachine {
	return &VolumeKvStateMachine{
		id:             id,
		raft:           raft,
		dentryData:     btree.New(32),
		inodeData:      btree.New(32),
		blockGroupData: btree.New(8),
	}
}

///CreateKvStateMachine ...
func CreateVolumeKvStateMachine(rs *raft.RaftServer, peers []proto.Peer, nodeID uint64, dir string, UUID string, raftGroupID uint64) (*VolumeKvStateMachine, *wal.Storage, error) {
	wc := &wal.Config{}
	raftStroage, err := wal.NewStorage(path.Join(dir, UUID, "wal"), wc)
	if err != nil {
		log.Error("new raft log stroage error: %v", err)
		return nil, nil, err
	}

	// state machine
	kvsm := newVolumeKvStateMachine(nodeID, rs)

	var index uint64

	index, err = LoadVolumeKvSnapShot(kvsm, path.Join(dir, UUID, "wal", "snap"))
	if err != nil {
		return nil, nil, err
	}

	log.Debug("CreateVolumeKvStateMachine Success index : %v", index)

	rc := &raft.RaftConfig{
		ID:           raftGroupID,
		Peers:        peers,
		Storage:      raftStroage,
		StateMachine: kvsm,
		Applied:      index,
	}
	err = rs.CreateRaft(rc)
	if err != nil {
		log.Error("creat raft failed. %v", err)
	}

	log.Debug("CreateRaft Success index : %v", rc.Applied)

	return kvsm, raftStroage, nil

}

//Apply ...
func (ms *VolumeKvStateMachine) Apply(data []byte, index uint64) (interface{}, error) {

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
	}

	ms.applied = index
	return nil, nil
}

//DentryGet ...
func (ms *VolumeKvStateMachine) DentryGet(raftGroupID uint64, key string) ([]byte, error) {
	if !ms.raft.IsLeader(raftGroupID) {
		return nil, errors.New("not leader")
	}

	var item btree.DentryKV
	item.K = key
	newItem := ms.dentryData.Get(item)

	if newItem != nil {
		return newItem.(btree.DentryKV).V, nil
	}
	return []byte{}, ErrKeyNotFound

}

func (ms *VolumeKvStateMachine) DentryGetRange(raftGroupID uint64, minKey string, maxKey string) ([]btree.DentryKV, error) {
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
func (ms *VolumeKvStateMachine) DentrySet(raftGroupID uint64, key string, value []byte) error {
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
func (ms *VolumeKvStateMachine) DentryDel(raftGroupID uint64, key string) error {
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
func (ms *VolumeKvStateMachine) InodeGet(raftGroupID uint64, key uint64) ([]byte, error) {

	if !ms.raft.IsLeader(raftGroupID) {
		return nil, errors.New("not leader")
	}

	var item btree.InodeKV
	item.K = key
	newItem := ms.inodeData.Get(item)

	if newItem != nil {
		return newItem.(btree.InodeKV).V, nil
	}
	return []byte{}, ErrKeyNotFound

}

//InodeSet ...
func (ms *VolumeKvStateMachine) InodeSet(raftGroupID uint64, key uint64, value []byte) error {
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
func (ms *VolumeKvStateMachine) InodeDel(raftGroupID uint64, key uint64) error {
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
func (ms *VolumeKvStateMachine) BGGet(raftGroupID uint64, key uint64) ([]byte, error) {
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
func (ms *VolumeKvStateMachine) BGSet(raftGroupID uint64, key uint64, value []byte) error {
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
func (ms *VolumeKvStateMachine) BGGetAll(raftGroupID uint64) ([]btree.BGKV, error) {
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
func (ms *VolumeKvStateMachine) ChunkIDGET(raftGroupID uint64) (uint64, error) {
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
func (ms *VolumeKvStateMachine) InodeIDGET(raftGroupID uint64) (uint64, error) {
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
func (ms *VolumeKvStateMachine) AddNode(peer proto.Peer) error {
	resp := ms.raft.ChangeMember(1, proto.ConfAddNode, peer, nil)
	_, err := resp.Response()
	if err != nil {
		return errors.New("AddNode error")
	}
	return nil
}

//RemoveNode ...
func (ms *VolumeKvStateMachine) RemoveNode(peer proto.Peer) error {
	resp := ms.raft.ChangeMember(1, proto.ConfRemoveNode, peer, nil)
	_, err := resp.Response()
	if err != nil {
		return errors.New("RemoveNode error")
	}
	return nil
}

//ApplyMemberChange ...
func (ms *VolumeKvStateMachine) ApplyMemberChange(confChange *proto.ConfChange, index uint64) (interface{}, error) {
	return nil, nil
}

//HandleLeaderChange ...
func (ms *VolumeKvStateMachine) HandleLeaderChange(leader uint64) {
}

//HandleFatalEvent ...
func (ms *VolumeKvStateMachine) HandleFatalEvent(err *raft.FatalError) {
	panic(err.Err)
}

func (ms *VolumeKvStateMachine) setApplied(index uint64) {
	ms.applied = index
}

//Snapshot ...
func (ms *VolumeKvStateMachine) Snapshot() (proto.Snapshot, error) {

	return &volumeKvSnapshot{
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
func (ms *VolumeKvStateMachine) ApplySnapshot(peers []proto.Peer, iter proto.SnapIterator) error {

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

type volumeKvSnapshot struct {
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

//Next ...
func (s *volumeKvSnapshot) Next() ([]byte, error) {

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
func (s *volumeKvSnapshot) ApplyIndex() uint64 {
	return s.applied
}

//Close ...
func (s *volumeKvSnapshot) Close() {
	return
}

////////////////////////////////////////////////////////////////////////////

//AddrDatabase ...
var VolumeAddrDatabase = make(map[uint64]*com.Address)

//Resolver ...
type VolumeResolver struct {
	nodes map[uint64]struct{}
	sync.Mutex
}

//NewResolver ...
func NewVolumeResolver() *VolumeResolver {
	return &VolumeResolver{nodes: make(map[uint64]struct{})}
}

//AddNode ...
func (r *VolumeResolver) AddNode(nodeID uint64, addr *com.Address) {
	r.Lock()
	r.nodes[nodeID] = struct{}{}
	VolumeAddrDatabase[nodeID] = addr
	r.Unlock()
}

//RemoveNode ...
func (r *VolumeResolver) RemoveNode(nodeID uint64, addr *com.Address) {
	r.Lock()
	delete(r.nodes, nodeID)
	delete(VolumeAddrDatabase, nodeID)
	r.Unlock()
}

//AllNodes ...
func (r *VolumeResolver) AllNodes() (all []uint64) {
	r.Lock()
	for k := range r.nodes {
		all = append(all, k)
	}
	r.Unlock()
	return
}

//NodeAddress ...
func (r *VolumeResolver) NodeAddress(nodeID uint64, stype raft.SocketType) (addr string, err error) {
	raddr, ok := VolumeAddrDatabase[nodeID]
	if !ok {
		return "", errors.New("no such node")
	}
	switch stype {
	case raft.HeartBeat:
		return raddr.Heartbeat, nil
	case raft.Replicate:
		return raddr.Replicate, nil
	default:
		return "", errors.New("unknown socket type")
	}
}

//TakeKvSnapShoot ...
func TakeVolumeKvSnapShot(ms *VolumeKvStateMachine, rsg *wal.Storage, path string) error {

	_, err := os.Stat(path)
	if err == nil {
		t := time.Now()
		os.Rename(path, path+"-backup"+"-"+t.String())
	}
	os.MkdirAll(path, 0777)

	f, err := os.OpenFile(path+"/applied", os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		f.Close()
		return err
	}
	w := bufio.NewWriter(f)

	w.Write([]byte(strconv.FormatUint(ms.applied, 10)))
	w.Flush()
	f.Close()

	f, err = os.OpenFile(path+"/chunkid", os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		f.Close()
		return err
	}
	w = bufio.NewWriter(f)

	w.Write([]byte(strconv.FormatUint(ms.chunkID, 10)))
	w.Flush()
	f.Close()

	f, err = os.OpenFile(path+"/inodeid", os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		f.Close()
		return err
	}
	w = bufio.NewWriter(f)

	w.Write([]byte(strconv.FormatUint(ms.inodeID, 10)))
	w.Flush()
	f.Close()

	f, err = os.OpenFile(path+"/dentrydata", os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		f.Close()
		return err
	}
	w = bufio.NewWriter(f)

	var data []byte

	ms.dentryData.Ascend(func(a btree.Item) bool {

		if data, err = json.Marshal(a.(btree.DentryKV)); err != nil {
			return false
		}

		w.Write(data)
		w.Write([]byte("\n"))
		w.Flush()
		return true
	})

	f.Close()

	f, err = os.OpenFile(path+"/inodedata", os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		f.Close()
		return err
	}
	w = bufio.NewWriter(f)

	ms.inodeData.Ascend(func(a btree.Item) bool {

		if data, err = json.Marshal(a.(btree.InodeKV)); err != nil {
			return false
		}

		w.Write(data)
		w.Write([]byte("\n"))
		w.Flush()
		return true
	})

	f.Close()

	f, err = os.OpenFile(path+"/bgdata", os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		f.Close()
		return err
	}
	w = bufio.NewWriter(f)

	ms.blockGroupData.Ascend(func(a btree.Item) bool {

		if data, err = json.Marshal(a.(btree.BGKV)); err != nil {
			return false
		}

		w.Write(data)
		w.WriteByte('\n')
		w.Flush()
		return true
	})

	f.Close()
	err = rsg.Truncate(ms.applied)
	if err != nil {
		log.Error("TakeKvSnapShoot Truncate failed index : %v , err :%v", ms.applied, err)
		return err
	}
	log.Error("TakeKvSnapShoot Truncate Success index : %v ", ms.applied)

	return nil
}

//LoadKvSnapShoot ...
func LoadVolumeKvSnapShot(ms *VolumeKvStateMachine, path string) (uint64, error) {
	fi, err := os.Open(path + "/applied")
	if err != nil {
		return 0, nil
	}
	data, err := ioutil.ReadAll(fi)
	ms.applied, err = strconv.ParseUint(string(data), 10, 64)
	log.Debug("ms.applied %v", ms.applied)
	fi.Close()

	fi, err = os.Open(path + "/chunkid")
	if err != nil {
		return 0, nil
	}
	data, err = ioutil.ReadAll(fi)
	ms.chunkID, err = strconv.ParseUint(string(data), 10, 64)
	log.Debug("ms.chunkID %v", ms.chunkID)
	fi.Close()

	fi, err = os.Open(path + "/inodeid")
	if err != nil {
		return 0, nil
	}
	data, err = ioutil.ReadAll(fi)
	ms.inodeID, err = strconv.ParseUint(string(data), 10, 64)
	log.Debug("ms.inodeID %v", ms.inodeID)
	fi.Close()

	fi, err = os.Open(path + "/dentrydata")
	if err != nil {
		return 0, err
	}
	buf := bufio.NewReader(fi)
	var dentry btree.DentryKV

	for {
		line, err := buf.ReadBytes('\n')
		if err != nil {
			if err == io.EOF {
				break
			}
			fi.Close()
			return 0, err
		}
		err = json.Unmarshal(line, &dentry)
		if err != nil {
			fi.Close()
			return 0, err
		}
		log.Debug("dentry %v", dentry)

		ms.dentryData.ReplaceOrInsert(dentry)
	}
	fi.Close()

	fi, err = os.Open(path + "/inodedata")
	if err != nil {
		return 0, err
	}
	buf = bufio.NewReader(fi)
	var inode btree.InodeKV

	for {
		line, err := buf.ReadBytes('\n')
		if err != nil {
			if err == io.EOF {
				break
			}
			fi.Close()
			return 0, err
		}
		err = json.Unmarshal(line, &inode)
		if err != nil {
			fi.Close()
			return 0, err
		}
		log.Debug("inode %v", inode)

		ms.inodeData.ReplaceOrInsert(inode)
	}
	fi.Close()

	fi, err = os.Open(path + "/bgdata")
	if err != nil {
		return 0, err
	}
	buf = bufio.NewReader(fi)
	var bg btree.BGKV

	for {
		line, err := buf.ReadBytes('\n')
		if err != nil {
			if err == io.EOF {
				break
			}
			fi.Close()
			return 0, err
		}
		err = json.Unmarshal(line, &bg)
		if err != nil {
			fi.Close()
			return 0, err
		}
		log.Debug("bg %v", bg)

		ms.blockGroupData.ReplaceOrInsert(bg)
	}
	fi.Close()

	log.Debug("ms.applied end %v", ms.applied)

	return ms.applied, nil

}
