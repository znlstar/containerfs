package raftopt

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sync"

	pbproto "github.com/golang/protobuf/proto"
	kvp "github.com/ipdcode/containerfs/proto/kvp"
	"github.com/ipdcode/raft"
	"github.com/ipdcode/raft/proto"
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
)

//KvStateMachine ...
type KvStateMachine struct {
	id      uint64
	applied uint64
	raft    *raft.RaftServer

	DentryLocker sync.RWMutex
	dentryData   map[string][]byte

	inodeLocker sync.RWMutex
	inodeData   map[string][]byte

	BlockGroupLocker sync.RWMutex
	blockGroupData   map[string][]byte

	chunkIDLocker sync.Mutex
	chunkID       uint64

	inodeIDLocker sync.Mutex
	inodeID       uint64
}

func newKvStatemachine(id uint64, raft *raft.RaftServer) *KvStateMachine {
	return &KvStateMachine{
		id:             id,
		raft:           raft,
		dentryData:     make(map[string][]byte),
		inodeData:      make(map[string][]byte),
		blockGroupData: make(map[string][]byte),
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
		ms.DentryLocker.Lock()
		ms.dentryData[kv.K] = kv.V
		ms.DentryLocker.Unlock()
	case OPT_DEL_DENTRY: // del dentryData
		ms.DentryLocker.Lock()
		delete(ms.dentryData, kv.K)
		ms.DentryLocker.Unlock()
	case OPT_SET_INODE: // set inodeData
		ms.inodeLocker.Lock()
		ms.inodeData[kv.K] = kv.V
		ms.inodeLocker.Unlock()
	case OPT_DEL_INODE: // del inodeData
		ms.inodeLocker.Lock()
		delete(ms.inodeData, kv.K)
		ms.inodeLocker.Unlock()
	case OPT_SET_BG: // set OPT_SET_BG
		ms.BlockGroupLocker.Lock()
		ms.blockGroupData[kv.K] = kv.V
		ms.BlockGroupLocker.Unlock()

	}

	ms.applied = index
	return nil, nil
}

//ApplyMemberChange ...
func (ms *KvStateMachine) ApplyMemberChange(confChange *proto.ConfChange, index uint64) (interface{}, error) {
	return nil, nil
}

//Snapshot ...
func (ms *KvStateMachine) Snapshot() (proto.Snapshot, error) {

	var dentrydata []byte
	var inodedata []byte
	var bgdata []byte
	var bigdata []byte

	var err error

	ms.DentryLocker.RLock()
	if dentrydata, err = json.Marshal(ms.dentryData); err != nil {
		ms.DentryLocker.RUnlock()
		return nil, err
	}
	ms.DentryLocker.RUnlock()

	ms.inodeLocker.RLock()
	if inodedata, err = json.Marshal(ms.inodeData); err != nil {
		ms.inodeLocker.RUnlock()
		return nil, err
	}
	ms.inodeLocker.RUnlock()

	ms.BlockGroupLocker.RLock()
	if bgdata, err = json.Marshal(ms.blockGroupData); err != nil {
		ms.BlockGroupLocker.RUnlock()
		return nil, err
	}
	ms.BlockGroupLocker.RUnlock()

	a := make([]byte, 8)
	binary.BigEndian.PutUint64(a, uint64(len(dentrydata)))
	bigdata = append(make([]byte, 8), a...)
	bigdata = append(bigdata, dentrydata...)

	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, uint64(len(inodedata)))
	bigdata = append(bigdata, b...)
	bigdata = append(bigdata, inodedata...)

	c := make([]byte, 8)
	binary.BigEndian.PutUint64(c, uint64(len(bgdata)))
	bigdata = append(bigdata, c...)
	bigdata = append(bigdata, bgdata...)

	binary.BigEndian.PutUint64(bigdata, ms.applied)

	d := make([]byte, 8)
	binary.BigEndian.PutUint64(d, ms.chunkID)
	bigdata = append(bigdata, d...)

	e := make([]byte, 8)
	binary.BigEndian.PutUint64(e, ms.inodeID)
	bigdata = append(bigdata, e...)

	return &kvSnapshot{
		applied: ms.applied,
		data:    bigdata,
	}, nil

}

//ApplySnapshot ...
func (ms *KvStateMachine) ApplySnapshot(peers []proto.Peer, iter proto.SnapIterator) error {

	var (
		bigdata []byte
		block   []byte
		err     error
	)
	for err == nil {
		if block, err = iter.Next(); len(block) > 0 {
			bigdata = append(bigdata, block...)
		}
	}
	if err != nil && err != io.EOF {
		return err
	}

	ms.applied = binary.BigEndian.Uint64(bigdata)

	ms.DentryLocker.Lock()
	dentryLen := binary.BigEndian.Uint64(bigdata[8:16])
	if err = json.Unmarshal(bigdata[16:16+dentryLen], &ms.dentryData); err != nil {
		ms.DentryLocker.Unlock()
		return err
	}
	ms.DentryLocker.Unlock()

	ms.inodeLocker.Lock()
	inodeLen := binary.BigEndian.Uint64(bigdata[16+dentryLen : 16+dentryLen+8])
	if err = json.Unmarshal(bigdata[16+dentryLen+8:16+dentryLen+8+inodeLen], &ms.inodeData); err != nil {
		ms.inodeLocker.Unlock()
		return err
	}
	ms.inodeLocker.Unlock()

	ms.BlockGroupLocker.Lock()
	bgLen := binary.BigEndian.Uint64(bigdata[16+dentryLen+8+inodeLen : 16+dentryLen+8+inodeLen+8])
	if err = json.Unmarshal(bigdata[16+dentryLen+8+inodeLen+8:16+dentryLen+8+inodeLen+8+bgLen], &ms.blockGroupData); err != nil {
		ms.BlockGroupLocker.Unlock()
		return err
	}
	ms.BlockGroupLocker.Unlock()

	ms.chunkID = binary.BigEndian.Uint64(bigdata[16+dentryLen+8+inodeLen+8+bgLen : 16+dentryLen+8+inodeLen+8+bgLen+8])
	ms.inodeID = binary.BigEndian.Uint64(bigdata[16+dentryLen+8+inodeLen+8+bgLen+8:])

	return nil
}

//HandleFatalEvent ...
func (ms *KvStateMachine) HandleFatalEvent(err *raft.FatalError) {
	panic(err.Err)
}

//DentryGet ...
func (ms *KvStateMachine) DentryGet(raftGroupID uint64, key string) ([]byte, error) {
	if !ms.raft.IsLeader(raftGroupID) {
		return nil, errors.New("not leader")
	}

	ms.DentryLocker.RLock()
	if v, ok := ms.dentryData[key]; ok {
		ms.DentryLocker.RUnlock()
		return v, nil
	}
	ms.DentryLocker.RUnlock()
	return []byte{}, errNotExists

}

//DentryGetAll ...
func (ms *KvStateMachine) DentryGetAll(raftGroupID uint64) (*map[string][]byte, error) {
	if !ms.raft.IsLeader(raftGroupID) {
		return nil, errors.New("not leader")
	}
	return &ms.dentryData, nil
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
func (ms *KvStateMachine) InodeGet(raftGroupID uint64, key string) ([]byte, error) {
	if !ms.raft.IsLeader(raftGroupID) {
		return nil, errors.New("not leader")
	}
	ms.inodeLocker.RLock()
	if v, ok := ms.inodeData[key]; ok {
		ms.inodeLocker.RUnlock()
		return v, nil
	}
	ms.inodeLocker.RUnlock()
	return []byte{}, errNotExists

}

//InodeSet ...
func (ms *KvStateMachine) InodeSet(raftGroupID uint64, key string, value []byte) error {
	if !ms.raft.IsLeader(raftGroupID) {
		return errors.New("not leader")
	}

	var data []byte
	var err error

	kv := &kvp.Kv{Opt: OPT_SET_INODE, K: key, V: value}

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
func (ms *KvStateMachine) InodeDel(raftGroupID uint64, key string) error {
	if !ms.raft.IsLeader(raftGroupID) {
		return errors.New("not leader")
	}

	var data []byte
	var err error

	kv := &kvp.Kv{Opt: OPT_DEL_INODE, K: key}

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
func (ms *KvStateMachine) BGGet(raftGroupID uint64, key string) ([]byte, error) {
	if !ms.raft.IsLeader(raftGroupID) {
		return nil, errors.New("not leader")
	}

	ms.BlockGroupLocker.RLock()
	if v, ok := ms.blockGroupData[key]; ok {
		ms.BlockGroupLocker.RUnlock()
		return v, nil
	}
	ms.BlockGroupLocker.RUnlock()
	return []byte{}, errNotExists

}

//BGSet ...
func (ms *KvStateMachine) BGSet(raftGroupID uint64, key string, value []byte) error {
	if !ms.raft.IsLeader(raftGroupID) {
		return errors.New("not leader")
	}

	var data []byte
	var err error

	kv := &kvp.Kv{Opt: OPT_SET_BG, K: key, V: value}

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
func (ms *KvStateMachine) BGGetAll(raftGroupID uint64) (*map[string][]byte, error) {
	if !ms.raft.IsLeader(raftGroupID) {
		return nil, errors.New("not leader")
	}
	return &ms.blockGroupData, nil
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

func (ms *KvStateMachine) setApplied(index uint64) {
	ms.applied = index
}

//HandleLeaderChange ...
func (ms *KvStateMachine) HandleLeaderChange(leader uint64) {
}

type kvSnapshot struct {
	offset  int
	applied uint64
	data    []byte
}

//Next ...
func (s *kvSnapshot) Next() ([]byte, error) {
	if s.offset >= len(s.data) {
		return nil, io.EOF
	}
	s.offset = len(s.data)
	return s.data, nil
}

//ApplyIndex ...
func (s *kvSnapshot) ApplyIndex() uint64 {
	return s.applied
}

//Close ...
func (s *kvSnapshot) Close() {
	return
}
