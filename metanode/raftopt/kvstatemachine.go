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
	"reflect"
)

var errNotExists = errors.New("Key not exists")

//KvStateMachine ...
type KvStateMachine struct {
	sync.RWMutex
	id      uint64
	applied uint64
	raft    *raft.RaftServer
	data    map[string][]byte
}

func newKvStatemachine(id uint64, raft *raft.RaftServer) *KvStateMachine {
	return &KvStateMachine{
		id:   id,
		raft: raft,
		data: make(map[string][]byte),
	}
}

//Apply ...
func (ms *KvStateMachine) Apply(data []byte, index uint64) (interface{}, error) {
	ms.Lock()
	defer func() {
		ms.Unlock()
	}()

	kv := &kvp.Kv{}
	err := pbproto.Unmarshal(data, kv)
	if err != nil {
		return nil, err
	}
	if reflect.DeepEqual(kv.V, []byte("!delete!")) {
		delete(ms.data, kv.K)
	} else {
		ms.data[kv.K] = kv.V
	}
	ms.applied = index
	return nil, nil
}

//ApplyMemberChange ...
func (ms *KvStateMachine) ApplyMemberChange(confChange *proto.ConfChange, index uint64) (interface{}, error) {
	ms.Lock()
	defer func() {
		ms.Unlock()
	}()

	return nil, nil
}

//Snapshot ...
func (ms *KvStateMachine) Snapshot() (proto.Snapshot, error) {
	ms.RLock()
	defer ms.RUnlock()

	var data []byte
	var err error
	if data, err = json.Marshal(ms.data); err != nil {
		return nil, err
	}
	data = append(make([]byte, 8), data...)
	binary.BigEndian.PutUint64(data, ms.applied)
	return &kvSnapshot{
		applied: ms.applied,
		data:    data,
	}, nil

}

//ApplySnapshot ...
func (ms *KvStateMachine) ApplySnapshot(peers []proto.Peer, iter proto.SnapIterator) error {
	ms.Lock()
	defer ms.Unlock()

	var (
		data  []byte
		block []byte
		err   error
	)
	for err == nil {
		if block, err = iter.Next(); len(block) > 0 {
			data = append(data, block...)
		}
	}
	if err != nil && err != io.EOF {
		return err
	}

	ms.applied = binary.BigEndian.Uint64(data)
	if err = json.Unmarshal(data[8:], &ms.data); err != nil {
		return err
	}
	return nil
}

//HandleFatalEvent ...
func (ms *KvStateMachine) HandleFatalEvent(err *raft.FatalError) {
	panic(err.Err)
}

//Get ...
func (ms *KvStateMachine) Get(raftGroupID uint64, key string) ([]byte, error) {
	ms.RLock()
	defer ms.RUnlock()

	if v, ok := ms.data[key]; ok {
		return v, nil
	}
	return []byte{}, errNotExists

}

//GetAll ...
func (ms *KvStateMachine) GetAll(raftGroupID uint64) (map[string][]byte, error) {
	return ms.data, nil
}

//Put ...
func (ms *KvStateMachine) Put(raftGroupID uint64, key string, value []byte) error {

	var data []byte
	var err error

	kv := &kvp.Kv{K: key, V: value}

	if data, err = pbproto.Marshal(kv); err != nil {
		return err
	}
	resp := ms.raft.Submit(raftGroupID, data)
	_, err = resp.Response()
	if err != nil {
		return fmt.Errorf("Put error[%v]", err)
		//errors.New(fmt.Sprintf("Put error[%v].\r\n", err))
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

func (ms *KvStateMachine) setApplied(index uint64) {
	ms.Lock()
	defer ms.Unlock()
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
