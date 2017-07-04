package raftopt

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sync"

	"github.com/ipdcode/raft"
	"github.com/ipdcode/raft/proto"
)

var errNotExists = errors.New("Key not exists")

//KvStateMachine ...
type KvStateMachine struct {
	sync.RWMutex
	id      uint64
	applied uint64
	raft    *raft.RaftServer
	data    map[string]string
}

func newKvStatemachine(id uint64, raft *raft.RaftServer) *KvStateMachine {
	return &KvStateMachine{
		id:   id,
		raft: raft,
		data: make(map[string]string),
	}
}

//Apply ...
func (ms *KvStateMachine) Apply(data []byte, index uint64) (interface{}, error) {
	ms.Lock()
	defer func() {
		ms.Unlock()
	}()

	var kv map[string]string
	if err := json.Unmarshal(data, &kv); err != nil {
		return nil, err
	}
	for k, v := range kv {
		if v == "!delete!" {
			delete(ms.data, k)
		} else {
			ms.data[k] = v
		}
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
func (ms *KvStateMachine) Get(raftGroupID uint64, key string) (string, error) {
	ms.RLock()
	defer ms.RUnlock()

	if v, ok := ms.data[key]; ok {
		return v, nil
	}
	return "", errNotExists

}

//GetAll ...
func (ms *KvStateMachine) GetAll(raftGroupID uint64) (map[string]string, error) {
	return ms.data, nil
}

//Put ...
func (ms *KvStateMachine) Put(raftGroupID uint64, key, value string) error {

	var data []byte
	var err error

	kv := map[string]string{key: value}
	if data, err = json.Marshal(kv); err != nil {
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
