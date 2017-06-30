package raftopt

import (
	"bufio"
	"encoding/binary"
	"encoding/json"
	log "github.com/ipdcode/containerfs/logger"
	"io/ioutil"
	"jd.com/sharkstore/raft"
	"jd.com/sharkstore/raft/proto"
	"jd.com/sharkstore/raft/storage/wal"
	"os"
	"path"
	"strconv"
	"time"
)

//StartRaftServer ...
func StartRaftServer(rs **raft.RaftServer, r *Resolver, addr *Address, nodeid uint64) error {

	var err error
	//  new raft server
	c := raft.DefaultConfig()
	c.TickInterval = time.Millisecond * 200
	c.NodeID = nodeid
	c.Resolver = r
	c.HeartbeatAddr = addr.Heartbeat
	c.ReplicateAddr = addr.Replicate
	*rs, err = raft.NewRaftServer(c)
	if err != nil {
		log.Error("create raft server failed: %v", err)
		return err
	}
	return nil
}

//CreateKvStateMachine ...
func CreateKvStateMachine(rs *raft.RaftServer, peers []proto.Peer, nodeID uint64, dir string, UUID string, raftGroupID uint64) (*KvStateMachine, *wal.Storage, error) {
	wc := &wal.Config{}
	raftStroage, err := wal.NewStorage(path.Join(dir, UUID, "wal"), wc)
	if err != nil {
		log.Error("new raft log stroage error: %v", err)
		return nil, nil, err
	}

	// state machine
	kvsm := newKvStatemachine(nodeID, rs)

	index, err := LoadKvSnapShoot(kvsm, path.Join(dir, UUID, "wal", "snap"))
	if err != nil {
		return nil, nil, err
	}

	log.Debug("CreateKvStateMachine Success index : %v", index)

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

//TakeKvSnapShoot ...
func TakeKvSnapShoot(kvsm *KvStateMachine, rsg *wal.Storage, path string) {

	var data []byte
	var err error

	if data, err = json.Marshal(kvsm.data); err != nil {
		return
	}
	var appliedStr = strconv.FormatInt(int64(kvsm.applied), 10)
	data = append(make([]byte, 8), data...)
	binary.BigEndian.PutUint64(data, kvsm.applied)
	f, err := os.OpenFile(path+"-"+appliedStr, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0666)
	if err != nil {
		f.Close()
		return
	}
	w := bufio.NewWriter(f)
	w.Write(data)
	w.Flush()
	f.Close()

	_, err = os.Stat(path)
	if err == nil {
		t := time.Now()
		os.Rename(path, path+"-backup"+"-"+t.String())
		os.Rename(path+"-"+appliedStr, path)
	} else {
		os.Rename(path+"-"+appliedStr, path)
	}

	err = rsg.Truncate(kvsm.applied)
	if err != nil {
		log.Error("TakeKvSnapShoot Truncate failed index : %v , err :%v", kvsm.applied, err)
	}
	log.Error("TakeKvSnapShoot Truncate Success index : %v ", kvsm.applied)

	return
}

//LoadKvSnapShoot ...
func LoadKvSnapShoot(kvsm *KvStateMachine, path string) (uint64, error) {
	fi, err := os.Open(path)
	if err != nil {
		return 0, nil
	}
	defer fi.Close()
	data, err := ioutil.ReadAll(fi)

	kvsm.applied = binary.BigEndian.Uint64(data)
	if err = json.Unmarshal(data[8:], &kvsm.data); err != nil {
		return 0, err
	}
	return kvsm.applied, nil
}

//KvGet ...
func KvGet(kvsm *KvStateMachine, raftGroupID uint64, k string) (string, error) {
	return kvsm.Get(raftGroupID, k)
}

//KvGetAll ...
func KvGetAll(kvsm *KvStateMachine, raftGroupID uint64) (map[string]string, error) {
	return kvsm.GetAll(raftGroupID)
}

//KvSet ...
func KvSet(kvsm *KvStateMachine, raftGroupID uint64, k string, v string) error {
	return kvsm.Put(raftGroupID, k, v)
}
