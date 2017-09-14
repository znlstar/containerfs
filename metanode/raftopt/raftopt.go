package raftopt

import (
	"bufio"
	"encoding/binary"
	"encoding/json"
	log "github.com/ipdcode/containerfs/logger"
	"github.com/ipdcode/raft"
	"github.com/ipdcode/raft/proto"
	"github.com/ipdcode/raft/storage/wal"
	"io/ioutil"
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
func TakeKvSnapShoot(ms *KvStateMachine, rsg *wal.Storage, path string) error {

	var bigdata []byte

	ms.DentryLocker.RLock()
	dentrydata, err := json.Marshal(ms.dentryData)
	if err != nil {
		ms.DentryLocker.RUnlock()
		return err
	}
	ms.DentryLocker.RUnlock()

	ms.inodeLocker.RLock()
	inodedata, err := json.Marshal(ms.inodeData)
	if err != nil {
		ms.inodeLocker.RUnlock()
		return err
	}
	ms.inodeLocker.RUnlock()

	ms.BlockGroupLocker.RLock()
	bgdata, err := json.Marshal(ms.blockGroupData)
	if err != nil {
		ms.BlockGroupLocker.RUnlock()
		return err
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

	var appliedStr = strconv.FormatInt(int64(ms.applied), 10)

	f, err := os.OpenFile(path+"-"+appliedStr, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0666)
	if err != nil {
		f.Close()
		return err
	}
	w := bufio.NewWriter(f)

	w.Write(bigdata)
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

	err = rsg.Truncate(ms.applied)
	if err != nil {
		log.Error("TakeKvSnapShoot Truncate failed index : %v , err :%v", ms.applied, err)
		return err
	}
	log.Error("TakeKvSnapShoot Truncate Success index : %v ", ms.applied)

	return nil
}

//LoadKvSnapShoot ...
func LoadKvSnapShoot(ms *KvStateMachine, path string) (uint64, error) {

	fi, err := os.Open(path)
	if err != nil {
		return 0, nil
	}
	defer fi.Close()
	bigdata, err := ioutil.ReadAll(fi)

	ms.applied = binary.BigEndian.Uint64(bigdata)

	ms.DentryLocker.Lock()
	dentryLen := binary.BigEndian.Uint64(bigdata[8:16])
	if err = json.Unmarshal(bigdata[16:16+dentryLen], &ms.dentryData); err != nil {
		ms.DentryLocker.Unlock()
		return 0, err
	}
	ms.DentryLocker.Unlock()

	ms.inodeLocker.Lock()
	inodeLen := binary.BigEndian.Uint64(bigdata[16+dentryLen : 16+dentryLen+8])
	if err = json.Unmarshal(bigdata[16+dentryLen+8:16+dentryLen+8+inodeLen], &ms.inodeData); err != nil {
		ms.inodeLocker.Unlock()
		return 0, err
	}
	ms.inodeLocker.Unlock()

	ms.BlockGroupLocker.Lock()
	bgLen := binary.BigEndian.Uint64(bigdata[16+dentryLen+8+inodeLen : 16+dentryLen+8+inodeLen+8])
	if err = json.Unmarshal(bigdata[16+dentryLen+8+inodeLen+8:16+dentryLen+8+inodeLen+8+bgLen], &ms.blockGroupData); err != nil {
		ms.BlockGroupLocker.Unlock()
		return 0, err
	}
	ms.BlockGroupLocker.Unlock()

	ms.chunkID = binary.BigEndian.Uint64(bigdata[16+dentryLen+8+inodeLen+8+bgLen : 16+dentryLen+8+inodeLen+8+bgLen+8])
	ms.inodeID = binary.BigEndian.Uint64(bigdata[16+dentryLen+8+inodeLen+8+bgLen+8:])

	return ms.applied, nil
}
