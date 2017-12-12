package raftopt

import (
	"bufio"
	"encoding/json"
	log "github.com/tigcode/containerfs/logger"
	"github.com/tigcode/containerfs/metanode/raftopt/BTree"
	"github.com/tigcode/raft"
	"github.com/tigcode/raft/proto"
	"github.com/tigcode/raft/storage/wal"
	"io"
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

	var index uint64

	if UUID != "Cluster" {
		index, err = LoadKvSnapShoot(kvsm, path.Join(dir, UUID, "wal", "snap"))
		if err != nil {
			return nil, nil, err
		}
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
func LoadKvSnapShoot(ms *KvStateMachine, path string) (uint64, error) {
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
