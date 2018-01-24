package raftopt

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	pbproto "github.com/golang/protobuf/proto"
	"github.com/tiglabs/containerfs/logger"
	log "github.com/tiglabs/containerfs/logger"
	"github.com/tiglabs/containerfs/proto/kvp"
	"github.com/tiglabs/containerfs/proto/vp"
	com "github.com/tiglabs/containerfs/raftopt/common"
	"github.com/tiglabs/containerfs/raftopt/common/BTree"
	"github.com/tiglabs/raft"
	"github.com/tiglabs/raft/proto"
	"github.com/tiglabs/raft/storage/wal"
	"io"
	"io/ioutil"
	"os"
	"path"
	"strconv"
	//strings"
	"sync"
	"sync/atomic"
	"time"
)

var (
	errNotExists   = errors.New("Key not exists")
	errNotLeader   = errors.New("Not leader")
	ErrKeyNotFound = errors.New("Key not found")
)

const (
	OPT_ALLOCATE_RGID   = 1
	OPT_ALLOCATE_BGID   = 2
	OPT_SET_DATANODE    = 3
	OPT_DEL_DATANODE    = 4
	OPT_SET_DATANODEBGP = 5
	OPT_DEL_DATANODEBGP = 6
	OPT_SET_BGP         = 7
	OPT_DEL_BGP         = 8
	OPT_SET_VOL         = 9
	OPT_DEL_VOL         = 10
	OPT_SET_METANODE    = 11
	OPT_DEL_METANODE    = 12
	OPT_SET_MNRG        = 13
	OPT_DEL_MNRG        = 14

	//opt applied idx
	OPT_APPLIED = 15
)

//KvStateMachine ...
type ClusterKvStateMachine struct {
	id      uint64
	applied uint64
	raft    *raft.RaftServer

	rgIDLocker sync.Mutex
	rgID       uint64

	bgIDLocker sync.Mutex
	bgID       uint64

	volItem btree.VOLKV
	volData *btree.BTree

	dataNodeItem btree.DataNodeKV
	dataNodeData *btree.BTree

	dataNodeBGPItem btree.DataNodeBGPKV
	dataNodeBGPData *btree.BTree

	metaNodeItem btree.MetaNodeKV
	metaNodeData *btree.BTree

	blockGroupItem btree.BlockGroupKV
	blockGroupData *btree.BTree

	mnrgItem btree.MNRGKV
	mnrgData *btree.BTree
}

func newClusterKvStatemachine(id uint64, raft *raft.RaftServer) *ClusterKvStateMachine {
	return &ClusterKvStateMachine{
		id:              id,
		raft:            raft,
		blockGroupData:  btree.New(8),
		dataNodeData:    btree.New(8),
		dataNodeBGPData: btree.New(8),
		volData:         btree.New(8),
		metaNodeData:    btree.New(8),
		mnrgData:        btree.New(8),
		rgID:            1,
	}
}

///CreateKvStateMachine ...
func CreateClusterKvStateMachine(rs *raft.RaftServer, peers []proto.Peer, nodeID uint64, dir string, UUID string, raftGroupID uint64) (*ClusterKvStateMachine, *wal.Storage, error) {
	wc := &wal.Config{}
	raftStroage, err := wal.NewStorage(path.Join(dir, UUID, "wal"), wc)
	if err != nil {
		log.Error("new raft log stroage error: %v", err)
		return nil, nil, err
	}

	// state machine
	kvsm := newClusterKvStatemachine(nodeID, rs)

	var index uint64

	index, err = LoadClusterKvSnapShot(kvsm, path.Join(dir, UUID, "wal", "snap"))
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

//Apply ...
func (ms *ClusterKvStateMachine) Apply(data []byte, index uint64) (interface{}, error) {

	kv := &kvp.Kv{}
	err := pbproto.Unmarshal(data, kv)
	if err != nil {
		return nil, err
	}

	switch kv.Opt {
	case OPT_ALLOCATE_RGID:
		atomic.AddUint64(&ms.rgID, 1)
	case OPT_ALLOCATE_BGID: // allockChunkID
		atomic.AddUint64(&ms.bgID, 1)

	case OPT_SET_DATANODE: // set OPT_SET_DATANODE
		ms.dataNodeItem.K = kv.K
		ms.dataNodeItem.V = kv.V
		ms.dataNodeData.ReplaceOrInsert(ms.dataNodeItem)
	case OPT_DEL_DATANODE: // del OPT_SET_DATANODE
		ms.dataNodeItem.K = kv.K
		ms.dataNodeData.Delete(ms.dataNodeItem)

	case OPT_SET_DATANODEBGP: // set OPT_SET_DATANODEBGP
		ms.dataNodeBGPItem.K = kv.K
		ms.dataNodeBGPItem.V = kv.V
		ms.dataNodeBGPData.ReplaceOrInsert(ms.dataNodeBGPItem)
	case OPT_DEL_DATANODEBGP: // del OPT_DEL_DATANODEBGP
		ms.dataNodeBGPItem.K = kv.K
		ms.dataNodeBGPData.Delete(ms.dataNodeBGPItem)

	case OPT_SET_BGP: // set OPT_SET_BGP
		uint64ID, _ := strconv.ParseUint(kv.K, 10, 64)
		ms.blockGroupItem.K = uint64ID
		ms.blockGroupItem.V = kv.V
		ms.blockGroupData.ReplaceOrInsert(ms.blockGroupItem)
	case OPT_DEL_BGP: // del OPT_DEL_BGP
		uint64ID, _ := strconv.ParseUint(kv.K, 10, 64)
		ms.blockGroupItem.K = uint64ID
		ms.blockGroupData.Delete(ms.blockGroupItem)

	case OPT_SET_VOL: // set OPT_SET_VOL
		ms.volItem.K = kv.K
		ms.volItem.V = kv.V
		ms.volData.ReplaceOrInsert(ms.volItem)
	case OPT_DEL_VOL: // del OPT_DEL_VOL
		ms.volItem.K = kv.K
		ms.volData.Delete(ms.volItem)

	case OPT_SET_METANODE: // set OPT_SET_METANODE
		tmpK, _ := strconv.ParseUint(kv.K, 10, 64)
		ms.metaNodeItem.K = tmpK
		ms.metaNodeItem.V = kv.V
		ms.metaNodeData.ReplaceOrInsert(ms.metaNodeItem)
	case OPT_DEL_METANODE: // del OPT_DEL_METANODE
		tmpK, _ := strconv.ParseUint(kv.K, 10, 64)
		ms.metaNodeItem.K = tmpK
		ms.metaNodeData.Delete(ms.metaNodeItem)

	case OPT_SET_MNRG: //set OPT_SET_MNRG
		uint64ID, _ := strconv.ParseUint(kv.K, 10, 64)
		ms.mnrgItem.K = uint64ID
		ms.mnrgItem.V = kv.V
		ms.mnrgData.ReplaceOrInsert(ms.mnrgItem)
	case OPT_DEL_MNRG: //del OPT_DEL_MNRG
		uint64ID, _ := strconv.ParseUint(kv.K, 10, 64)
		ms.mnrgItem.K = uint64ID
		ms.mnrgData.Delete(ms.mnrgItem)
	}
	ms.applied = index
	return nil, nil
}

//BGGet ...
func (ms *ClusterKvStateMachine) BlockGroupGet(key uint64) (*vp.BlockGroup, error) {
	// if !ms.raft.IsLeader(1) {
	// 	return nil, errors.New("not leader")
	// }

	var item btree.BlockGroupKV
	item.K = key
	newItem := ms.blockGroupData.Get(item)

	if newItem == nil {
		return nil, errNotExists
	}
	blockGroup := &vp.BlockGroup{}
	err := pbproto.Unmarshal(newItem.(btree.BlockGroupKV).V, blockGroup)
	if err != nil {
		return nil, err
	}
	return blockGroup, nil
}

//BGSet ...
func (ms *ClusterKvStateMachine) BlockGroupSet(key uint64, blockGroup *vp.BlockGroup) error {
	if !ms.raft.IsLeader(1) {
		return errors.New("not leader")
	}

	var value []byte
	var err error
	if value, err = pbproto.Marshal(blockGroup); err != nil {
		return err
	}

	var data []byte
	k := strconv.FormatUint(key, 10)
	kv := &kvp.Kv{Opt: OPT_SET_BGP, K: k, V: value}

	if data, err = pbproto.Marshal(kv); err != nil {
		return err
	}
	resp := ms.raft.Submit(1, data)
	_, err = resp.Response()
	if err != nil {
		return fmt.Errorf("Put error[%v]", err)
	}
	return nil

}

func (ms *ClusterKvStateMachine) BlockGroupDel(raftGroupID uint64, key string) error {
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

//BGGetAll ...
func (ms *ClusterKvStateMachine) BlockGroupGetAll() ([]*vp.BlockGroup, error) {
	if !ms.raft.IsLeader(1) {
		return nil, errors.New("not leader")
	}

	var v []btree.BlockGroupKV

	ms.blockGroupData.Ascend(func(a btree.Item) bool {
		v = append(v, a.(btree.BlockGroupKV))
		return true
	})

	var blockGroups []*vp.BlockGroup
	var err error
	for _, vv := range v {
		blockGroup := &vp.BlockGroup{}
		if err = pbproto.Unmarshal(vv.V, blockGroup); err != nil {
			return nil, err
		}
		blockGroups = append(blockGroups, blockGroup)
	}
	return blockGroups, nil
}

func (ms *ClusterKvStateMachine) DataNodeGetAll(raftGroupID uint64) ([]*vp.DataNode, error) {
	if !ms.raft.IsLeader(raftGroupID) {
		return nil, errors.New("not leader")
	}
	var v []btree.DataNodeKV

	ms.dataNodeData.Ascend(func(a btree.Item) bool {
		v = append(v, a.(btree.DataNodeKV))
		return true
	})

	var dataNodes []*vp.DataNode
	var err error
	for _, vv := range v {
		dataNode := &vp.DataNode{}
		if err = pbproto.Unmarshal(vv.V, dataNode); err != nil {
			return nil, err
		}
		dataNodes = append(dataNodes, dataNode)
	}
	return dataNodes, nil
}

func (ms *ClusterKvStateMachine) DataNodeGetRange(raftGroupID uint64, minKey string) ([]*vp.DataNode, error) {
	if !ms.raft.IsLeader(raftGroupID) {
		return nil, errors.New("not leader")
	}

	var v []btree.DataNodeKV

	var itemMin btree.DataNodeKV
	itemMin.K = minKey

	ms.dataNodeData.AscendGreaterOrEqual(itemMin, func(a btree.Item) bool {
		v = append(v, a.(btree.DataNodeKV))
		return true
	})

	var dataNodes []*vp.DataNode
	var err error
	for _, vv := range v {
		dataNode := &vp.DataNode{}
		if err = pbproto.Unmarshal(vv.V, dataNode); err != nil {
			return nil, err
		}
		dataNodes = append(dataNodes, dataNode)
	}
	return dataNodes, nil
}

func (ms *ClusterKvStateMachine) DataNodeGet(raftGroupID uint64, key string) (*vp.DataNode, error) {
	var item btree.DataNodeKV
	item.K = key
	newItem := ms.dataNodeData.Get(item)

	if newItem != nil {
		dataNode := &vp.DataNode{}
		err := pbproto.Unmarshal(newItem.(btree.DataNodeKV).V, dataNode)
		if err != nil {
			return nil, err
		}
		return dataNode, nil
	}
	return nil, errNotExists
}

func (ms *ClusterKvStateMachine) DataNodeSet(raftGroupID uint64, key string, dataNode *vp.DataNode) error {
	if !ms.raft.IsLeader(raftGroupID) {
		return errors.New("not leader")
	}

	var value []byte
	var err error
	if value, err = pbproto.Marshal(dataNode); err != nil {
		return err
	}

	var data []byte
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

func (ms *ClusterKvStateMachine) DelDataNode(raftGroupID uint64, key string) error {
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

func (ms *ClusterKvStateMachine) DataNodeBGPGetAll() ([]*vp.DataNodeBGPS, error) {
	if !ms.raft.IsLeader(1) {
		return nil, errors.New("not leader")
	}
	var v []btree.DataNodeBGPKV

	ms.dataNodeBGPData.Ascend(func(a btree.Item) bool {
		v = append(v, a.(btree.DataNodeBGPKV))
		return true
	})
	var err error
	var dataNodeBGPS []*vp.DataNodeBGPS
	for _, vv := range v {
		dataNodeBGP := &vp.DataNodeBGPS{}
		if err = pbproto.Unmarshal(vv.V, dataNodeBGP); err != nil {
			return nil, err
		}
		dataNodeBGPS = append(dataNodeBGPS, dataNodeBGP)
	}
	return dataNodeBGPS, nil
}

func (ms *ClusterKvStateMachine) DataNodeBGPGet(key string) (*vp.DataNodeBGPS, error) {
	if !ms.raft.IsLeader(1) {
		return nil, errors.New("not leader")
	}
	var item btree.DataNodeBGPKV
	item.K = key
	newItem := ms.dataNodeBGPData.Get(item)
	if newItem == nil {
		return nil, ErrKeyNotFound
	}
	dataNodeBGPS := &vp.DataNodeBGPS{}
	err := pbproto.Unmarshal(newItem.(btree.DataNodeBGPKV).V, dataNodeBGPS)
	if err != nil {
		return nil, err
	}
	return dataNodeBGPS, nil
}

func (ms *ClusterKvStateMachine) DataNodeBGPSet(key string, dataNodeBGPS *vp.DataNodeBGPS) error {
	if !ms.raft.IsLeader(1) {
		return errors.New("not leader")
	}
	var data []byte
	var value []byte
	var err error

	if value, err = pbproto.Marshal(dataNodeBGPS); err != nil {
		return err
	}

	kv := &kvp.Kv{Opt: OPT_SET_DATANODEBGP, K: key, V: value}
	if data, err = pbproto.Marshal(kv); err != nil {
		return err
	}
	resp := ms.raft.Submit(1, data)
	_, err = resp.Response()
	if err != nil {
		return fmt.Errorf("Put error[%v]", err)
	}
	return nil

}

func (ms *ClusterKvStateMachine) DelDataNodeBGP(key string) error {
	if !ms.raft.IsLeader(1) {
		return errors.New("not leader")
	}
	var data []byte
	var err error

	kv := &kvp.Kv{Opt: OPT_DEL_DATANODEBGP, K: key}

	if data, err = pbproto.Marshal(kv); err != nil {
		return err
	}
	resp := ms.raft.Submit(1, data)
	_, err = resp.Response()
	if err != nil {
		return fmt.Errorf("Del error[%v]", err)
	}
	return nil

}

func (ms *ClusterKvStateMachine) DataNodeBGPDelBGP(key string, bgps []uint64) error {
	if !ms.raft.IsLeader(1) {
		return errNotLeader
	}
	var item btree.DataNodeBGPKV
	item.K = key
	newItem := ms.dataNodeBGPData.Get(item)

	var data, value []byte
	var err error
	tmpDataNodeBGP := &vp.DataNodeBGPS{Host: key}
	if newItem != nil {
		err = pbproto.Unmarshal(newItem.(btree.DataNodeBGPKV).V, tmpDataNodeBGP)
		if err != nil {
			logger.Error("parse failed: %v", err)
			return err
		}
	}

	tmpMap := make(map[uint64]struct{})
	for _, bgp := range bgps {
		tmpMap[bgp] = struct{}{}
	}

	dataNodeBGP := &vp.DataNodeBGPS{Host: key}
	for _, bgp := range tmpDataNodeBGP.BGPS {
		if _, ok := tmpMap[bgp]; !ok {
			dataNodeBGP.BGPS = append(dataNodeBGP.BGPS, bgp)
		}
	}
	if value, err = pbproto.Marshal(dataNodeBGP); err != nil {
		logger.Debug("serialize failed: %v", err)
		return err
	}
	kv := &kvp.Kv{Opt: OPT_SET_DATANODEBGP, K: key, V: value}
	if data, err = pbproto.Marshal(kv); err != nil {
		return err
	}
	resp := ms.raft.Submit(1, data)
	_, err = resp.Response()
	return err
}
func (ms *ClusterKvStateMachine) DataNodeBGPAddBGP(key string, bgp uint64) error {
	var item btree.DataNodeBGPKV
	item.K = key
	newItem := ms.dataNodeBGPData.Get(item)

	var data, value []byte
	var err error
	tmpDataNodeBGP := &vp.DataNodeBGPS{Host: key}
	if newItem != nil {
		err = pbproto.Unmarshal(newItem.(btree.DataNodeBGPKV).V, tmpDataNodeBGP)
		if err != nil {
			logger.Debug("parse failed: %v", err)
			return err
		}
	}
	tmpDataNodeBGP.BGPS = append(tmpDataNodeBGP.BGPS, bgp)
	if value, err = pbproto.Marshal(tmpDataNodeBGP); err != nil {
		logger.Debug("serialize failed: %v", err)
		return err
	}
	kv := &kvp.Kv{Opt: OPT_SET_DATANODEBGP, K: key, V: value}
	if data, err = pbproto.Marshal(kv); err != nil {
		return err
	}
	resp := ms.raft.Submit(1, data)
	_, err = resp.Response()
	if err != nil {
		return fmt.Errorf("Del error[%v]", err)
	}
	return nil

}
func (ms *ClusterKvStateMachine) RGIDGET(raftGroupID uint64) (uint64, error) {
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

//blockgroupIDGET ...
func (ms *ClusterKvStateMachine) BGIDGET(raftGroupID uint64) (uint64, error) {
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

func (ms *ClusterKvStateMachine) VolumeGet(raftGroupID uint64, key string) (*vp.Volume, error) {
	var item btree.VOLKV
	item.K = key
	newItem := ms.volData.Get(item)

	if newItem == nil {
		return nil, errNotExists
	}

	volume := &vp.Volume{}
	if err := pbproto.Unmarshal(newItem.(btree.VOLKV).V, volume); err != nil {
		return nil, err
	}
	return volume, nil
}

func (ms *ClusterKvStateMachine) VolumeGetAll(raftGroupID uint64) ([]*vp.Volume, error) {
	var v []btree.VOLKV

	ms.volData.Ascend(func(a btree.Item) bool {
		v = append(v, a.(btree.VOLKV))
		return true
	})

	var volumes []*vp.Volume
	var err error
	for _, vv := range v {
		volume := &vp.Volume{}
		if err = pbproto.Unmarshal(vv.V, volume); err != nil {
			return nil, err
		}
		volumes = append(volumes, volume)
	}
	return volumes, nil
}

func (ms *ClusterKvStateMachine) VolumeSet(raftGroupID uint64, key string, volume *vp.Volume) error {
	if !ms.raft.IsLeader(raftGroupID) {
		return errors.New("not leader")
	}

	var value []byte
	var err error
	if value, err = pbproto.Marshal(volume); err != nil {
		return err
	}

	var data []byte
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

func (ms *ClusterKvStateMachine) VolumeDel(raftGroupID uint64, key string) error {
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
func (ms *ClusterKvStateMachine) MetaNodeGetAll(raftGroupID uint64) ([]*vp.MetaNode, error) {
	var v []btree.MetaNodeKV

	ms.metaNodeData.Ascend(func(a btree.Item) bool {
		v = append(v, a.(btree.MetaNodeKV))
		return true
	})

	var metaNodes []*vp.MetaNode
	var err error
	for _, vv := range v {
		metaNode := &vp.MetaNode{}
		if err = pbproto.Unmarshal(vv.V, metaNode); err != nil {
			return nil, err
		}
		metaNodes = append(metaNodes, metaNode)
	}
	return metaNodes, nil
}

// cluster ...
func (ms *ClusterKvStateMachine) MetaNodeGetRange(raftGroupID uint64, minKey uint64) ([]*vp.MetaNode, error) {
	var v []btree.MetaNodeKV

	var itemMin btree.MetaNodeKV
	itemMin.K = minKey

	ms.dataNodeData.AscendGreaterOrEqual(itemMin, func(a btree.Item) bool {
		v = append(v, a.(btree.MetaNodeKV))
		return true
	})

	var metaNodes []*vp.MetaNode
	var err error
	for _, vv := range v {
		metaNode := &vp.MetaNode{}
		if err = pbproto.Unmarshal(vv.V, metaNode); err != nil {
			return nil, err
		}
		metaNodes = append(metaNodes, metaNode)
	}
	return metaNodes, nil
}

func (ms *ClusterKvStateMachine) MetaNodeGet(raftGroupID uint64, key uint64) (*vp.MetaNode, error) {
	var item btree.MetaNodeKV
	item.K = key
	newItem := ms.metaNodeData.Get(item)

	if newItem == nil {
		return nil, errNotExists
	}

	metaNode := &vp.MetaNode{}
	if err := pbproto.Unmarshal(newItem.(btree.MetaNodeKV).V, metaNode); err != nil {
		return nil, err
	}
	return metaNode, nil
}

func (ms *ClusterKvStateMachine) MetaNodeSet(raftGroupID uint64, key uint64, metaNode *vp.MetaNode) error {
	if !ms.raft.IsLeader(raftGroupID) {
		return errors.New("not leader")
	}

	var value []byte
	var err error
	if value, err = pbproto.Marshal(metaNode); err != nil {
		return err
	}

	var data []byte
	k := strconv.FormatUint(key, 10)
	kv := &kvp.Kv{Opt: OPT_SET_METANODE, K: k, V: value}

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

func (ms *ClusterKvStateMachine) DelMetaNode(raftGroupID uint64, key uint64) error {
	if !ms.raft.IsLeader(raftGroupID) {
		return errors.New("not leader")
	}
	var data []byte
	var err error

	k := strconv.FormatUint(key, 10)
	kv := &kvp.Kv{Opt: OPT_DEL_METANODE, K: k}

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
func (ms *ClusterKvStateMachine) MetaNodeRGGet(key uint64) (*vp.MetaNodeRG, error) {
	var item btree.MNRGKV
	item.K = key
	newItem := ms.mnrgData.Get(item)
	if newItem == nil {
		return nil, errNotExists
	}

	metaNodeRG := &vp.MetaNodeRG{}
	err := pbproto.Unmarshal(newItem.(btree.MNRGKV).V, metaNodeRG)
	if err != nil {
		return nil, err
	}
	return metaNodeRG, nil
}

func (ms *ClusterKvStateMachine) MetaNodeRGSet(key uint64, metaNodeRG *vp.MetaNodeRG) error {
	if !ms.raft.IsLeader(1) {
		return errNotLeader
	}
	var value []byte
	var err error
	if value, err = pbproto.Marshal(metaNodeRG); err != nil {
		return err
	}

	var data []byte
	rftKey := strconv.FormatUint(key, 10)
	kv := &kvp.Kv{Opt: OPT_SET_MNRG, K: rftKey, V: value}
	if data, err = pbproto.Marshal(kv); err != nil {
		return err
	}
	resp := ms.raft.Submit(1, data)
	_, err = resp.Response()
	if err != nil {
		logger.Debug("failed to set metanoderg: %v", err)
		return err
	}
	return nil
}
func (ms *ClusterKvStateMachine) MetaNodeRGGetRange(minKey uint64) ([]*vp.MetaNodeRG, error) {
	var v []btree.MNRGKV

	var itemMin btree.MNRGKV
	itemMin.K = minKey

	ms.mnrgData.AscendGreaterOrEqual(itemMin, func(a btree.Item) bool {
		v = append(v, a.(btree.MNRGKV))
		return true
	})

	var metaNodeRGS []*vp.MetaNodeRG
	var err error
	for _, vv := range v {
		metaNodeRG := &vp.MetaNodeRG{}
		if err = pbproto.Unmarshal(vv.V, metaNodeRG); err != nil {
			return nil, err
		}
		metaNodeRGS = append(metaNodeRGS, metaNodeRG)
	}
	return metaNodeRGS, nil
}
func (ms *ClusterKvStateMachine) MetaNodeRGGetAll() ([]*vp.MetaNodeRG, error) {
	logger.Debug("MetaNodeRGGetAll")
	var v []btree.MNRGKV

	ms.mnrgData.Ascend(func(a btree.Item) bool {
		v = append(v, a.(btree.MNRGKV))
		return true
	})

	var metaNodeRGS []*vp.MetaNodeRG
	var err error
	for _, vv := range v {
		metaNodeRG := &vp.MetaNodeRG{}
		if err = pbproto.Unmarshal(vv.V, metaNodeRG); err != nil {
			return nil, err
		}
		metaNodeRGS = append(metaNodeRGS, metaNodeRG)
	}
	return metaNodeRGS, nil
}
func (ms *ClusterKvStateMachine) DelMetaNodeRG(key uint64) error {
	if !ms.raft.IsLeader(1) {
		return errNotLeader
	}
	var data []byte
	var err error

	rftKey := strconv.FormatUint(key, 10)
	kv := &kvp.Kv{Opt: OPT_DEL_MNRG, K: rftKey}

	if data, err = pbproto.Marshal(kv); err != nil {
		return err
	}
	resp := ms.raft.Submit(1, data)
	_, err = resp.Response()
	if err != nil {
		return fmt.Errorf("DelMetaNodeRG error[%v]", err)
	}
	return nil
}

//AddNode ...
func (ms *ClusterKvStateMachine) AddNode(peer proto.Peer) error {
	resp := ms.raft.ChangeMember(1, proto.ConfAddNode, peer, nil)
	_, err := resp.Response()
	if err != nil {
		return errors.New("AddNode error")
	}
	return nil
}

//RemoveNode ...
func (ms *ClusterKvStateMachine) RemoveNode(peer proto.Peer) error {
	resp := ms.raft.ChangeMember(1, proto.ConfRemoveNode, peer, nil)
	_, err := resp.Response()
	if err != nil {
		return errors.New("RemoveNode error")
	}
	return nil
}

//ApplyMemberChange ...
func (ms *ClusterKvStateMachine) ApplyMemberChange(confChange *proto.ConfChange, index uint64) (interface{}, error) {
	return nil, nil
}

//HandleLeaderChange ...
func (ms *ClusterKvStateMachine) HandleLeaderChange(leader uint64) {
}

//HandleFatalEvent ...
func (ms *ClusterKvStateMachine) HandleFatalEvent(err *raft.FatalError) {
	panic(err.Err)
}

func (ms *ClusterKvStateMachine) setApplied(index uint64) {
	ms.applied = index
}

//Snapshot ...
func (ms *ClusterKvStateMachine) Snapshot() (proto.Snapshot, error) {

	ss := &ClusterKvSnapShot{
		applied:     ms.applied,
		rgID:        ms.rgID,
		bgID:        ms.bgID,
		curBtreeIdx: 0,
		snap:        make([]*snapItem, 6),
	}
	ss.snap[0] = &snapItem{tree: ms.volData, curItem: ms.volData.Min(), opt: OPT_SET_VOL}
	ss.snap[1] = &snapItem{tree: ms.dataNodeData, curItem: ms.dataNodeData.Min(), opt: OPT_SET_DATANODE}
	ss.snap[2] = &snapItem{tree: ms.dataNodeBGPData, curItem: ms.dataNodeBGPData.Min(), opt: OPT_SET_DATANODEBGP}
	ss.snap[3] = &snapItem{tree: ms.metaNodeData, curItem: ms.metaNodeData.Min(), opt: OPT_SET_METANODE}
	ss.snap[4] = &snapItem{tree: ms.blockGroupData, curItem: ms.blockGroupData.Min(), opt: OPT_SET_BGP}
	ss.snap[5] = &snapItem{tree: ms.mnrgData, curItem: ms.mnrgData.Min(), opt: OPT_SET_MNRG}

	return ss, nil

}

//ApplySnapshot ...
func (ms *ClusterKvStateMachine) ApplySnapshot(peers []proto.Peer, iter proto.SnapIterator) error {

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
			case OPT_ALLOCATE_RGID:
				uint64ID, _ = strconv.ParseUint(kv.K, 10, 64)
				ms.rgID = uint64ID
			case OPT_ALLOCATE_BGID:
				uint64ID, _ = strconv.ParseUint(kv.K, 10, 64)
				ms.bgID = uint64ID
			case OPT_SET_BGP:
				uintID, _ := strconv.ParseUint(kv.K, 10, 64)
				ms.blockGroupItem.K = uintID
				ms.blockGroupItem.V = kv.V
				ms.blockGroupData.ReplaceOrInsert(ms.blockGroupItem)
			case OPT_SET_DATANODE:
				ms.dataNodeItem.K = kv.K
				ms.dataNodeItem.V = kv.V
				ms.dataNodeData.ReplaceOrInsert(ms.dataNodeItem)
			case OPT_SET_METANODE:
				uintID, _ := strconv.ParseUint(kv.K, 10, 64)
				ms.metaNodeItem.K = uintID
				ms.metaNodeItem.V = kv.V
				ms.metaNodeData.ReplaceOrInsert(ms.metaNodeItem)
			case OPT_SET_MNRG:
				uint64ID, _ := strconv.ParseUint(kv.K, 10, 64)
				ms.mnrgItem.K = uint64ID
				ms.mnrgItem.V = kv.V
				ms.mnrgData.ReplaceOrInsert(ms.mnrgItem)
			case OPT_SET_VOL:
				ms.volItem.K = kv.K
				ms.volItem.V = kv.V
				ms.volData.ReplaceOrInsert(ms.volItem)
			}

		}
	}
	if err != nil && err != io.EOF {
		return err
	}

	return nil
}

type ClusterKvSnapShot struct {
	applied     uint64
	appliedFlag bool

	rgID     uint64
	rgIDFlag bool

	bgID     uint64
	bgIDFlag bool

	curBtreeIdx int

	snap []*snapItem
}

type snapItem struct {
	tree    *btree.BTree
	curItem btree.Item
	opt     uint32
}

func tmplog(v string) {
	logger.Debug("Next:", v)
}

//Next ...
func (s *ClusterKvSnapShot) Next() ([]byte, error) {

	if s.appliedFlag && s.rgIDFlag && s.bgIDFlag && s.curBtreeIdx >= len(s.snap) {
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
	if !s.rgIDFlag {

		kv.Opt = OPT_ALLOCATE_RGID
		kv.K = strconv.FormatUint(s.rgID, 10)

		if data, err = pbproto.Marshal(kv); err != nil {
			return nil, err
		}

		s.rgIDFlag = true
		return data, nil
	}
	if !s.bgIDFlag {

		kv.Opt = OPT_ALLOCATE_BGID
		kv.K = strconv.FormatUint(s.bgID, 10)

		if data, err = pbproto.Marshal(kv); err != nil {
			return nil, err
		}

		s.bgIDFlag = true
		return data, nil
	}

	for s.curBtreeIdx < len(s.snap) {
		snap := s.snap[s.curBtreeIdx]
		kv.Opt = snap.opt
		snap.tree.AscendGreaterOrEqual(snap.curItem, func(a btree.Item) bool {
			if snap.curItem.Less(a) {
				kv.K = a.(KV).Key()
				kv.V = a.(KV).Value()
				snap.curItem = a
				return false
			}
			kv.K = a.(KV).Key()
			kv.V = a.(KV).Value()
			return true
		})
		if snap.curItem.(KV).Key() == snap.tree.Max().(KV).Key() {
			s.curBtreeIdx = s.curBtreeIdx + 1
			continue
		}
		data, err = pbproto.Marshal(kv)
		return nil, err
	}

	return nil, io.ErrUnexpectedEOF
}

//ApplyIndex ...
func (s *ClusterKvSnapShot) ApplyIndex() uint64 {
	return s.applied
}

//Close ...
func (s *ClusterKvSnapShot) Close() {
	return
}

//interface for snapshot
type KV interface {
	Key() string
	Value() []byte
}

//TakeKvSnapShot ...
func TakeClusterKvSnapShot(ms *ClusterKvStateMachine, rsg *wal.Storage, dir string) error {

	_, err := os.Stat(dir)
	if err == nil {
		t := time.Now()
		os.Rename(dir, dir+"-backup"+"-"+t.String())
	}
	os.MkdirAll(dir, 0777)

	var data []byte
	var fpath string

	flag := os.O_RDWR | os.O_CREATE | os.O_APPEND
	perm := os.FileMode(0666)
	fpath = path.Join(dir, "applied")
	f, err := os.OpenFile(fpath, flag, perm)
	if err != nil {
		f.Close()
		return err
	}
	w := bufio.NewWriter(f)
	w.Write([]byte(strconv.FormatUint(ms.applied, 10)))
	w.Flush()
	f.Close()

	fpath = path.Join(dir, "rgid")
	f, err = os.OpenFile(fpath, flag, perm)
	if err != nil {
		f.Close()
		return err
	}
	w = bufio.NewWriter(f)
	w.Write([]byte(strconv.FormatUint(ms.applied, 10)))
	w.Flush()
	f.Close()
	fpath = path.Join(dir, "bgid")
	f, err = os.OpenFile(fpath, flag, perm)
	if err != nil {
		f.Close()
		return err
	}
	w = bufio.NewWriter(f)
	w.Write([]byte(strconv.FormatUint(ms.bgID, 10)))
	w.Flush()
	f.Close()

	fpath = path.Join(dir, "volData")
	if f, err = os.OpenFile(fpath, flag, perm); err != nil {
		return err
	}
	w = bufio.NewWriter(f)
	ms.volData.Ascend(func(a btree.Item) bool {
		if data, err = json.Marshal(a.(btree.VOLKV)); err != nil {
			return false
		}
		w.Write(data)
		w.WriteByte('\n')
		w.Flush()
		return true
	})
	f.Close()

	fpath = path.Join(dir, "dataNodeData")
	if f, err = os.OpenFile(fpath, flag, perm); err != nil {
		return err
	}
	w = bufio.NewWriter(f)
	ms.dataNodeData.Ascend(func(a btree.Item) bool {
		if data, err = json.Marshal(a.(btree.DataNodeKV)); err != nil {
			return false
		}
		w.Write(data)
		w.WriteByte('\n')
		w.Flush()
		return true
	})
	f.Close()

	fpath = path.Join(dir, "dataNodeBGPData")
	if f, err = os.OpenFile(fpath, flag, perm); err != nil {
		return err
	}
	w = bufio.NewWriter(f)
	ms.dataNodeBGPData.Ascend(func(a btree.Item) bool {
		if data, err = json.Marshal(a.(btree.DataNodeBGPKV)); err != nil {
			return false
		}
		w.Write(data)
		w.WriteByte('\n')
		w.Flush()
		return true
	})
	f.Close()

	fpath = path.Join(dir, "metaNodeData")
	if f, err = os.OpenFile(fpath, flag, perm); err != nil {
		return err
	}
	w = bufio.NewWriter(f)
	ms.metaNodeData.Ascend(func(a btree.Item) bool {
		if data, err = json.Marshal(a.(btree.MetaNodeKV)); err != nil {
			return false
		}
		w.Write(data)
		w.WriteByte('\n')
		w.Flush()
		return true
	})
	f.Close()

	fpath = path.Join(dir, "blockGroupData")
	if f, err = os.OpenFile(fpath, flag, perm); err != nil {
		return err
	}
	w = bufio.NewWriter(f)
	ms.blockGroupData.Ascend(func(a btree.Item) bool {
		if data, err = json.Marshal(a.(btree.BlockGroupKV)); err != nil {
			return false
		}
		w.Write(data)
		w.WriteByte('\n')
		w.Flush()
		return true
	})
	f.Close()

	fpath = path.Join(dir, "mnrgData")
	if f, err = os.OpenFile(fpath, flag, perm); err != nil {
		return err
	}
	w = bufio.NewWriter(f)
	ms.mnrgData.Ascend(func(a btree.Item) bool {
		if data, err = json.Marshal(a.(btree.MNRGKV)); err != nil {
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
		log.Error("TakeKvSnapShot Truncate failed index : %v , err :%v", ms.applied, err)
		return err
	}

	return nil
}

//LoadKvSnapShot ...
func LoadClusterKvSnapShot(ms *ClusterKvStateMachine, dir string) (uint64, error) {

	var fpath string

	fpath = path.Join(dir, "applied")
	f, err := os.Open(fpath)
	if err != nil {
		return 0, nil
	}
	data, err := ioutil.ReadAll(f)
	ms.applied, err = strconv.ParseUint(string(data), 10, 64)
	log.Debug("ms.applied %v", ms.applied)
	f.Close()

	fpath = path.Join(dir, "rgid")
	f, err = os.Open(fpath)
	if err != nil {
		return 0, nil
	}
	data, err = ioutil.ReadAll(f)
	ms.rgID, err = strconv.ParseUint(string(data), 10, 64)
	log.Debug("ms.rgID %v", ms.rgID)
	f.Close()

	fpath = path.Join(dir, "bgid")
	f, err = os.Open(fpath)
	if err != nil {
		return 0, nil
	}
	data, err = ioutil.ReadAll(f)
	ms.bgID, err = strconv.ParseUint(string(data), 10, 64)
	log.Debug("ms.bgID %v", ms.bgID)
	f.Close()

	fpath = path.Join(dir, "volData")
	if f, err = os.Open(fpath); err != nil {
		return 0, nil
	}
	buf := bufio.NewReader(f)

	var vol btree.VOLKV
	for {
		line, err := buf.ReadBytes('\n')
		if err != nil {
			if err == io.EOF {
				break
			}
			f.Close()
			return 0, err
		}
		err = json.Unmarshal(line, &vol)
		if err != nil {
			f.Close()
			return 0, err
		}
		log.Debug("vol %v", vol)
		ms.volData.ReplaceOrInsert(vol)
	}
	f.Close()

	fpath = path.Join(dir, "dataNodeData")
	if f, err = os.Open(fpath); err != nil {
		return 0, nil
	}
	buf = bufio.NewReader(f)

	var dataNode btree.DataNodeKV
	for {
		line, err := buf.ReadBytes('\n')
		if err != nil {
			if err == io.EOF {
				break
			}
			f.Close()
			return 0, err
		}
		err = json.Unmarshal(line, &dataNode)
		if err != nil {
			f.Close()
			return 0, err
		}
		log.Debug("dataNode %v", dataNode)
		ms.dataNodeData.ReplaceOrInsert(dataNode)
	}
	f.Close()

	fpath = path.Join(dir, "dataNodeBGPData")
	if f, err = os.Open(fpath); err != nil {
		return 0, nil
	}
	buf = bufio.NewReader(f)

	var dataNodeBGP btree.DataNodeBGPKV
	for {
		line, err := buf.ReadBytes('\n')
		if err != nil {
			if err == io.EOF {
				break
			}
			f.Close()
			return 0, err
		}
		err = json.Unmarshal(line, &dataNodeBGP)
		if err != nil {
			f.Close()
			return 0, err
		}
		log.Debug("dataNodeBGP %v", dataNodeBGP)
		ms.dataNodeBGPData.ReplaceOrInsert(dataNodeBGP)
	}
	f.Close()

	fpath = path.Join(dir, "metaNodeData")
	if f, err = os.Open(fpath); err != nil {
		return 0, nil
	}
	buf = bufio.NewReader(f)
	var metaNode btree.MetaNodeKV
	for {
		line, err := buf.ReadBytes('\n')
		if err != nil {
			if err == io.EOF {
				break
			}
			f.Close()
			return 0, err
		}
		err = json.Unmarshal(line, &metaNode)
		if err != nil {
			f.Close()
			return 0, err
		}
		log.Debug("metaNode %v", metaNode)
		ms.metaNodeData.ReplaceOrInsert(metaNode)
	}
	f.Close()

	fpath = path.Join(dir, "blockGroupData")
	if f, err = os.Open(fpath); err != nil {
		return 0, nil
	}
	buf = bufio.NewReader(f)

	var blockGroup btree.BlockGroupKV
	for {
		line, err := buf.ReadBytes('\n')
		if err != nil {
			if err == io.EOF {
				break
			}
			f.Close()
			return 0, err
		}
		err = json.Unmarshal(line, &blockGroup)
		if err != nil {
			f.Close()
			return 0, err
		}
		log.Debug("blockGroup %v", blockGroup)
		ms.blockGroupData.ReplaceOrInsert(blockGroup)
	}
	f.Close()

	fpath = path.Join(dir, "mnrgData")
	if f, err = os.Open(fpath); err != nil {
		return 0, nil
	}
	buf = bufio.NewReader(f)

	var mnrg btree.MNRGKV
	for {
		line, err := buf.ReadBytes('\n')
		if err != nil {
			if err == io.EOF {
				break
			}
			f.Close()
			return 0, err
		}
		err = json.Unmarshal(line, &mnrg)
		if err != nil {
			f.Close()
			return 0, err
		}
		log.Debug("mnrg %v", mnrg)
		ms.mnrgData.ReplaceOrInsert(mnrg)
	}
	f.Close()

	log.Debug("ms.applied end %v", ms.applied)

	return ms.applied, nil

}

////////////////////////////////////////////////////////////////////////////

//AddrDatabase ...
var ClusterAddrDatabase = make(map[uint64]*com.Address)

//AddInit ...
func AddInit(ips []string) {
	fmt.Println("IPS:")
	for i := range ips {
		ClusterAddrDatabase[uint64(i+1)] = &com.Address{
			Heartbeat: fmt.Sprintf("%s:77%d1", ips[i], i),
			Replicate: fmt.Sprintf("%s:77%d2", ips[i], i),
			Grpc:      fmt.Sprintf("%s:77%d3", ips[i], i),
			Pprof:     fmt.Sprintf("%s:77%d4", ips[i], i),
		}
		fmt.Println(ClusterAddrDatabase[uint64(i+1)])
	}
}

//Resolver ...
type ClusterResolver struct {
	nodes map[uint64]struct{}
	sync.Mutex
}

//NewResolver ...
func NewClusterResolver() *ClusterResolver {
	return &ClusterResolver{nodes: make(map[uint64]struct{})}
}

//AddNode ...
func (r *ClusterResolver) AddNode(nodeID uint64, addr *com.Address) {
	r.Lock()
	r.nodes[nodeID] = struct{}{}
	r.Unlock()
}

//RemoveNode ...
func (r *ClusterResolver) RemoveNode(nodeID uint64, addr *com.Address) {
	r.Lock()
	delete(r.nodes, nodeID)
	r.Unlock()
}

//AllNodes ...
func (r *ClusterResolver) AllNodes() (all []uint64) {
	r.Lock()
	for k := range r.nodes {
		all = append(all, k)
	}
	r.Unlock()
	return
}

//NodeAddress ...
func (r *ClusterResolver) NodeAddress(nodeID uint64, stype raft.SocketType) (addr string, err error) {
	raddr, ok := ClusterAddrDatabase[nodeID]
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
