// Copyright (c) 2017, TIG All rights reserved.
// Use of this source code is governed by a Apache License 2.0 that can be found in the LICENSE file.

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
	"github.com/google/btree"
	"github.com/tiglabs/containerfs/logger"
	"github.com/tiglabs/containerfs/proto/kvp"
	"github.com/tiglabs/containerfs/proto/vp"
	"github.com/tiglabs/containerfs/raftopt/btreeinstance"
	"github.com/tiglabs/containerfs/raftopt/common"
	"github.com/tiglabs/raft"
	"github.com/tiglabs/raft/proto"
	"github.com/tiglabs/raft/storage/wal"
)

var (
	errNotExists = errors.New("Key not exists")
	errNotLeader = errors.New("Not leader")
	// ErrKeyNotFound is key not found
	ErrKeyNotFound = errors.New("Key not found")
)

//ClusterKvStateMachine is a wrapper of btree based state machine for cluster management
type ClusterKvStateMachine struct {
	id      uint64
	applied uint64
	raft    *raft.RaftServer

	rgIDLocker sync.Mutex
	rgID       uint64

	bgIDLocker sync.Mutex
	bgID       uint64

	volItem btreeinstance.VOLKV
	volData *btree.BTree

	dataNodeItem btreeinstance.DataNodeKV
	dataNodeData *btree.BTree

	dataNodeBGItem btreeinstance.DataNodeBGKV
	dataNodeBGData *btree.BTree

	metaNodeItem btreeinstance.MetaNodeKV
	metaNodeData *btree.BTree

	blockGroupItem btreeinstance.BlockGroupKV
	blockGroupData *btree.BTree

	mnrgItem btreeinstance.MNRGKV
	mnrgData *btree.BTree
}

//newClusterKvStatemachine creates a new ClusterKvStateMachine
func newClusterKvStatemachine(id uint64, raft *raft.RaftServer) *ClusterKvStateMachine {
	return &ClusterKvStateMachine{
		id:             id,
		raft:           raft,
		blockGroupData: btree.New(8),
		dataNodeData:   btree.New(8),
		dataNodeBGData: btree.New(8),
		volData:        btree.New(8),
		metaNodeData:   btree.New(8),
		mnrgData:       btree.New(8),
		rgID:           1,
	}
}

//CreateClusterKvStateMachine is exported out to initializing cluster kv service
func CreateClusterKvStateMachine(rs *raft.RaftServer, peers []proto.Peer, nodeID uint64, dir string, UUID string, raftGroupID uint64) (*ClusterKvStateMachine, *wal.Storage, error) {
	wc := &wal.Config{}
	raftStroage, err := wal.NewStorage(path.Join(dir, UUID, "wal"), wc)
	if err != nil {
		logger.Error("new raft log stroage error: %v", err)
		return nil, nil, err
	}

	// state machine
	kvsm := newClusterKvStatemachine(nodeID, rs)

	var index uint64

	index, err = LoadClusterKvSnapShot(kvsm, path.Join(dir, UUID, "wal", "snap"))
	if err != nil {
		return nil, nil, err
	}

	logger.Debug("CreateKvStateMachine Success index : %v", index)

	rc := &raft.RaftConfig{
		ID:           raftGroupID,
		Peers:        peers,
		Storage:      raftStroage,
		StateMachine: kvsm,
		Applied:      index,
	}
	err = rs.CreateRaft(rc)
	if err != nil {
		logger.Error("creat raft failed. %v", err)
	}

	logger.Debug("CreateRaft Success index : %v", rc.Applied)

	return kvsm, raftStroage, nil

}

//Apply dispatches raft operations
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

	case OPT_SET_DATANODEBG: // set OPT_SET_DATANODEBG
		ms.dataNodeBGItem.K = kv.K
		ms.dataNodeBGItem.V = kv.V
		ms.dataNodeBGData.ReplaceOrInsert(ms.dataNodeBGItem)
	case OPT_DEL_DATANODEBG: // del OPT_DEL_DATANODEBG
		ms.dataNodeBGItem.K = kv.K
		ms.dataNodeBGData.Delete(ms.dataNodeBGItem)

	case OPT_SET_BG: // set OPT_SET_BG
		uint64ID, _ := strconv.ParseUint(kv.K, 10, 64)
		ms.blockGroupItem.K = uint64ID
		ms.blockGroupItem.V = kv.V
		ms.blockGroupData.ReplaceOrInsert(ms.blockGroupItem)
	case OPT_DEL_BG: // del OPT_DEL_BG
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

//BlockGroupGet gets Block Group by key
func (ms *ClusterKvStateMachine) BlockGroupGet(key uint64) (*vp.BlockGroup, error) {
	// if !ms.raft.IsLeader(1) {
	// 	return nil, errors.New("not leader")
	// }

	var item btreeinstance.BlockGroupKV
	item.K = key
	newItem := ms.blockGroupData.Get(item)

	if newItem == nil {
		return nil, errNotExists
	}
	blockGroup := &vp.BlockGroup{}
	err := pbproto.Unmarshal(newItem.(btreeinstance.BlockGroupKV).V, blockGroup)
	if err != nil {
		return nil, err
	}
	return blockGroup, nil
}

//BlockGroupSet sets Block Group into sm
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
	kv := &kvp.Kv{Opt: OPT_SET_BG, K: k, V: value}

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

//BlockGroupDel removes Block Group from cluster by key
func (ms *ClusterKvStateMachine) BlockGroupDel(raftGroupID uint64, key string) error {
	if !ms.raft.IsLeader(raftGroupID) {
		return errors.New("not leader")
	}
	var data []byte
	var err error

	kv := &kvp.Kv{Opt: OPT_DEL_BG, K: key}

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

//BlockGroupGetAll gets all Block Groups
func (ms *ClusterKvStateMachine) BlockGroupGetAll() ([]*vp.BlockGroup, error) {
	if !ms.raft.IsLeader(1) {
		return nil, errors.New("not leader")
	}

	var v []btreeinstance.BlockGroupKV

	ms.blockGroupData.Ascend(func(a btree.Item) bool {
		v = append(v, a.(btreeinstance.BlockGroupKV))
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

//DataNodeGetAll gets all datanodes in cluster
func (ms *ClusterKvStateMachine) DataNodeGetAll(raftGroupID uint64) ([]*vp.DataNode, error) {
	if !ms.raft.IsLeader(raftGroupID) {
		return nil, errors.New("not leader")
	}
	var v []btreeinstance.DataNodeKV

	ms.dataNodeData.Ascend(func(a btree.Item) bool {
		v = append(v, a.(btreeinstance.DataNodeKV))
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

//DataNodeGetRange gets all daatanodes in cluster with key not less than minKey
func (ms *ClusterKvStateMachine) DataNodeGetRange(raftGroupID uint64, minKey string) ([]*vp.DataNode, error) {
	if !ms.raft.IsLeader(raftGroupID) {
		return nil, errors.New("not leader")
	}

	var v []btreeinstance.DataNodeKV

	var itemMin btreeinstance.DataNodeKV
	itemMin.K = minKey

	ms.dataNodeData.AscendGreaterOrEqual(itemMin, func(a btree.Item) bool {
		v = append(v, a.(btreeinstance.DataNodeKV))
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

//DataNodeGet gets datanode by key
func (ms *ClusterKvStateMachine) DataNodeGet(raftGroupID uint64, key string) (*vp.DataNode, error) {
	var item btreeinstance.DataNodeKV
	item.K = key
	newItem := ms.dataNodeData.Get(item)

	if newItem != nil {
		dataNode := &vp.DataNode{}
		err := pbproto.Unmarshal(newItem.(btreeinstance.DataNodeKV).V, dataNode)
		if err != nil {
			return nil, err
		}
		return dataNode, nil
	}
	return nil, ErrKeyNotFound
}

//DataNodeSet sets datanode kv
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

//DelDataNode removes datanode kv by key
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

//DataNodeBGGetAll gets all items in map from datanode to Block Group
func (ms *ClusterKvStateMachine) DataNodeBGGetAll() ([]*vp.DataNodeBGS, error) {
	if !ms.raft.IsLeader(1) {
		return nil, errors.New("not leader")
	}
	var v []btreeinstance.DataNodeBGKV

	ms.dataNodeBGData.Ascend(func(a btree.Item) bool {
		v = append(v, a.(btreeinstance.DataNodeBGKV))
		return true
	})
	var err error
	var dataNodeBGS []*vp.DataNodeBGS
	for _, vv := range v {
		dataNodeBG := &vp.DataNodeBGS{}
		if err = pbproto.Unmarshal(vv.V, dataNodeBG); err != nil {
			return nil, err
		}
		dataNodeBGS = append(dataNodeBGS, dataNodeBG)
	}
	return dataNodeBGS, nil
}

//DataNodeBGGet gets map from datanode by key to Block Group
func (ms *ClusterKvStateMachine) DataNodeBGGet(key string) (*vp.DataNodeBGS, error) {
	if !ms.raft.IsLeader(1) {
		return nil, errors.New("not leader")
	}
	var item btreeinstance.DataNodeBGKV
	item.K = key
	newItem := ms.dataNodeBGData.Get(item)
	if newItem == nil {
		return nil, ErrKeyNotFound
	}
	dataNodeBGS := &vp.DataNodeBGS{}
	err := pbproto.Unmarshal(newItem.(btreeinstance.DataNodeBGKV).V, dataNodeBGS)
	if err != nil {
		return nil, err
	}
	return dataNodeBGS, nil
}

//DataNodeBGSet sets map from datanode by key to Block Group
func (ms *ClusterKvStateMachine) DataNodeBGSet(key string, dataNodeBGS *vp.DataNodeBGS) error {
	if !ms.raft.IsLeader(1) {
		return errors.New("not leader")
	}
	var data []byte
	var value []byte
	var err error

	if value, err = pbproto.Marshal(dataNodeBGS); err != nil {
		return err
	}

	kv := &kvp.Kv{Opt: OPT_SET_DATANODEBG, K: key, V: value}
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

//DelDataNodeBG removes map from datanode by key to all Block Group
func (ms *ClusterKvStateMachine) DelDataNodeBG(key string) error {
	if !ms.raft.IsLeader(1) {
		return errors.New("not leader")
	}
	var data []byte
	var err error

	kv := &kvp.Kv{Opt: OPT_DEL_DATANODEBG, K: key}

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

//DataNodeBGDelBG removes Block Groups in bgs from datanode Block Group map
func (ms *ClusterKvStateMachine) DataNodeBGDelBG(key string, bgs []uint64) error {
	if !ms.raft.IsLeader(1) {
		return errNotLeader
	}
	var item btreeinstance.DataNodeBGKV
	item.K = key
	newItem := ms.dataNodeBGData.Get(item)

	var data, value []byte
	var err error
	tmpDataNodeBG := &vp.DataNodeBGS{Host: key}
	if newItem != nil {
		err = pbproto.Unmarshal(newItem.(btreeinstance.DataNodeBGKV).V, tmpDataNodeBG)
		if err != nil {
			logger.Error("parse failed: %v", err)
			return err
		}
	}

	tmpMap := make(map[uint64]struct{})
	for _, bg := range bgs {
		tmpMap[bg] = struct{}{}
	}

	dataNodeBG := &vp.DataNodeBGS{Host: key}
	for _, bg := range tmpDataNodeBG.BGS {
		if _, ok := tmpMap[bg]; !ok {
			dataNodeBG.BGS = append(dataNodeBG.BGS, bg)
		}
	}
	if value, err = pbproto.Marshal(dataNodeBG); err != nil {
		logger.Debug("serialize failed: %v", err)
		return err
	}
	kv := &kvp.Kv{Opt: OPT_SET_DATANODEBG, K: key, V: value}
	if data, err = pbproto.Marshal(kv); err != nil {
		return err
	}
	resp := ms.raft.Submit(1, data)
	_, err = resp.Response()
	return err
}

//DataNodeBGAddBG adds Block Groups into bgs from datanode Block Group map
func (ms *ClusterKvStateMachine) DataNodeBGAddBG(key string, bg uint64) error {
	var item btreeinstance.DataNodeBGKV
	item.K = key
	newItem := ms.dataNodeBGData.Get(item)

	var data, value []byte
	var err error
	tmpDataNodeBG := &vp.DataNodeBGS{Host: key}
	if newItem != nil {
		err = pbproto.Unmarshal(newItem.(btreeinstance.DataNodeBGKV).V, tmpDataNodeBG)
		if err != nil {
			logger.Debug("parse failed: %v", err)
			return err
		}
	}
	tmpDataNodeBG.BGS = append(tmpDataNodeBG.BGS, bg)
	if value, err = pbproto.Marshal(tmpDataNodeBG); err != nil {
		logger.Debug("serialize failed: %v", err)
		return err
	}
	kv := &kvp.Kv{Opt: OPT_SET_DATANODEBG, K: key, V: value}
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

//RGIDGET generates a new global unique raft group id
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

//BGIDGET generates a new global unique Block Group id
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

//VolumeGet gets volume info from volume kv by key
func (ms *ClusterKvStateMachine) VolumeGet(raftGroupID uint64, key string) (*vp.Volume, error) {
	var item btreeinstance.VOLKV
	item.K = key
	newItem := ms.volData.Get(item)

	if newItem == nil {
		return nil, errNotExists
	}

	volume := &vp.Volume{}
	if err := pbproto.Unmarshal(newItem.(btreeinstance.VOLKV).V, volume); err != nil {
		return nil, err
	}
	return volume, nil
}

//VolumeGetAll gets info of all volumes in volume kv
func (ms *ClusterKvStateMachine) VolumeGetAll(raftGroupID uint64) ([]*vp.Volume, error) {
	var v []btreeinstance.VOLKV

	ms.volData.Ascend(func(a btree.Item) bool {
		v = append(v, a.(btreeinstance.VOLKV))
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

//VolumeSet sets volume kv
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

//VolumeDel removes volume from volume kv by key
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

//MetaNodeGetAll gets all metandoes registered to cluster
func (ms *ClusterKvStateMachine) MetaNodeGetAll(raftGroupID uint64) ([]*vp.MetaNode, error) {
	var v []btreeinstance.MetaNodeKV

	ms.metaNodeData.Ascend(func(a btree.Item) bool {
		v = append(v, a.(btreeinstance.MetaNodeKV))
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

//MetaNodeGetRange gets all metanodes with key not less than minKey
func (ms *ClusterKvStateMachine) MetaNodeGetRange(raftGroupID uint64, minKey uint64) ([]*vp.MetaNode, error) {
	var v []btreeinstance.MetaNodeKV

	var itemMin btreeinstance.MetaNodeKV
	itemMin.K = minKey

	ms.dataNodeData.AscendGreaterOrEqual(itemMin, func(a btree.Item) bool {
		v = append(v, a.(btreeinstance.MetaNodeKV))
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

//MetaNodeGet gets metanode info by key
func (ms *ClusterKvStateMachine) MetaNodeGet(raftGroupID uint64, key uint64) (*vp.MetaNode, error) {
	var item btreeinstance.MetaNodeKV
	item.K = key
	newItem := ms.metaNodeData.Get(item)

	if newItem == nil {
		return nil, errNotExists
	}

	metaNode := &vp.MetaNode{}
	if err := pbproto.Unmarshal(newItem.(btreeinstance.MetaNodeKV).V, metaNode); err != nil {
		return nil, err
	}
	return metaNode, nil
}

//MetaNodeSet sets metanode kv
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

//DelMetaNode removes metanode from metanode kv by key
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

//MetaNodeRGGet gets metanode raftgroup by key
func (ms *ClusterKvStateMachine) MetaNodeRGGet(key uint64) (*vp.MetaNodeRG, error) {
	var item btreeinstance.MNRGKV
	item.K = key
	newItem := ms.mnrgData.Get(item)
	if newItem == nil {
		return nil, errNotExists
	}

	metaNodeRG := &vp.MetaNodeRG{}
	err := pbproto.Unmarshal(newItem.(btreeinstance.MNRGKV).V, metaNodeRG)
	if err != nil {
		return nil, err
	}
	return metaNodeRG, nil
}

//MetaNodeRGSet sets metanode kv
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

//MetaNodeRGGetRange gets all metanodes from kv with key not less than minKey
func (ms *ClusterKvStateMachine) MetaNodeRGGetRange(minKey uint64) ([]*vp.MetaNodeRG, error) {
	var v []btreeinstance.MNRGKV

	var itemMin btreeinstance.MNRGKV
	itemMin.K = minKey

	ms.mnrgData.AscendGreaterOrEqual(itemMin, func(a btree.Item) bool {
		v = append(v, a.(btreeinstance.MNRGKV))
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

//MetaNodeRGGetAll gets all metanode raftgroup
func (ms *ClusterKvStateMachine) MetaNodeRGGetAll() ([]*vp.MetaNodeRG, error) {
	logger.Debug("MetaNodeRGGetAll")
	var v []btreeinstance.MNRGKV

	ms.mnrgData.Ascend(func(a btree.Item) bool {
		v = append(v, a.(btreeinstance.MNRGKV))
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

//DelMetaNodeRG removes metanode raftgroup from kv by key
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

//AddNode adds an volmgr raft node
func (ms *ClusterKvStateMachine) AddNode(peer proto.Peer) error {
	resp := ms.raft.ChangeMember(1, proto.ConfAddNode, peer, nil)
	_, err := resp.Response()
	if err != nil {
		return errors.New("AddNode error")
	}
	return nil
}

//RemoveNode removes volmgr raft node by peer
func (ms *ClusterKvStateMachine) RemoveNode(peer proto.Peer) error {
	resp := ms.raft.ChangeMember(1, proto.ConfRemoveNode, peer, nil)
	_, err := resp.Response()
	if err != nil {
		return errors.New("RemoveNode error")
	}
	return nil
}

//ApplyMemberChange applies member changed raft request
func (ms *ClusterKvStateMachine) ApplyMemberChange(confChange *proto.ConfChange, index uint64) (interface{}, error) {
	return nil, nil
}

//HandleLeaderChange handles leader changed request
func (ms *ClusterKvStateMachine) HandleLeaderChange(leader uint64) {
}

//HandleFatalEvent handle fatal event request
func (ms *ClusterKvStateMachine) HandleFatalEvent(err *raft.FatalError) {
	panic(err.Err)
}

//setApplied sets applied id
func (ms *ClusterKvStateMachine) setApplied(index uint64) {
	ms.applied = index
}

//Snapshot takes snapshot of kv in mem
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
	ss.snap[2] = &snapItem{tree: ms.dataNodeBGData, curItem: ms.dataNodeBGData.Min(), opt: OPT_SET_DATANODEBG}
	ss.snap[3] = &snapItem{tree: ms.metaNodeData, curItem: ms.metaNodeData.Min(), opt: OPT_SET_METANODE}
	ss.snap[4] = &snapItem{tree: ms.blockGroupData, curItem: ms.blockGroupData.Min(), opt: OPT_SET_BG}
	ss.snap[5] = &snapItem{tree: ms.mnrgData, curItem: ms.mnrgData.Min(), opt: OPT_SET_MNRG}

	return ss, nil

}

//ApplySnapshot applies snapshot request
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
			case OPT_SET_BG:
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

//ClusterKvSnapShot is an uitility tool for taking kv snapshot
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

//snapItem is an uitility tool from snapshot
type snapItem struct {
	tree    *btree.BTree
	curItem btree.Item
	opt     uint32
}

//tmplog is an utility function for logging sanpshot
func tmplog(v string) {
	logger.Debug("Next:", v)
}

//Next implements interface of snapshot
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

		if data, err = pbproto.Marshal(kv); err != nil {
			return nil, err
		}

		return data, err
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

//KV interface for snapshot
type KV interface {
	Key() string
	Value() []byte
}

//TakeClusterKvSnapShot ...
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
		if data, err = json.Marshal(a.(btreeinstance.VOLKV)); err != nil {
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
		if data, err = json.Marshal(a.(btreeinstance.DataNodeKV)); err != nil {
			return false
		}
		w.Write(data)
		w.WriteByte('\n')
		w.Flush()
		return true
	})
	f.Close()

	fpath = path.Join(dir, "dataNodeBGData")
	if f, err = os.OpenFile(fpath, flag, perm); err != nil {
		return err
	}
	w = bufio.NewWriter(f)
	ms.dataNodeBGData.Ascend(func(a btree.Item) bool {
		if data, err = json.Marshal(a.(btreeinstance.DataNodeBGKV)); err != nil {
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
		if data, err = json.Marshal(a.(btreeinstance.MetaNodeKV)); err != nil {
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
		if data, err = json.Marshal(a.(btreeinstance.BlockGroupKV)); err != nil {
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
		if data, err = json.Marshal(a.(btreeinstance.MNRGKV)); err != nil {
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
		logger.Error("TakeKvSnapShot Truncate failed index : %v , err :%v", ms.applied, err)
		return err
	}

	return nil
}

//LoadClusterKvSnapShot ...
func LoadClusterKvSnapShot(ms *ClusterKvStateMachine, dir string) (uint64, error) {

	var fpath string

	fpath = path.Join(dir, "applied")
	f, err := os.Open(fpath)
	if err != nil {
		return 0, nil
	}
	data, err := ioutil.ReadAll(f)
	ms.applied, err = strconv.ParseUint(string(data), 10, 64)
	logger.Debug("ms.applied %v", ms.applied)
	f.Close()

	fpath = path.Join(dir, "rgid")
	f, err = os.Open(fpath)
	if err != nil {
		return 0, nil
	}
	data, err = ioutil.ReadAll(f)
	ms.rgID, err = strconv.ParseUint(string(data), 10, 64)
	logger.Debug("ms.rgID %v", ms.rgID)
	f.Close()

	fpath = path.Join(dir, "bgid")
	f, err = os.Open(fpath)
	if err != nil {
		return 0, nil
	}
	data, err = ioutil.ReadAll(f)
	ms.bgID, err = strconv.ParseUint(string(data), 10, 64)
	logger.Debug("ms.bgID %v", ms.bgID)
	f.Close()

	fpath = path.Join(dir, "volData")
	if f, err = os.Open(fpath); err != nil {
		return 0, nil
	}
	buf := bufio.NewReader(f)

	var vol btreeinstance.VOLKV
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
		logger.Debug("vol %v", vol)
		ms.volData.ReplaceOrInsert(vol)
	}
	f.Close()

	fpath = path.Join(dir, "dataNodeData")
	if f, err = os.Open(fpath); err != nil {
		return 0, nil
	}
	buf = bufio.NewReader(f)

	var dataNode btreeinstance.DataNodeKV
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
		logger.Debug("dataNode %v", dataNode)
		ms.dataNodeData.ReplaceOrInsert(dataNode)
	}
	f.Close()

	fpath = path.Join(dir, "dataNodeBGData")
	if f, err = os.Open(fpath); err != nil {
		return 0, nil
	}
	buf = bufio.NewReader(f)

	var dataNodeBG btreeinstance.DataNodeBGKV
	for {
		line, err := buf.ReadBytes('\n')
		if err != nil {
			if err == io.EOF {
				break
			}
			f.Close()
			return 0, err
		}
		err = json.Unmarshal(line, &dataNodeBG)
		if err != nil {
			f.Close()
			return 0, err
		}
		logger.Debug("dataNodeBG %v", dataNodeBG)
		ms.dataNodeBGData.ReplaceOrInsert(dataNodeBG)
	}
	f.Close()

	fpath = path.Join(dir, "metaNodeData")
	if f, err = os.Open(fpath); err != nil {
		return 0, nil
	}
	buf = bufio.NewReader(f)
	var metaNode btreeinstance.MetaNodeKV
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
		logger.Debug("metaNode %v", metaNode)
		ms.metaNodeData.ReplaceOrInsert(metaNode)
	}
	f.Close()

	fpath = path.Join(dir, "blockGroupData")
	if f, err = os.Open(fpath); err != nil {
		return 0, nil
	}
	buf = bufio.NewReader(f)

	var blockGroup btreeinstance.BlockGroupKV
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
		logger.Debug("blockGroup %v", blockGroup)
		ms.blockGroupData.ReplaceOrInsert(blockGroup)
	}
	f.Close()

	fpath = path.Join(dir, "mnrgData")
	if f, err = os.Open(fpath); err != nil {
		return 0, nil
	}
	buf = bufio.NewReader(f)

	var mnrg btreeinstance.MNRGKV
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
		logger.Debug("mnrg %v", mnrg)
		ms.mnrgData.ReplaceOrInsert(mnrg)
	}
	f.Close()

	logger.Debug("ms.applied end %v", ms.applied)

	return ms.applied, nil

}

////////////////////////////////////////////////////////////////////////////

//ClusterAddrDatabase is a map from nodeid to address
var ClusterAddrDatabase = make(map[uint64]*common.Address)

//AddInit initializes ClusterAddrDatabase
func AddInit(ips []string) {
	fmt.Println("IPS:")
	for i := range ips {
		ClusterAddrDatabase[uint64(i+1)] = &common.Address{
			Heartbeat: fmt.Sprintf("%s:77%d1", ips[i], i),
			Replicate: fmt.Sprintf("%s:77%d2", ips[i], i),
			Grpc:      fmt.Sprintf("%s:77%d3", ips[i], i),
			Pprof:     fmt.Sprintf("%s:77%d4", ips[i], i),
		}
		fmt.Println(ClusterAddrDatabase[uint64(i+1)])
	}
}

//ClusterResolver implements address resolving interface
type ClusterResolver struct {
	nodes map[uint64]struct{}
	sync.Mutex
}

//NewClusterResolver creates a new ClusterResolver
func NewClusterResolver() *ClusterResolver {
	return &ClusterResolver{nodes: make(map[uint64]struct{})}
}

//AddNode adds new node
func (r *ClusterResolver) AddNode(nodeID uint64, addr *common.Address) {
	r.Lock()
	r.nodes[nodeID] = struct{}{}
	r.Unlock()
}

//RemoveNode removes node by node id
func (r *ClusterResolver) RemoveNode(nodeID uint64, addr *common.Address) {
	r.Lock()
	delete(r.nodes, nodeID)
	r.Unlock()
}

//AllNodes gets all nodes
func (r *ClusterResolver) AllNodes() (all []uint64) {
	r.Lock()
	for k := range r.nodes {
		all = append(all, k)
	}
	r.Unlock()
	return
}

//NodeAddress gets node address by node id
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
