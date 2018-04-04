package btreeinstance

import (
	"github.com/google/btree"
	"strconv"
)

type Uint64 uint64

// Less returns true if uint64(a) < uint64(b).
func (a Uint64) Less(b btree.Item) bool {
	return a < b.(Uint64)
}

type String string

// Less returns true if int(a) < int(b).
func (a String) Less(b btree.Item) bool {
	return a < b.(String)
}

type DentryKV struct {
	K string
	V []byte
}

func (a DentryKV) Less(b btree.Item) bool {
	return a.K < b.(DentryKV).K
}

type InodeKV struct {
	K uint64
	V []byte
}

func (a InodeKV) Less(b btree.Item) bool {
	return a.K < b.(InodeKV).K
}

type BGKV struct {
	K uint64
	V []byte
}

func (a BGKV) Less(b btree.Item) bool {
	return a.K < b.(BGKV).K
}

// -------- Cluster btrees ---------------

//for datanode
type DataNodeKV struct {
	K string
	V []byte
}

func (a DataNodeKV) Less(b btree.Item) bool {
	return a.K < b.(DataNodeKV).K
}

//for datanodebgp
type DataNodeBGPKV struct {
	K string
	V []byte
}

func (a DataNodeBGPKV) Less(b btree.Item) bool {
	return a.K < b.(DataNodeBGPKV).K
}

//for meatanode
type MetaNodeKV struct {
	K uint64
	V []byte
}

func (a MetaNodeKV) Less(b btree.Item) bool {
	return a.K < b.(MetaNodeKV).K
}

//for volume blockgroup
type BlockGroupKV struct {
	K uint64
	V []byte
}

func (a BlockGroupKV) Less(b btree.Item) bool {
	return a.K < b.(BlockGroupKV).K
}

//for MetaNode RaftGroup
type MNRGKV struct {
	K uint64
	V []byte
}

func (a MNRGKV) Less(b btree.Item) bool {
	return a.K < b.(MNRGKV).K
}

//for volume info
type VOLKV struct {
	K string
	V []byte
}

func (a VOLKV) Less(b btree.Item) bool {
	return a.K < b.(VOLKV).K
}

//to implement kv interface for snapshot
func (a DataNodeKV) Key() string {
	return a.K
}
func (a DataNodeKV) Value() []byte {
	return a.V
}

func (a DataNodeBGPKV) Key() string {
	return a.Key()
}
func (a DataNodeBGPKV) Value() []byte {
	return a.V
}

func (a MetaNodeKV) Key() string {
	return strconv.FormatUint(a.K, 10)
}
func (a MetaNodeKV) Value() []byte {
	return a.V
}

func (a BlockGroupKV) Key() string {
	return strconv.FormatUint(a.K, 10)
}
func (a BlockGroupKV) Value() []byte {
	return a.V
}

func (a MNRGKV) Key() string {
	return strconv.FormatUint(a.K, 10)
}
func (a MNRGKV) Value() []byte {
	return a.V
}

func (a VOLKV) Key() string {
	return a.K
}
func (a VOLKV) Value() []byte {
	return a.V
}
