// Copyright (c) 2017, tig.jd.com. All rights reserved.
// Use of this source code is governed by a Apache License 2.0 that can be found in the LICENSE file.

package btreeinstance

import (
	"github.com/google/btree"
	"strconv"
)

//Uint64 ...
type Uint64 uint64

//Less
func (a Uint64) Less(b btree.Item) bool {
	return a < b.(Uint64)
}

//String ...
type String string

// Less returns true if int(a) < int(b).
func (a String) Less(b btree.Item) bool {
	return a < b.(String)
}

//DentryKV ...
type DentryKV struct {
	K string
	V []byte
}

//Less ...
func (a DentryKV) Less(b btree.Item) bool {
	return a.K < b.(DentryKV).K
}

//InodeKV ...
type InodeKV struct {
	K uint64
	V []byte
}

//Less ...
func (a InodeKV) Less(b btree.Item) bool {
	return a.K < b.(InodeKV).K
}

//BGKV ...
type BGKV struct {
	K uint64
	V []byte
}

//Less ...
func (a BGKV) Less(b btree.Item) bool {
	return a.K < b.(BGKV).K
}

// -------- Cluster btrees ---------------

//DataNodeKV ...
type DataNodeKV struct {
	K string
	V []byte
}

//Less ...
func (a DataNodeKV) Less(b btree.Item) bool {
	return a.K < b.(DataNodeKV).K
}

//DataNodeBGKV ...
type DataNodeBGKV struct {
	K string
	V []byte
}

//Less ...
func (a DataNodeBGKV) Less(b btree.Item) bool {
	return a.K < b.(DataNodeBGKV).K
}

//MetaNodeKV ...
type MetaNodeKV struct {
	K uint64
	V []byte
}

//Less ...
func (a MetaNodeKV) Less(b btree.Item) bool {
	return a.K < b.(MetaNodeKV).K
}

//BlockGroupKV ...
type BlockGroupKV struct {
	K uint64
	V []byte
}

//Less ...
func (a BlockGroupKV) Less(b btree.Item) bool {
	return a.K < b.(BlockGroupKV).K
}

//MNRGKV ...
type MNRGKV struct {
	K uint64
	V []byte
}

//Less ...
func (a MNRGKV) Less(b btree.Item) bool {
	return a.K < b.(MNRGKV).K
}

//VOLKV ...
type VOLKV struct {
	K string
	V []byte
}

//Less ...
func (a VOLKV) Less(b btree.Item) bool {
	return a.K < b.(VOLKV).K
}

//Key ...
func (a DataNodeKV) Key() string {
	return a.K
}

//Value ...
func (a DataNodeKV) Value() []byte {
	return a.V
}

//Key ...
func (a DataNodeBGKV) Key() string {
	return a.Key()
}

//Value ...
func (a DataNodeBGKV) Value() []byte {
	return a.V
}

//Key ...
func (a MetaNodeKV) Key() string {
	return strconv.FormatUint(a.K, 10)
}

//Value ...
func (a MetaNodeKV) Value() []byte {
	return a.V
}

//Key ...
func (a BlockGroupKV) Key() string {
	return strconv.FormatUint(a.K, 10)
}

//Value ...
func (a BlockGroupKV) Value() []byte {
	return a.V
}

//Key ...
func (a MNRGKV) Key() string {
	return strconv.FormatUint(a.K, 10)
}

//Value ...
func (a MNRGKV) Value() []byte {
	return a.V
}

//Key ...
func (a VOLKV) Key() string {
	return a.K
}

//Value ...
func (a VOLKV) Value() []byte {
	return a.V
}
