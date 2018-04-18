package fsm

import (
	"github.com/tiglabs/containerfs/proto"
	"github.com/tiglabs/containerfs/raftopt"
	"github.com/google/btree"
)

type MetaRangeFsm struct {
	Inodes *btree.BTree
	Dentry *btree.BTree
	raftopt.RaftStoreFsm
}

func (mr *MetaRangeFsm) Create(request *proto.CreateRequest) (response *proto.CreateResponse) {
	return
}

func (mr *MetaRangeFsm) Delete(request *proto.DeleteRequest) (response *proto.DeleteRsponse) {
	return
}

func (mr *MetaRangeFsm) OpenFile(request *proto.OpenFileRequest) (response *proto.OpenFileResponse) {
	return
}

func (mr *MetaRangeFsm) Rename(request *proto.RenameRequest) (response *proto.RenameResponse) {
	return
}

func (mr *MetaRangeFsm) List(request *proto.ListDirRequest) (response *proto.ListDirResponse) {
	return
}
