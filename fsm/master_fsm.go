package fsm

import (
	"github.com/tiglabs/containerfs/proto"
	"github.com/tiglabs/containerfs/raftopt"
)

/*
  this struct is used for master need persites store
*/

type MasterFsm struct {
	raftopt.RaftStoreFsm
}

func (mf *MasterFsm) CreateNameSpace(request proto.CreateNameSpaceRequest) (response proto.CreateNameSpaceResponse) {
	return
}

func (mf *MasterFsm) CreateMetaRange(request proto.CreateMetaRangeRequst) (response proto.CreateMetaRangeResponse) {
	return
}
