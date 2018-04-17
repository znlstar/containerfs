package fsm

import (
	"github.com/tiglabs/action_dev/prot"
	"github.com/tiglabs/action_dev/raftop"
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
