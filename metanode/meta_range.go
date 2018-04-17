package metanode

import (
	"github.com/tiglabs/containerfs/fsm"
)

type MetaRange struct {
	id          string
	store       *fsm.MetaRangeFsm
	peers       []string
	raftGroupId uint64
	status      int
}

func NewMetaRange(id string) (mr *MetaRange) {
	return &MetaRange{id: id}
}


