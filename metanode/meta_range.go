package metanode

import (
	"github.com/tiglabs/action_dev/fsm"
)

type MetaRange struct {
	id          string
	disk        *Disk
	store       *fsm.MetaRangeFsm
	peers       []string
	raftGroupId uint64
	status      int
}

func NewMetaRange(id string) (mr *MetaRange) {
	return &MetaRange{id: id}
}

func (mr *MetaRange) initStore(disk *Disk) (err error) {
	return
}
