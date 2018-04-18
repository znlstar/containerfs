package metanode

import (
	"github.com/tiglabs/containerfs/fsm"
	"github.com/tiglabs/containerfs/proto"
	"encoding/json"
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

func (mr *MetaRange) operatorPkg(p *Packet) {
	var (
		body []byte
		err  error
	)
	switch p.Opcode {
	case proto.OpCreate:
		request := new(proto.CreateRequest)
		if err = json.Unmarshal(p.Data, request); err != nil {

		}
		response := mr.store.Create(request)
		body, err = json.Marshal(response)
		if err != nil {

		}

	}

}