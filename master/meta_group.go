package master

import (
	"fmt"
)

type MetaRange struct {
	start uint64
	end   uint64
	addr  string
}

type MetaGroup struct {
	groupID string
	start   uint64
	end     uint64
	members []*MetaRange
}

func NewMetaRange(start, end uint64, addr string) (mr *MetaRange) {
	mr = &MetaRange{start: start, end: end, addr: addr}
	return
}

func NewMetaGroup(start, end uint64) (mg *MetaGroup, err error) {
	groupID := fmt.Sprintf("%v_%v", start, end)
	mg = &MetaGroup{groupID: groupID, start: start, end: end, members: make([]*MetaRange, 0)}

	return
}

func (mg *MetaGroup) AddMember(mr *MetaRange) (err error) {

	return
}
