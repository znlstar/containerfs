package master

import "sync"

type NameSpace struct {
	name       string
	metaGroups []*MetaGroup
	sync.Mutex
}

func NewNameSpace(name string) (ns *NameSpace) {
	return &NameSpace{name: name, metaGroups: make([]*MetaGroup, 0)}
}

func (ns *NameSpace) AddMetaGroup(mg *MetaGroup) {
	exsit := false
	for _, omg := range ns.metaGroups {
		if omg.start == mg.start && omg.end == mg.end {
			exsit = true
		}
	}
	if !exsit {
		ns.metaGroups = append(ns.metaGroups, mg)
	}
}

func (ns *NameSpace) GetMetaGroup(inode uint64) (mg *MetaGroup) {
	for _, mg = range ns.metaGroups {
		if mg.start >= inode && mg.end < inode {
			return mg
		}
	}

	return nil
}
