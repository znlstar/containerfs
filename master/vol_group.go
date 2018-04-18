package master

import "sync"

type Vol struct {
	addr   string
	loc    uint8
	status uint8
}

type VolGroup struct {
	groupId          uint64
	volType          string
	goal             uint8
	status           uint8
	location         []*Vol
	PersistenceHosts []string
	sync.Mutex
}

func (vg *VolGroup) addMember(v *Vol) {

}

func (vg *VolGroup) checkBadStatus() {

}
