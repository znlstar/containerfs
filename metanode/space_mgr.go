package metanode

import (
	"fmt"
	"google.golang.org/grpc/examples/route_guide/mock_routeguide"
	"sync"
)

type SpaceManager struct {
	disks      []*Disk
	metaRanges map[string]*MetaRange
	sync.Mutex
}

func NewSpaceManager(diskPaths []string) (space *SpaceManager) {
	disks := make([]*Disk, 0)
	for _, path := range diskPaths {
		d, err := NewDisk(path)
		if err != nil {
			gLog.LogError(fmt.Sprintf("action[%v] path[%v] error[%v]", ActionInitDisk, path, err.Error()))
			continue
		}
		disks = append(disks, d)
	}

	space = new(SpaceManager)
	space.disks = disks
	space.initMetaRange()

	return
}

func (space *SpaceManager) initMetaRange() (err error) {
	mrs := make([]*MetaRange, 0)
	var wg sync.WaitGroup
	var lock sync.Mutex
	for _, d := range space.disks {
		wg.Add(1)
		go d.LoadMetaRange(&wg, &mrs, &lock)
	}
	wg.Done()
	space.Lock()
	defer space.Unlock()
	for _, mr := range mrs {
		space.metaRanges[mr.id] = mr
	}

	return
}

func (space *SpaceManager) addMetaRange(mr *MetaRange) {

}

func (space *SpaceManager) chooseSuitDisk() (d *Disk) {
	return
}

func (space *SpaceManager) getMetaRange(id string) (mr *MetaRange) {
	return
}
