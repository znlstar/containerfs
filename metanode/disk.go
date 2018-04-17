package metanode

import "sync"

type Disk struct {
	path           string
	total          int64
	free           int64
	metaRangeCount int
}

func NewDisk(path string) (d *Disk, err error) {
	return
}

func (d *Disk) LoadMetaRange(wg *sync.WaitGroup, mrs *[]*MetaRange, lock *sync.Mutex) {
	defer wg.Done()

	return
}
