package sdk

import (
	"time"
	"sync"
)

type Vol struct {
	VolId uint64
	Goal  uint8
	Hosts []string
	Status uint8
}


const (
	VolViewUrl="/client/volview"
)

type VolWraper struct {
	MasterAddrs  []string
	ReadOnlyVols []*Vol
	ReadWriteVols []*Vol
	sync.RWMutex
}


func (wraper *VolWraper)Init(master []string) (err error){
	go wraper.update()
	return
}


func (wraper *VolWraper)update(){
	ticker:=time.NewTicker(time.Second*10)
	for {
		select {
			case <-ticker.C:
				wraper.getVols()
		}
	}
}


func (wraper *VolWraper)getVols()(err error){
	return
}


func (wraper *VolWraper)GetWriteVol()(v *Vol,err error){
	return
}

func (wraper *VolWraper)GetVol(volId uint64)(err error){
	return
}