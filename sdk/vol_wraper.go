package sdk

import (
	"time"
	"sync"
	"github.com/tiglabs/containerfs/util/pool"
	"net"
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
	ConnPool      *pool.ConnPool
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
				wraper.getVolsFromMaster()
		}
	}
}


func (wraper *VolWraper) getVolsFromMaster()(err error){
	return
}


func (wraper *VolWraper)GetWriteVol()(v *Vol,err error){
	return
}

func (wraper *VolWraper)GetVol(volId uint64)(err error){
	return
}

func (wraper *VolWraper)GetConnect(addr string)(conn net.Conn,err error){
	return
}

func (wraper *VolWraper)PutConnect(conn net.Conn){

}