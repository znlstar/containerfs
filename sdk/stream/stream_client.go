package stream

import (
	"github.com/tiglabs/containerfs/sdk"
	"io"
	"container/list"
	"sync"
)

type StreamClient struct {
	dataList list.Element
	sync.Mutex
	wraper *sdk.VolWraper
}


func (sc *StreamClient)StreamWrite(writer io.ReadWriter)(reader io.Reader,err error){

	return
}

func (sc *StreamClient)StreamRead(writer io.Writer)(reader io.Reader,err error){

	return
}