package stream

import (
	"github.com/tiglabs/containerfs/sdk"
	"sync"
)

type StreamClient struct {
	sync.Mutex
	wraper  *sdk.VolWraper
	extents []*ExtentWriter
	keys    chan string
}

func (sc *StreamClient) StreamWrite(data []byte, offset int64) (size int) {

	return
}
