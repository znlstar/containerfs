package stream

import (
	"container/list"
)

const (
	CFSBLOCKSIZE       = 0x1000
	CFSEXTENTSIZE      = 0x04000000
	CFSBLOCKBITS       = 16
	CFSCHUNKMASK       = 0x03FFFFFF
	MaxPacketPerExtent = 1024
)

type ExtentWriter struct {
	original     list.List
	sendList     list.List
	volId        uint64
	extentId     uint64
	packet       *Packet
	queue        chan *Packet
	serialNumber int
	packetSend   int
	packetAck    int
}

func (ew *ExtentWriter)(data []byte, offset int) {
	total, write := 0
	length := len(data)
	for total < length && !ew.isFull() {
		if ew.currPkg == nil {
			ew.currPkg =
		}
	}

}

func (ew *ExtentWriter) isFull() (bool) {
	return ew.packetSend == MaxPacketPerExtent
}
