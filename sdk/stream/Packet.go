package stream

import (
	"github.com/tiglabs/containerfs/proto"
	"github.com/tiglabs/containerfs/sdk"
)

type Packet struct {
	proto.Packet
	buffer []byte
	curr   uint32
}

func NewExtentWritePacket(vol *sdk.Vol, extentId uint64) (p *Packet) {
	p = new(Packet)
	p.VolID = vol.VolId
	p.Opcode = proto.OpWrite
	p.FileID = extentId
	var addrArgs string
	for i, host := range vol.Hosts {
		if i == len(vol.Hosts)-1 {
			addrArgs = addrArgs + host
		} else {
			addrArgs = addrArgs + host + proto.AddrSplit
		}
	}
	p.Arg = ([]byte)(addrArgs)
	p.Arglen = uint32(len(p.Arg))
	p.Size = 0
	p.StoreType = proto.TinyStoreMode
	p.ReqID =

	return
}

func (p *Packet) fill(data []byte) (canWrite bool) {

}
