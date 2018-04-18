package datanode

import (
	"github.com/tiglabs/containerfs/proto"
	"net"
)

type Packet struct {
	proto.Packet
	goal uint8
	nextConn net.Conn
	nextAddr string
	allAddr []string
	isReturn bool
}
