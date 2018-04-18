package proto

import (
	"encoding/binary"
	"errors"
	"io"
	"net"
	"strconv"
	"time"
	"fmt"
	"sync/atomic"
)

var (
	ReqIDGlobal = int64(1)
)

func GetReqID() int64 {
	return atomic.AddInt64(&ReqIDGlobal, 1)
}


const (
	ExtentNameSplit = "_"
	VolNameSplit    = "_"
	AddrSplit       = "/"
	HeaderSize      = 44
	PkgArgMaxSize   = 100
)

//operations
const (
	ProtoMagic        uint8 = 0xFF
	OpCreateFile            = 0x01
	OpMarkDelete            = 0x02
	OpWrite                 = 0x03
	OpRead                  = 0x04
	OpStreamRead            = 0x05
	OpGetWatermark          = 0x06
	OpGetAllWatermark       = 0x07
	OpNotifyRepair          = 0x08
	OpERepairRead           = 0x09
	OpCRepairRead           = 0x0A
	OpFlowInfo              = 0x0B
	OpSyncDelNeedle         = 0x0C
	OpNotifyCompact         = 0x0D

	OpRename = 0x0E
	OpOpen   = 0x0F
	OpCreate = 0x10
	OpDelete = 0x11
	OpList   = 0x12


	OpIntraGroupNetErr uint8 = 0xF3
	OpArgUnmatchErr    uint8 = 0xF4
	OpFileNotExistErr  uint8 = 0xF5
	OpDiskNoSpaceErr   uint8 = 0xF6
	OpDiskErr          uint8 = 0xF7
	OpErr              uint8 = 0xF8
	OpAgain            uint8 = 0xF9

	OpOk uint8 = 0xFA
)


const (
	WriteDeadlineTime=5
	NoReadDeadlineTime=-1
)

const (
	TinyStoreMode   = 0
	ExtentStoreMode = 1
)

type Packet struct {
	Magic     uint8
	StoreType uint8
	Opcode    uint8
	Nodes     uint8
	Crc       uint32
	Size      uint32
	Arglen    uint32
	VolID     uint32
	FileID    uint64
	Offset    int64
	ReqID     int64
	Arg       []byte //if create or append ops, data contains addrs
	Data      []byte
	StartT    int64
	OrgOpcode uint8
}

func NewPacket() *Packet {
	p := new(Packet)
	p.Magic = ProtoMagic
	p.StartT = time.Now().UnixNano()

	return p
}

func GetOpMesg(opcode uint8) (m string) {
	switch opcode {
	case OpCreateFile:
		m = "CreateFile"
	case OpMarkDelete:
		m = "MarkDelete"
	case OpWrite:
		m = "Write"
	case OpRead:
		m = "Read"
	case OpStreamRead:
		m = "StreamRead"
	case OpGetWatermark:
		m = "GetWatermark"
	case OpGetAllWatermark:
		m = "GetAllWatermark"
	case OpNotifyRepair:
		m = "NotifyRepair"
	case OpCRepairRead:
		m = "ChunkRepairRead"
	case OpNotifyCompact:
		m = "NotifyCompact"
	case OpERepairRead:
		m = "ExtentRepairRead"
	case OpFlowInfo:
		m = "FlowInfo"
	case OpIntraGroupNetErr:
		m = "IntraGroupNetErr"
	case OpArgUnmatchErr:
		m = "ArgUnmatchErr"
	case OpFileNotExistErr:
		m = "FileNotExistErr"
	case OpDiskNoSpaceErr:
		m = "DiskNoSpaceErr"
	case OpDiskErr:
		m = "DiskErr"
	case OpErr:
		m = "Err"
	case OpAgain:
		m = "Again"
	case OpOk:
		m = "Ok"
	case OpSyncDelNeedle:
		m = "OpSyncHasDelNeedle"
	default:
		return ""

	}
	return
}


func (p *Packet) MarshalHeader(out []byte) {
	out[0] = p.Magic
	out[1] = p.StoreType
	out[2] = p.Opcode
	out[3] = p.Nodes
	binary.BigEndian.PutUint32(out[4:8], p.Crc)
	binary.BigEndian.PutUint32(out[8:12], p.Size)
	binary.BigEndian.PutUint32(out[12:16], p.Arglen)
	binary.BigEndian.PutUint32(out[16:20], p.VolID)
	binary.BigEndian.PutUint64(out[20:28], p.FileID)
	binary.BigEndian.PutUint64(out[28:36], uint64(p.Offset))
	binary.BigEndian.PutUint64(out[36:HeaderSize], uint64(p.ReqID))
	p.OrgOpcode = p.Opcode

	return
}

func (p *Packet) UnmarshalHeader(in []byte) error {
	p.Magic = in[0]
	if p.Magic != ProtoMagic {
		return errors.New("Bad Magic " + strconv.Itoa(int(p.Magic)))
	}

	p.StoreType = in[1]
	p.Opcode = in[2]
	p.Nodes = in[3]
	p.Crc = binary.BigEndian.Uint32(in[4:8])
	p.Size = binary.BigEndian.Uint32(in[8:12])
	p.Arglen = binary.BigEndian.Uint32(in[12:16])
	p.VolID = binary.BigEndian.Uint32(in[16:20])
	p.FileID = binary.BigEndian.Uint64(in[20:28])
	p.Offset = int64(binary.BigEndian.Uint64(in[28:36]))
	p.ReqID = int64(binary.BigEndian.Uint64(in[36:HeaderSize]))
	p.OrgOpcode = p.Opcode

	return nil
}

func (p *Packet) WriteToNoDeadLineConn(c net.Conn, freeBody bool) (err error) {
	header:=make([]byte,HeaderSize)

	p.MarshalHeader(header)
	if _, err = c.Write(header); err == nil {
		if _, err = c.Write(p.Arg[:int(p.Arglen)]); err == nil {
			if p.Data != nil {
				_, err = c.Write(p.Data[:p.Size])
			}
		}
	}

	return
}

func (p *Packet) WriteToConn(c net.Conn, freeBody bool) (err error) {
	c.SetWriteDeadline(time.Now().Add(WriteDeadlineTime * time.Second))
	header:=make([]byte,HeaderSize)

	p.MarshalHeader(header)
	if _, err = c.Write(header); err == nil {
		if _, err = c.Write(p.Arg[:int(p.Arglen)]); err == nil {
			if p.Data != nil {
				_, err = c.Write(p.Data[:p.Size])
			}
		}
	}

	return
}

func (p *Packet) WriteHeaderToConn(c net.Conn) (err error) {
	header:=make([]byte,HeaderSize)
	p.MarshalHeader(header)
	_, err = c.Write(header)

	return
}

func ReadFull(c net.Conn, buf *[]byte, readSize int) (err error) {
	*buf = make([]byte, readSize)
	_, err = io.ReadFull(c, (*buf)[:readSize])

	return
}

func (p *Packet) ReadFromConn(c net.Conn, deadlineTime time.Duration, dataSize int64) (err error) {
	if deadlineTime != NoReadDeadlineTime {
		c.SetReadDeadline(time.Now().Add(deadlineTime * time.Second))
	}
	header:=make([]byte,HeaderSize)

	if _, err = io.ReadFull(c, header); err != nil {
		return
	}
	if err = p.UnmarshalHeader(header); err != nil {
		return
	}

	if p.Arglen > 0 {
		if err = ReadFull(c, &p.Arg, int(p.Arglen)); err != nil {
			return
		}
	}

	if p.Size < 0 {
		return
	}
	size:=p.Size
	if p.Opcode==OpRead || p.Opcode==OpStreamRead{
		size=0
	}
	return ReadFull(c, &p.Data, int(size))
}




func (p *Packet) PackOkReply() {
	p.Opcode = OpOk
	p.Size = 0
	p.Arglen = 0
}

func (p *Packet) PackOkReadReply() {
	p.Opcode = OpOk
	p.Arglen = 0
}

func (p *Packet) PackOkGetWatermarkReply(size int64) {
	p.Offset = size
	p.Size = 0
	p.Opcode = OpOk
	p.Arglen = 0
}

func (p *Packet) PackOkGetInfoReply(buf []byte) {
	p.Size = uint32(len(buf))
	p.Data = make([]byte, p.Size)
	copy(p.Data[:p.Size], buf)
	p.Opcode = OpOk
	p.Arglen = 0
}

func (p *Packet) GetUniqLogId() (m string) {
	m = fmt.Sprintf("%v_%v_%v_%v_%v", p.ReqID, p.VolID, p.FileID, p.Offset, GetOpMesg(p.OrgOpcode))

	return
}

