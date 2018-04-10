package common

import (
	"time"

	"github.com/tiglabs/containerfs/logger"
	"github.com/tiglabs/raft"
)

// Address ...
type Address struct {
	Grpc      string // 9901
	Heartbeat string // 9902
	Replicate string // 9903
	Pprof     string // 9904
}

// Resolver interface
type Resolver interface {
	AddNode(uint64, *Address)
	RemoveNode(uint64, *Address)
	AllNodes() []uint64
	NodeAddress(uint64, raft.SocketType) (string, error)
}

// StartRaftServer ...
func StartRaftServer(rs **raft.RaftServer, r Resolver, addr *Address, nodeid uint64) error {

	var err error
	//  new raft server
	c := raft.DefaultConfig()
	c.TickInterval = time.Millisecond * 200
	c.NodeID = nodeid
	c.Resolver = r
	c.HeartbeatAddr = addr.Heartbeat
	c.ReplicateAddr = addr.Replicate
	*rs, err = raft.NewRaftServer(c)
	if err != nil {
		logger.Error("create raft server failed: %v", err)
		return err
	}
	return nil
}
