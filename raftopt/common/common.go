// Copyright (c) 2017, tig.jd.com. All rights reserved.
// Use of this source code is governed by a Apache License 2.0 that can be found in the LICENSE file.

package common

import (
	"time"

	"github.com/tiglabs/containerfs/logger"
	"github.com/tiglabs/raft"
)

// Address is a wrapper of server address
type Address struct {
	Grpc      string // 9901
	Heartbeat string // 9902
	Replicate string // 9903
	Pprof     string // 9904
}

//Resolver defines an interface for resolving node addresses
type Resolver interface {
	AddNode(uint64, *Address)
	RemoveNode(uint64, *Address)
	AllNodes() []uint64
	NodeAddress(uint64, raft.SocketType) (string, error)
}

//StartRaftServer initializes and start up raft server
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
