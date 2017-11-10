package raftopt

import (
	"errors"
	"github.com/tigcode/raft"
	"sync"
)

//Resolver ...
type Resolver struct {
	nodes map[uint64]struct{}
	sync.Mutex
}

//NewResolver ...
func NewResolver() *Resolver {
	return &Resolver{nodes: make(map[uint64]struct{})}
}

//AddNode ...
func (r *Resolver) AddNode(nodeID uint64) {
	r.Lock()
	r.nodes[nodeID] = struct{}{}
	r.Unlock()
}

//RemoveNode ...
func (r *Resolver) RemoveNode(nodeID uint64) {
	r.Lock()
	delete(r.nodes, nodeID)
	r.Unlock()
}

//AllNodes ...
func (r *Resolver) AllNodes() (all []uint64) {
	r.Lock()
	for k := range r.nodes {
		all = append(all, k)
	}
	r.Unlock()
	return
}

//NodeAddress ...
func (r *Resolver) NodeAddress(nodeID uint64, stype raft.SocketType) (addr string, err error) {
	raddr, ok := AddrDatabase[nodeID]
	if !ok {
		return "", errors.New("no such node")
	}
	switch stype {
	case raft.HeartBeat:
		return raddr.Heartbeat, nil
	case raft.Replicate:
		return raddr.Replicate, nil
	default:
		return "", errors.New("unknown socket type")
	}
}
