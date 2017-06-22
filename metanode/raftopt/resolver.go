package raftopt

import (
	"errors"
	"jd.com/sharkstore/raft"
	"sync"
)

type Resolver struct {
	nodes map[uint64]struct{}
	sync.Mutex
}

func NewResolver() *Resolver {
	return &Resolver{nodes: make(map[uint64]struct{})}
}

func (r *Resolver) AddNode(nodeID uint64) {
	r.Lock()
	r.nodes[nodeID] = struct{}{}
	r.Unlock()
}

func (r *Resolver) RemoveNode(nodeID uint64) {
	r.Lock()
	delete(r.nodes, nodeID)
	r.Unlock()
}

func (r *Resolver) AllNodes() (all []uint64) {
	r.Lock()
	for k := range r.nodes {
		all = append(all, k)
	}
	r.Unlock()
	return
}

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
