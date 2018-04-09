//package main
package volmgr

import (
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/tiglabs/containerfs/logger"
	"github.com/tiglabs/containerfs/proto/vp"
	"github.com/tiglabs/containerfs/raftopt"
	com "github.com/tiglabs/containerfs/raftopt/common"
	"github.com/tiglabs/raft"
	"github.com/tiglabs/raft/proto"
	"github.com/tiglabs/raft/storage/wal"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

var errNotLeader = fmt.Errorf("not leader")

type VolMgrServerAddr struct {
	Host   string
	NodeID uint64
	Peers  []proto.Peer
	Ips    []string
	Waldir string
	Log    string
}

var va VolMgrServerAddr

type Cluster struct {
	sync.Mutex
	RaftGroup   *raftopt.ClusterKvStateMachine
	RaftStorage *wal.Storage
	IsLoaded    bool
}

type VolMgrServer struct {
	NodeID          uint64
	Addr            *com.Address
	Resolver        com.Resolver
	RaftServer      *raft.RaftServer
	Cluster         *Cluster
	wg              sync.WaitGroup
	BgStatusMap     map[uint64]int32
	BgStatusMapSync sync.Mutex
	va              *VolMgrServerAddr
	sync.Mutex
}

func NewVolMgrServer(va *VolMgrServerAddr) (vs *VolMgrServer, err error) {

	addr, ok := raftopt.ClusterAddrDatabase[va.NodeID]
	if !ok {
		return nil, fmt.Errorf("no such address info. nodeId: %d", va.NodeID)
	}

	err = com.StartRaftServer(&vs.RaftServer, vs.Resolver, addr, va.NodeID)
	if err != nil {
		logger.Error("StartRaftServer failed ...")
		return nil, fmt.Errorf("failed to start raft service %v", err)
	}

	r := raftopt.NewClusterResolver()
	for _, p := range va.Peers {
		r.AddNode(p.ID, nil)
	}

	vs = &VolMgrServer{NodeID: va.NodeID,
		Addr:        addr,
		Resolver:    r,
		BgStatusMap: make(map[uint64]int32),
		va:          va}
	return vs, nil
}

func (vs *VolMgrServer) Load() error {
	sm, sg, err := raftopt.CreateClusterKvStateMachine(vs.RaftServer, vs.va.Peers, vs.va.NodeID, vs.va.Waldir, "Cluster", 1)
	if err != nil {
		return err
	}
	vs.Cluster = &Cluster{RaftGroup: sm, RaftStorage: sg}
	logger.Debug("VolMgrServer.load success...")
	return nil
}

func (vs *VolMgrServer) StartService() {

	lis, err := net.Listen("tcp", vs.Addr.Grpc)
	if err != nil {
		panic(fmt.Sprintf("Failed to listen on:%v", vs.Addr.Grpc))
	}
	s := grpc.NewServer()
	vp.RegisterVolMgrServer(s, vs)
	// Register reflection service on gRPC server.
	reflection.Register(s)
	if err := s.Serve(lis); err != nil {
		panic("Failed to serve")
	}
}

func (s *VolMgrServer) ShowLeaders() {
	ticker := time.NewTicker(time.Second * 10)
	go func() {
		for range ticker.C {
			l, t := s.RaftServer.LeaderTerm(1)
			logger.Debug("--------- RaftGroup LeaderID %v Term %v ---------", l, t)
		}
	}()
}

func (s *VolMgrServer) StartHealthCheck() {
	td := time.NewTicker(time.Second * 1)
	go func() {
		for range td.C {
			if s.RaftServer.IsLeader(1) {
				s.DetectDataNodes()
			}
		}
	}()

	tm := time.NewTicker(time.Second * 1)
	go func() {
		for range tm.C {
			if s.RaftServer.IsLeader(1) {
				s.DetectMetaNodes()
			}
		}
	}()
}
