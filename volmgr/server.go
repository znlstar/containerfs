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
	"github.com/tiglabs/containerfs/raftopt/common"
	"github.com/tiglabs/raft"
	"github.com/tiglabs/raft/proto"
	"github.com/tiglabs/raft/storage/wal"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

type VolMgrServerAddr struct {
	Host   string
	NodeID uint64
	Peers  []proto.Peer
	Ips    []string
	Waldir string
	Log    string
}

type Cluster struct {
	sync.Mutex
	RaftGroup   *raftopt.ClusterKvStateMachine
	RaftStorage *wal.Storage
	IsLoaded    bool
}

type VolMgrServer struct {
	NodeID          uint64
	Addr            *common.Address
	Resolver        common.Resolver
	RaftServer      *raft.RaftServer
	Cluster         *Cluster
	wg              sync.WaitGroup
	BgStatusMap     map[uint64]int32
	BgStatusMapSync sync.Mutex
	volmgrAddr      *VolMgrServerAddr
	sync.Mutex
}

func NewVolMgrServer(volmgrAddr *VolMgrServerAddr) (vs *VolMgrServer, err error) {

	addr, ok := raftopt.ClusterAddrDatabase[volmgrAddr.NodeID]
	if !ok {
		logger.Error("NewVolMgrServer ClusterAddrDatabase get failed")
		return nil, fmt.Errorf("no such address info. nodeId: %d", volmgrAddr.NodeID)
	}

	err = common.StartRaftServer(&vs.RaftServer, vs.Resolver, addr, volmgrAddr.NodeID)
	if err != nil {
		logger.Error("NewVolMgrServer StartRaftServer failed err %v", err)
		return nil, fmt.Errorf("failed to start raft service %v", err)
	}

	r := raftopt.NewClusterResolver()
	for _, p := range volmgrAddr.Peers {
		r.AddNode(p.ID, nil)
	}

	vs = &VolMgrServer{NodeID: volmgrAddr.NodeID,
		Addr:        addr,
		Resolver:    r,
		BgStatusMap: make(map[uint64]int32),
		volmgrAddr:  volmgrAddr}

	vs.showLeaders()
	vs.startHealthCheck()

	return vs, nil
}

func (vs *VolMgrServer) Load() error {
	sm, sg, err := raftopt.CreateClusterKvStateMachine(vs.RaftServer, vs.volmgrAddr.Peers, vs.volmgrAddr.NodeID, vs.volmgrAddr.Waldir, "Cluster", 1)
	if err != nil {
		return err
	}
	vs.Cluster = &Cluster{RaftGroup: sm, RaftStorage: sg}
	logger.Debug("VolMgrServer.Load success...")
	return nil
}

func (vs *VolMgrServer) StartService() {

	lis, err := net.Listen("tcp", vs.Addr.Grpc)
	if err != nil {
		panic(fmt.Sprintf("Failed to listen on:%v", vs.Addr.Grpc))
	}
	s := grpc.NewServer()
	vp.RegisterVolMgrServer(s, vs)
	reflection.Register(s)
	if err := s.Serve(lis); err != nil {
		panic("Failed to serve")
	}
}

func (vs *VolMgrServer) showLeaders() {
	ticker := time.NewTicker(time.Second * 10)
	go func() {
		for range ticker.C {
			l, t := vs.RaftServer.LeaderTerm(1)
			logger.Debug("ShowLeaders RaftGroup LeaderID %v Term %v", l, t)
		}
	}()
}

func (vs *VolMgrServer) startHealthCheck() {
	td := time.NewTicker(time.Second * 1)
	go func() {
		for range td.C {
			if vs.RaftServer.IsLeader(1) {
				vs.DetectDataNodes()
			}
		}
	}()

	tm := time.NewTicker(time.Second * 1)
	go func() {
		for range tm.C {
			if vs.RaftServer.IsLeader(1) {
				vs.DetectMetaNodes()
			}
		}
	}()
}
