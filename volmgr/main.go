package main

import (
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	_ "net/http/pprof"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/tiglabs/containerfs/logger"
	"github.com/tiglabs/containerfs/proto/vp"
	"github.com/tiglabs/containerfs/raftopt"
	com "github.com/tiglabs/containerfs/raftopt/common"
	"github.com/tiglabs/containerfs/utils"
	"github.com/tiglabs/raft"
	"github.com/tiglabs/raft/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

type addr struct {
	host   string
	nodeID uint64
	peers  []proto.Peer
	ips    []string
	waldir string
	log    string
}

const (
	BlkSizeG      = 5
	BlkSize       = 5 * 1024 * 1024 * 1024  /*one blksize 5G*/
	OneExpandSize = 30 * 1024 * 1024 * 1024 /*allocated volumesize 30G for each time*/
)

// VolMgrServerAddr ...
var VolMgrServerAddr addr

// VolMgrServer ...
type VolMgrServer struct {
	NodeID          uint64
	Addr            *com.Address
	Resolver        com.Resolver
	RaftServer      *raft.RaftServer
	Cluster         *cluster
	wg              sync.WaitGroup
	bgStatusMap     map[uint64]int32
	bgStatusMapSync sync.Mutex
	sync.Mutex
}

func startVolMgrService(volMgrServer *VolMgrServer) {

	lis, err := net.Listen("tcp", volMgrServer.Addr.Grpc)
	if err != nil {
		panic(fmt.Sprintf("Failed to listen on:%v", volMgrServer.Addr.Grpc))
	}
	s := grpc.NewServer()
	vp.RegisterVolMgrServer(s, volMgrServer)
	// Register reflection service on gRPC server.
	reflection.Register(s)
	if err := s.Serve(lis); err != nil {
		panic("Failed to serve")
	}
}

func (vs *VolMgrServer) load() error {
	sm, sg, err := raftopt.CreateClusterKvStateMachine(vs.RaftServer, VolMgrServerAddr.peers, VolMgrServerAddr.nodeID, VolMgrServerAddr.waldir, "Cluster", 1)
	if err != nil {
		return err
	}
	vs.Cluster = &cluster{RaftGroup: sm, RaftStorage: sg}
	logger.Debug("VolMgrServer.load success...")
	return nil
}

func init() {

	flag.StringVar(&VolMgrServerAddr.host, "host", "127.0.0.1", "ContainerFS VolMgr Host")
	nodeid := flag.Int64("nodeid", 1, "ContainerFS VolMgr ID")
	peers := flag.String("nodepeer", "1,2,3", "ContainerFS VolMgr peers")
	ips := flag.String("nodeips", "127.0.0.1,127.0.0.1,127.0.0.1", "ContainerFS VolMgr ips")
	flag.StringVar(&VolMgrServerAddr.waldir, "wal", "/export/containerfs/VolMgr/data", "ContainerFS VolMgr waldir")
	flag.StringVar(&VolMgrServerAddr.log, "logpath", "/export/Logs/containerfs/logs/", "ContainerFS VolMgr log")
	loglevel := flag.String("loglevel", "error", "ContainerFS VolMgr log level")

	flag.Parse()
	if len(os.Args) >= 2 && (os.Args[1] == "version") {
		fmt.Println(utils.Version())
		os.Exit(0)
	}
	VolMgrServerAddr.nodeID = uint64(*nodeid)
	VolMgrServerAddr.ips = strings.Split(*ips, ",")
	peerarray := strings.Split(*peers, ",")
	var err error
	VolMgrServerAddr.peers, err = parsePeers(peerarray)
	if err != nil {
		logger.Error("parse peers failed!. peers=%v", peers)
	}

	logger.SetConsole(true)
	logger.SetRollingFile(VolMgrServerAddr.log, "volmgr.log", 10, 100, logger.MB) //each 100M rolling
	switch *loglevel {
	case "error":
		logger.SetLevel(logger.ERROR)
	case "debug":
		logger.SetLevel(logger.DEBUG)
	case "info":
		logger.SetLevel(logger.INFO)
	default:
		logger.SetLevel(logger.ERROR)
	}

}

func parsePeers(peersstr []string) (peers []proto.Peer, err error) {
	for _, s := range peersstr {
		p, err := strconv.Atoi(s)
		if err != nil {
			return nil, err
		}
		peers = append(peers, proto.Peer{ID: uint64(p)})
	}
	return
}

func showLeaders(s *VolMgrServer) {
	l, t := s.RaftServer.LeaderTerm(1)
	logger.Debug("--------- RaftGroup LeaderID %v Term %v ---------", l, t)
	return

}

func logleveldebug(w http.ResponseWriter, req *http.Request) {
	logger.SetLevel(logger.DEBUG)
	io.WriteString(w, "ok!\n")
}

func loglevelerror(w http.ResponseWriter, req *http.Request) {
	logger.SetLevel(logger.ERROR)
	io.WriteString(w, "ok!\n")
}

func main() {

	//for multi-cpu scheduling
	numCPU := runtime.NumCPU()
	runtime.GOMAXPROCS(numCPU)

	raftopt.AddInit(VolMgrServerAddr.ips)

	fmt.Println("VolMgrServerAddr:")
	fmt.Println(VolMgrServerAddr)

	var volMgrServer VolMgrServer

	volMgrServer.bgStatusMap = make(map[uint64]int32)
	// resolver
	r := raftopt.NewClusterResolver()
	volMgrServer.Resolver = r

	// address
	addrInfo, ok := raftopt.ClusterAddrDatabase[VolMgrServerAddr.nodeID]
	if !ok {
		logger.Error("no such address info. nodeId: %d", VolMgrServerAddr.nodeID)
	}
	volMgrServer.Addr = addrInfo

	//  new raft server
	err := com.StartRaftServer(&volMgrServer.RaftServer, volMgrServer.Resolver, addrInfo, VolMgrServerAddr.nodeID)
	if err != nil {
		logger.Error("StartRaftServer failed ...")
		os.Exit(1)
	}
	logger.Debug("StartRaftServer success ...")

	// parse peers
	for _, p := range VolMgrServerAddr.peers {
		r.AddNode(p.ID, nil)
	}
	logger.Debug("AddNode success ...")

	if err := volMgrServer.load(); err != nil {
		logger.Error("loadMetaData failed: %v..", err)
		os.Exit(1)
	}

	http.HandleFunc("/logleveldebug", logleveldebug)
	http.HandleFunc("/loglevelerror", loglevelerror)
	go func() {
		http.ListenAndServe(volMgrServer.Addr.Pprof, nil)
	}()

	ticker := time.NewTicker(time.Second * 10)
	go func() {
		for range ticker.C {
			showLeaders(&volMgrServer)
		}
	}()

	volMgrServer.startClusterHealthCheck()

	startVolMgrService(&volMgrServer)

}

func (s *VolMgrServer) startClusterHealthCheck() {
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
