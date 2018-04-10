package main

import (
	"flag"
	"fmt"
	"net/http"
	"os"
	"runtime"
	"strings"

	"github.com/tiglabs/containerfs/logger"
	"github.com/tiglabs/containerfs/metanode"
	ns "github.com/tiglabs/containerfs/metanode/namespace"
	"github.com/tiglabs/containerfs/raftopt"
	"github.com/tiglabs/containerfs/raftopt/common"
	"github.com/tiglabs/containerfs/utils"
)

func init() {

	var volmgrHostString string
	var nodeid uint64
	var loglevel string

	flag.StringVar(&metanode.MetaNodeServerAddr.Host, "host", "127.0.0.1", "ContainerFS Metanode Host")
	flag.StringVar(&volmgrHostString, "volmgr", "10.8.64.216,10.8.64.217,10.8.64.218", "ContainerFS VolMgr Host")
	flag.Uint64Var(&nodeid, "nodeid", 1, "ContainerFS Metanode ID")
	flag.StringVar(&metanode.MetaNodeServerAddr.Waldir, "wal", "/export/containerfs/metanode/data", "ContainerFS Meta waldir")
	flag.StringVar(&metanode.MetaNodeServerAddr.Log, "logpath", "/export/Logs/containerfs/logs/", "ContainerFS Meta log")
	flag.StringVar(&loglevel, "loglevel", "error", "ContainerFS metanode log level")

	flag.Parse()
	if len(os.Args) >= 2 && (os.Args[1] == "version") {
		fmt.Println(utils.Version())
		os.Exit(0)
	}

	tmp := strings.Split(volmgrHostString, ",")

	metanode.MetaNodeServerAddr.VolmgrHosts = make([]string, 3)
	metanode.MetaNodeServerAddr.VolmgrHosts[0] = tmp[0] + ":7703"
	metanode.MetaNodeServerAddr.VolmgrHosts[1] = tmp[1] + ":7713"
	metanode.MetaNodeServerAddr.VolmgrHosts[2] = tmp[2] + ":7723"

	metanode.MetaNodeServerAddr.NodeID = nodeid
	ns.VolMgrHosts = metanode.MetaNodeServerAddr.VolmgrHosts

	logger.SetConsole(true)
	logger.SetRollingFile(metanode.MetaNodeServerAddr.Log, "metanode.log", 10, 100, logger.MB) //each 100M rolling
	switch loglevel {
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

func main() {

	runtime.GOMAXPROCS(runtime.NumCPU())

	var metaServer metanode.MetaNodeServer
	// resolver
	r := raftopt.NewVolumeResolver()
	metaServer.Resolver = r

	//  new raft server
	addr := &common.Address{
		Grpc:      metanode.MetaNodeServerAddr.Host + ":9901",
		Heartbeat: metanode.MetaNodeServerAddr.Host + ":9902",
		Replicate: metanode.MetaNodeServerAddr.Host + ":9903",
		Pprof:     metanode.MetaNodeServerAddr.Host + ":9904",
	}
	metaServer.Addr = addr
	err := common.StartRaftServer(&metaServer.RaftServer, metaServer.Resolver, addr, metanode.MetaNodeServerAddr.NodeID)
	if err != nil {
		logger.Error("StartRaftServer failed ...")
		os.Exit(1)
	}
	logger.Debug("StartRaftServer success ...")

	if ret := metaServer.RegistryToVolMgr(); ret != 0 {
		os.Exit(1)
	}

	logger.Debug("AddNode success ...")

	ret := metaServer.LoadMetaData()
	if ret != 0 {
		if ret == 1 {
			logger.Debug("loadMetaData  no volumes")
		} else {
			logger.Error("loadMetaData failed ...")
			os.Exit(1)
		}
	}

	http.HandleFunc("/logleveldebug", utils.Logleveldebug)
	http.HandleFunc("/loglevelerror", utils.Loglevelerror)
	go func() {
		http.ListenAndServe(addr.Pprof, nil)
	}()

	metanode.StartMetaDataService(&metaServer)

}
