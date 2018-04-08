package main

import (
	"flag"
	"fmt"
	"net"
	"os"
	"runtime"
	"runtime/debug"
	"strings"

	dt "github.com/tiglabs/containerfs/datanode"
	"github.com/tiglabs/containerfs/logger"
	"github.com/tiglabs/containerfs/proto/dp"
	"github.com/tiglabs/containerfs/utils"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)


func startDataService() {

	lis, err := net.Listen("tcp", dt.DtAddr.Host)
	if err != nil {
		panic(fmt.Sprintf("Failed to listen on:%v", dt.DtAddr.Host))
	}
	s := grpc.NewServer()
	dp.RegisterDataNodeServer(s, &dt.DataNodeServer{M2SReplClientStreamCache: make(map[uint64]*dt.M2SReplClientStream), C2MReplServerStreamCache: make(map[uint64]*dt.C2MReplServerStream)})
	reflection.Register(s)
	if err := s.Serve(lis); err != nil {
		panic(fmt.Sprintf("Failed to start Serve on:%v", dt.DtAddr.Host))
	}
}

func init() {

	var loglevel string
	var volMgrHosts string

	flag.StringVar(&dt.DtAddr.Host, "host", "127.0.0.1:8801", "ContainerFS DataNode Host")
	flag.StringVar(&dt.DtAddr.Tier, "tier", "sas", "ContainerFS DataNode Storage Medium")
	flag.StringVar(&dt.DtAddr.Path, "datapath", "", "ContainerFS DataNode Data Path")
	flag.StringVar(&dt.DtAddr.Log, "logpath", "/export/Logs/containerfs/logs/", "ContainerFS Log Path")
	flag.StringVar(&loglevel, "loglevel", "error", "ContainerFS Log Level")
	flag.StringVar(&volMgrHosts, "volmgr", "10.8.64.216,10.8.64.217,10.8.64.218", "ContainerFS VolMgr hosts")

	flag.Parse()
	if len(os.Args) >= 2 && (os.Args[1] == "version") {
		fmt.Println(utils.Version())
		os.Exit(0)
	}

	tmp := strings.Split(volMgrHosts, ",")

	dt.VolMgrHosts = make([]string, 3)
	dt.VolMgrHosts[0] = tmp[0] + ":7703"
	dt.VolMgrHosts[1] = tmp[1] + ":7713"
	dt.VolMgrHosts[2] = tmp[2] + ":7723"

	dt.DtAddr.Flag = dt.DtAddr.Path + "/.registryflag"

	logger.SetConsole(true)
	logger.SetRollingFile(dt.DtAddr.Log, "datanode.log", 10, 100, logger.MB) //each 100M rolling

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

	_, err := os.Stat(dt.DtAddr.Path)
	if err != nil {
		logger.Error("data node statup failed : dt.DtAddr.Path not exist !")
		os.Exit(1)
	}

	dt.RegistryToVolMgr()

}

func main() {

	numCPU := runtime.NumCPU()
	runtime.GOMAXPROCS(numCPU)

	defer func() {
		if err := recover(); err != nil {
			logger.Error("panic !!! :%v", err)
			logger.Error("stacks:%v", string(debug.Stack()))
		}
	}()

	startDataService()
}

