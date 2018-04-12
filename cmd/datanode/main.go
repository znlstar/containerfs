// Copyright (c) 2017, TIG All rights reserved.
// Use of this source code is governed by a Apache License 2.0 that can be found in the LICENSE file.

package main

import (
	"flag"
	"fmt"
	"os"
	"runtime"
	"strings"

	"github.com/tiglabs/containerfs/datanode"
	"github.com/tiglabs/containerfs/logger"
	"github.com/tiglabs/containerfs/utils"
)

func init() {

	var loglevel string
	var volMgrHosts string

	flag.StringVar(&datanode.DtAddr.Host, "host", "127.0.0.1:8801", "ContainerFS DataNode Host")
	flag.StringVar(&datanode.DtAddr.Tier, "tier", "sas", "ContainerFS DataNode Storage Medium")
	flag.StringVar(&datanode.DtAddr.Path, "datapath", "", "ContainerFS DataNode Data Path")
	flag.StringVar(&datanode.DtAddr.Log, "logpath", "/export/Logs/containerfs/logs/", "ContainerFS Log Path")
	flag.StringVar(&loglevel, "loglevel", "error", "ContainerFS Log Level")
	flag.StringVar(&volMgrHosts, "volmgr", "10.8.64.216,10.8.64.217,10.8.64.218", "ContainerFS VolMgr hosts")

	flag.Parse()
	if len(os.Args) >= 2 && (os.Args[1] == "version") {
		fmt.Println(utils.Version())
		os.Exit(0)
	}

	tmp := strings.Split(volMgrHosts, ",")

	datanode.VolMgrHosts = make([]string, 3)
	datanode.VolMgrHosts[0] = tmp[0] + ":7703"
	datanode.VolMgrHosts[1] = tmp[1] + ":7713"
	datanode.VolMgrHosts[2] = tmp[2] + ":7723"

	datanode.DtAddr.Flag = datanode.DtAddr.Path + "/.registryflag"

	logger.SetConsole(true)
	logger.SetRollingFile(datanode.DtAddr.Log, "datanode.log", 10, 100, logger.MB) //each 100M rolling

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

	_, err := os.Stat(datanode.DtAddr.Path)
	if err != nil {
		logger.Error("data node statup failed : datanode.DtAddr.Path not exist !")
		os.Exit(1)
	}

	datanode.RegistryToVolMgr()

}

func main() {

	runtime.GOMAXPROCS(runtime.NumCPU())
	datanode.StartDataService()
}
