package main

import (
	"flag"
	"fmt"
	fs "github.com/ipdcode/containerfs/fs"
	"github.com/ipdcode/containerfs/logger"
	"os"
	"strings"
)

func main() {

	var loglevel string
	var logpath string
	var peers string

	flag.StringVar(&fs.VolMgrAddr, "volmgr", "127.0.0.1:10001", "ContainerFS VolMgr Host")
	flag.StringVar(&peers, "metanode", "127.0.0.1:9903,127.0.0.1:9913,127.0.0.1:9923", "ContainerFS MetaNode Host")
	flag.StringVar(&logpath, "logpath", "/export/Logs/containerfs/logs/", "ContainerFS Log Path")
	flag.StringVar(&loglevel, "loglevel", "error", "ContainerFS Log Level")

	flag.Parse()

	fs.MetaNodePeers = strings.Split(peers, ",")
	fs.MetaNodeAddr = fs.MetaNodePeers[0]
	fs.BufferSize = 1024 * 1024

	logger.SetConsole(true)
	logger.SetRollingFile(logpath, "CLI.log", 10, 100, logger.MB) //each 100M rolling
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

	switch os.Args[2] {

	case "createvol":
		argNum := flag.NArg()
		if argNum != 2 {
			fmt.Println("createvol [volname] [space GB]")
			os.Exit(1)
		}
		ret := fs.CreateVol(flag.Arg(0), flag.Arg(1))
		if ret != 0 {
			fmt.Println("failed")
		}

	case "expendvol":
		argNum := flag.NArg()
		if argNum != 2 {
			fmt.Println("expendvol [volUUID] [space GB]")
			os.Exit(1)
		}
		ret := fs.ExpendVol(flag.Arg(0), flag.Arg(1))
		if ret != 0 {
			fmt.Println("failed")
		}
	case "deletevol":
		argNum := flag.NArg()
		if argNum != 1 {
			fmt.Println("deletevol [voluuid]")
			os.Exit(1)
		}
		ret := fs.DeleteVol(flag.Arg(0))
		if ret != 0 {
			fmt.Println("failed")
		}
	case "snapshootvol":
		argNum := flag.NArg()
		if argNum != 1 {
			fmt.Println("snapshootvol [voluuid]")
			os.Exit(1)
		}
		ret := fs.SnapShootVol(flag.Arg(0))
		if ret != 0 {
			fmt.Println("failed")
		}
	case "getvolinfo":
		argNum := flag.NArg()
		if argNum != 1 {
			fmt.Println("getvolinfo [volUUID]")
			os.Exit(1)
		}
		ret, vi := fs.GetVolInfo(flag.Arg(0))
		if ret == 0 {
			fmt.Println(vi)
		} else {
			fmt.Printf("get volume info failed , ret :%d", ret)
		}

	default:
		fmt.Println("wrong operation")
	}

}
