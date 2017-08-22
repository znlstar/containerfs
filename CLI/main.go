package main

import (
	"fmt"
	fs "github.com/ipdcode/containerfs/fs"
	"github.com/ipdcode/containerfs/logger"
	"github.com/lxmgo/config"
	"os"
)

func main() {

	c, err := config.NewConfig(os.Args[1])
	if err != nil {
		fmt.Println("NewConfig err")
		os.Exit(1)
	}

	fs.VolMgrAddr = c.String("volmgr::host")
	fs.MetaNodePeers = c.Strings("metanode::host")
	fs.MetaNodeAddr = fs.MetaNodePeers[0]
	fs.BufferSize = 1024 * 1024

	logger.SetConsole(true)
	logger.SetRollingFile(c.String("logger::log"), "fuse.log", 10, 100, logger.MB) //each 100M rolling
	switch level := c.String("logger::loglevel"); level {
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
		argNum := len(os.Args)
		if argNum != 5 {
			fmt.Println("createvol [volname] [space GB]")
			os.Exit(1)
		}
		ret := fs.CreateVol(os.Args[3], os.Args[4])
		if ret != 0 {
			fmt.Println("failed")
		}

	case "expendvol":
		argNum := len(os.Args)
		if argNum != 5 {
			fmt.Println("expendvol [volUUID] [space GB]")
			os.Exit(1)
		}
		ret := fs.ExpendVol(os.Args[3], os.Args[4])
		if ret != 0 {
			fmt.Println("failed")
		}
	case "deletevol":
		argNum := len(os.Args)
		if argNum != 4 {
			fmt.Println("deletevol [voluuid]")
			os.Exit(1)
		}
		ret := fs.DeleteVol(os.Args[3])
		if ret != 0 {
			fmt.Println("failed")
		}
	case "snapshootvol":
		argNum := len(os.Args)
		if argNum != 4 {
			fmt.Println("snapshootvol [voluuid]")
			os.Exit(1)
		}
		ret := fs.SnapShootVol(os.Args[3])
		if ret != 0 {
			fmt.Println("failed")
		}
	case "getvolinfo":
		argNum := len(os.Args)
		if argNum != 4 {
			fmt.Println("getvolinfo [volUUID]")
			os.Exit(1)
		}
		ret, vi := fs.GetVolInfo(os.Args[3])
		if ret == 0 {
			fmt.Println(vi)
		} else {
			fmt.Printf("get volume info failed , ret :%d", ret)
		}

	default:
		fmt.Println("wrong operation")
	}

}
