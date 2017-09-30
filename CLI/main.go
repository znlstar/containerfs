package main

import (
	"flag"
	"fmt"
	fs "github.com/ipdcode/containerfs/fs"
	"github.com/ipdcode/containerfs/logger"
	"os"
	"strconv"
	"strings"
)

func main() {

	var loglevel string
	var logpath string
	var peers string
	var cmd string

	flag.StringVar(&fs.VolMgrAddr, "volmgr", "127.0.0.1:10001", "ContainerFS VolMgr Host")
	flag.StringVar(&peers, "metanode", "127.0.0.1:9903,127.0.0.1:9913,127.0.0.1:9923", "ContainerFS MetaNode Host")
	flag.StringVar(&logpath, "logpath", "/export/Logs/containerfs/logs/", "ContainerFS Log Path")
	flag.StringVar(&loglevel, "loglevel", "error", "ContainerFS Log Level")
	flag.StringVar(&cmd,"action","",`
		createvol [volumename ] size
		expandvol [volumeuuid ] size
		deletevol [volumeuuid ]
		snapshootvol [volumeuuid ]
		getvolleader [volumeuuid ]
		getvolinfo [volumeuuid ]
		getinodeinfo [volumeuuid ] parentinodeid name`)	

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

	//switch flag.Arg(0) {
	switch cmd {

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

	case "expandvol":
		argNum := flag.NArg()
		if argNum != 2 {
			fmt.Println("expandvol [volUUID] [space GB]")
			os.Exit(1)
		}
		ret := fs.ExpandVolTS(flag.Arg(0), flag.Arg(1))
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
	case "getvolleader":
		argNum := flag.NArg()
		if argNum != 1 {
			fmt.Println("getvolleader [voluuid]")
			os.Exit(1)
		}
		leader := fs.GetVolumeLeader(flag.Arg(0))
		fmt.Println(leader)
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
	case "getinodeinfo":
		argNum := flag.NArg()
		if argNum != 3 {
			fmt.Println("getinodeinfo [volUUID] parentinodeid name")
			os.Exit(1)
		}
		cfs := fs.OpenFileSystem(flag.Arg(0))
		pinode, err := strconv.ParseUint(flag.Arg(1), 10, 64)
		if err == nil {
			ret, inode, inodeinfo := cfs.GetInodeInfoDirect(pinode, flag.Arg(2))
			if ret != 0 {
				fmt.Println("no such inode")
			} else {
				fmt.Printf("inode:%v\ninodeinfo:%v\n", inode, inodeinfo)
				ret, chunkinfo, _ := cfs.GetFileChunksDirect(pinode, flag.Arg(2))
				if ret != 0 {
					fmt.Printf("no chunkinfo")
				} else {
					fmt.Printf("chunkinfo:%v\n", chunkinfo)

				}
			}
		} else {
			fmt.Println("err parent inode", err)
		}

	default:
		fmt.Println("wrong action operation")
	}

}
