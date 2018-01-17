package main

import (
	"flag"
	"fmt"
	fs "github.com/tiglabs/containerfs/fs"
	"github.com/tiglabs/containerfs/logger"
	"os"
	"strconv"
	"strings"
)

func main() {

	var loglevel string
	var logpath string
	var peers string
	var cmd string

	flag.StringVar(&peers, "metanode", "127.0.0.1:9903,127.0.0.1:9913,127.0.0.1:9923", "ContainerFS MetaNode Host")
	flag.StringVar(&logpath, "logpath", "/export/Logs/containerfs/logs/", "ContainerFS Log Path")
	flag.StringVar(&loglevel, "loglevel", "error", "ContainerFS Log Level")
	flag.StringVar(&cmd, "action", "", `
		createvol [volumename ] size [sata/sas/ssd/nvme...]
		expandvol [volumeuuid ] size
		migrate [datanodeIP] [datanodePort]
		deletevol [volumeuuid ]
		snapshotvol [volumeuuid ]
		getvolleader [volumeuuid ]
		getvolinfo [volumeuuid ]
		getvols
		getinodeinfo [volumeuuid ] parentinodeid name
		getdatanodes
		deldatanode ip port`)

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
		if argNum != 2 && argNum != 3 {
			fmt.Println("createvol [volname] [space GB] [sata/sas/ssd/nvme...]")
			os.Exit(1)
		}

		var ret int32
		if argNum == 2 {
			ret = fs.CreateVolbyMeta(flag.Arg(0), flag.Arg(1), "sas")
		} else if argNum == 3 {
			ret = fs.CreateVolbyMeta(flag.Arg(0), flag.Arg(1), flag.Arg(2))
		}
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

	case "migrate":
		argNum := flag.NArg()
		if argNum != 2 {
			fmt.Println("migrate [datanodeIP:Port]")
			os.Exit(1)
		}
		ret := fs.Migrate(flag.Arg(0))
		if ret != 0 {
			fmt.Println("Migrate failed")
		} else {
			fmt.Println("Migrate process on background")
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
	case "snapshotvol":
		argNum := flag.NArg()
		if argNum != 1 {
			fmt.Println("snapshotvol [voluuid]")
			os.Exit(1)
		}
		ret := fs.SnapShotVol(flag.Arg(0))
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
			for _, v := range vi.BGPS {
				fmt.Printf("BG: %v -->\n", v.Blocks[0].BGID)
				for _, vv := range v.Blocks {
					fmt.Println(vv)
				}
			}
		} else {
			fmt.Printf("get volume info failed , ret :%d", ret)
		}
	case "getvols":
		argNum := flag.NArg()
		if argNum != 0 {
			fmt.Println("getvols")
			os.Exit(1)
		}
		ret, vi := fs.VolumeInfos()
		if ret == 0 {
			for _, v := range vi {
				fmt.Println(v)
			}
		} else {
			fmt.Printf("get all volumes info failed , ret :%d", ret)
		}
	case "getinodeinfo":
		argNum := flag.NArg()
		if argNum != 3 {
			fmt.Println("getinodeinfo [volUUID] parentinodeid name")
			os.Exit(1)
		}
		cfs := fs.OpenFileSystem(flag.Arg(0))
		if cfs == nil {
			fmt.Println("no such volume")
			os.Exit(1)
		}

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
	case "getdatanodes":
		argNum := flag.NArg()
		if argNum != 0 {
			fmt.Println("getdatanodes")
			os.Exit(1)
		}
		ret, vi := fs.GetAllDataNode()
		if ret == 0 {
			for _, v := range vi {
				fmt.Println(v)
			}
		} else {
			fmt.Printf("get all datanodes info failed , ret :%d", ret)
		}

	case "deldatanode":
		argNum := flag.NArg()
		if argNum != 2 {
			fmt.Println("deldatanode ip:port")
			os.Exit(1)
		}
		ret := fs.DelDataNode(flag.Arg(0))
		if ret == 0 {
			fmt.Printf("del success")
		} else {
			fmt.Printf("del failed , ret :%d", ret)
		}
	default:
		fmt.Println("wrong action operation")
	}

}
