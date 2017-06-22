package main

import (
	"flag"
	"fmt"
	fs "github.com/ipdcode/containerfs/fs"
	"strings"
)

func main() {

	volMgrAddr := flag.String("volmgr", "127.0.0.1:10001", "ContainerFS volmgr host")
	metaNodeAddr := flag.String("metanode", "127.0.0.1:9903,127.0.0.1:9913,127.0.0.1:9923", "ContainerFS metanode hosts")
	volName := flag.String("volname", "xxx", "ContainerFS Volume name")
	capacity := flag.String("capacity", "10", "ContainerFS Volume capacity")
	uuid := flag.String("uuid", "xxx", "ContainerFS Volume UUID")
	opt := flag.String("opt", "xxx", "ContainerFS opts (createvol deletevol getvolinfo)")

	flag.Parse()

	fs.VolMgrAddr = *volMgrAddr
	fs.MetaNodePeers = strings.Split(*metaNodeAddr, ",")
	fs.MetaNodeAddr = fs.MetaNodePeers[0]

	switch *opt {

	case "createvol":
		ret := fs.CreateVol(*volName, *capacity)
		if ret != 0 {
			fmt.Println("failed")
		}
	case "snapshootvol":
		ret := fs.SnapShootVol(*volName)
		if ret != 0 {
			fmt.Println("failed")
		}
	case "deletevol":
		ret := fs.DeleteVol(*uuid)
		if ret != 0 {
			fmt.Println("failed")
		}
	case "getvolinfo":
		ret, vi := fs.GetVolInfo(*uuid)
		if ret == 0 {
			fmt.Println(vi)
		} else {
			fmt.Printf("get volume info failed , ret :%d", ret)
		}
	}

}
