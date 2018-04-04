package main

import (
	"flag"
	"fmt"
	"time"

	"github.com/tiglabs/containerfs/proto/mp"
	"github.com/tiglabs/containerfs/proto/vp"
	"github.com/tiglabs/containerfs/utils"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
)

var (
	VolMgrHosts = []string{"10.8.65.159:7703", "10.8.65.160:7713", "10.8.65.161:7723"}
)

func main() {
	var con *grpc.ClientConn
	var err error

	flag.Parse()
	args := flag.Args()
	if len(args) > 0 {
		con, err = utils.Dial(args[0])
	} else {
		_, con, err = utils.DialVolMgr(VolMgrHosts)
	}
	if err != nil {
		fmt.Println("dial volmgr failed", err)
		return
	}
	defer con.Close()

	vc := vp.NewVolMgrClient(con)
	client := ClientTest{client: vc}
	// // // // client.testGetMetaNode()
	// // // // client.testGetDataNode()
	// client.testGetMetaNodeRG()
	//client.testCreateVolume()
	client.testGetMetaNodeRGPeers()
	// vc := mp.NewMetaNodeClient(con)
	// client := MetaNodeClientTest{client: vc}
	// // client.testAllocateChunk()
	// client.testGetMetaNodeLeader()
	return
}

type ClientTest struct {
	client vp.VolMgrClient
}
type MetaNodeClientTest struct {
	client mp.MetaNodeClient
}

func (cl MetaNodeClientTest) testAllocateChunk() {
	req := &mp.AllocateChunkReq{VolID: "60c8daf2402a0a2e80791a261fad4224"}
	ack, err := cl.client.AllocateChunk(context.Background(), req)
	if AckError(ack, err) {
		return
	}
	fmt.Println(ack.ChunkInfo.BlockGroupWithHost.Hosts)
}
func (cl MetaNodeClientTest) testGetMetaNodeLeader() {
	req := &mp.GetMetaLeaderReq{VolID: "60c8daf2402a0a2e80791a261fad4224"}
	ack, err := cl.client.GetMetaLeader(context.Background(), req)
	if AckError(ack, err) {
		return
	}
	fmt.Println(ack.Leader)
}
func (cl ClientTest) testGetMetaNodeRGPeers() {
	req := &vp.GetMetaNodeRGPeersReq{MetaNodeID: 1}
	ack, err := cl.client.GetMetaNodeRGPeers(context.Background(), req)
	if AckError(ack, err) {
		return
	}
	fmt.Println(len(ack.RaftGroups))
	for _, rg := range ack.RaftGroups {
		for _, mn := range rg.MetaNodes {
			fmt.Println(mn.GetId(), mn.GetHost())
		}
		fmt.Println(rg.GetRGID(), rg.GetUUID())
	}
	return
}
func (cl ClientTest) testGetMetaNodeRG() {
	req := &vp.GetMetaNodeRGReq{UUID: "60c8daf2402a0a2e80791a261fad4224"}
	ack, err := cl.client.GetMetaNodeRG(context.Background(), req)
	if AckError(ack, err) {
		return
	}
	fmt.Println(ack.GetLeader())
	for _, mn := range ack.GetMetaNodes() {
		fmt.Println(mn.GetHost(), mn.GetId(), mn.GetMem(), mn.GetStatus())
	}
}
func (cl ClientTest) testGetVolume() {
	req := vp.GetVolInfoReq{UUID: "13db245a40f9a15b8064aefdb8c91f25"}
	ack, err := cl.client.GetVolInfo(context.Background(), &req)
	if AckError(ack, err) {
		return
	}
	fmt.Println(ack.Volume.BlockGroups, ack.Volume.GetUUID())
	return
}
func (cl ClientTest) testCreateVolume() {
	req := vp.CreateVolReq{
		VolName:    "test_" + fmt.Sprintf("%d", time.Now().Unix()),
		Tier:       "sas",
		SpaceQuota: 10,
	}
	ack, err := cl.client.CreateVol(context.Background(), &req)
	if AckError(ack, err) {
		return
	}
	fmt.Println(ack.GetRet(), ack.GetRaftGroupID(), ack.GetUUID())
	return
}
func (cl ClientTest) testGetMetaNode() {
	req := vp.GetAllMetaNodeReq{}
	ack, err := cl.client.GetMetaNode(context.Background(), &req)
	if AckError(ack, err) {
		return
	}
	fmt.Println(ack.GetRet())
	for _, mn := range ack.MetaNodes {
		fmt.Println(mn.GetHost(), mn.GetId(), mn.GetStatus())
	}
	return
}

func (cl ClientTest) testVolMgrInfo() {
	req := vp.VolMgrInfoReq{}
	ack, err := cl.client.VolMgrInfo(context.Background(), &req)
	if AckError(ack, err) {
		return
	}
	fmt.Println("ack:", *ack)
}
func (cl ClientTest) testDataNodeRegistry() {
	req := vp.DataNode{Host: "10.27.10.12:8001",
		MountPoint: "/mnt/test",
		Tier:       "sas",
		Capacity:   1234,
		Used:       0,
		Free:       1234,
		Status:     0}

	ack, err := cl.client.DataNodeRegistry(context.Background(), &req)
	if AckError(ack, err) {
		return
	}
	fmt.Println(ack.Ret)
}

func (cl ClientTest) testGetDataNode() {
	req := vp.GetDataNodeReq{}
	ack, err := cl.client.GetDataNode(context.Background(), &req)
	if AckError(ack, err) {
		return
	}
	for _, dn := range ack.DataNodes {
		fmt.Println(dn.GetHost(), dn.GetMountPoint(), dn.GetStatus())
	}
	return
}

type Ack interface {
	GetRet() int32
}

func AckError(ack Ack, err error) bool {
	if err != nil {
		fmt.Println("call erro:", err)
		return true
	}

	if ack.GetRet() != 0 {
		fmt.Println("call failed", ack.GetRet())
		return true
	}
	return false
}
