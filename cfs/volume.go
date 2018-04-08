package cfs

import (
	"fmt"
	"strconv"
	"time"

	"github.com/tiglabs/containerfs/logger"
	"github.com/tiglabs/containerfs/proto/mp"
	"github.com/tiglabs/containerfs/proto/vp"
	"github.com/tiglabs/containerfs/utils"
	"golang.org/x/net/context"
)

func GetAllDatanode() (int32, []*vp.DataNode) {
	_, conn, err := utils.DialVolMgr(VolMgrHosts)
	if err != nil {
		logger.Error("GetAllDatanode failed,Dial to VolMgrHosts fail :%v", err)
		return -1, nil
	}
	defer conn.Close()
	vc := vp.NewVolMgrClient(conn)
	pGetDataNodeReq := &vp.GetDataNodeReq{}
	ctx, _ := context.WithTimeout(context.Background(), 5*time.Second)
	pGetDataNodeAck, err := vc.GetDataNode(ctx, pGetDataNodeReq)
	if err != nil {
		logger.Error("GetAllDatanode failed,grpc func err :%v", err)
		return -1, nil
	}
	if pGetDataNodeAck.Ret != 0 {
		logger.Error("GetAllDatanode failed,grpc func ret :%v", pGetDataNodeAck.Ret)
		return -1, nil
	}
	return 0, pGetDataNodeAck.DataNodes
}

func GetAllMetanode() (int32, []*vp.MetaNode) {
	_, conn, err := utils.DialVolMgr(VolMgrHosts)
	if err != nil {
		logger.Error("GetAllDatanode failed,Dial to VolMgrHosts fail :%v", err)
		return -1, nil
	}
	defer conn.Close()
	vc := vp.NewVolMgrClient(conn)
	pGetAllMetaNodeReq := &vp.GetAllMetaNodeReq{}
	ctx, _ := context.WithTimeout(context.Background(), 5*time.Second)
	pGetAllMetaNodeAck, err := vc.GetMetaNode(ctx, pGetAllMetaNodeReq)
	if err != nil {
		logger.Error("GetAllMetanode failed,grpc func err :%v", err)
		return -1, nil
	}
	if pGetAllMetaNodeAck.Ret != 0 {
		logger.Error("GetAllMetanode failed,grpc func ret :%v", pGetAllMetaNodeAck.Ret)
		return -1, nil
	}
	return 0, pGetAllMetaNodeAck.MetaNodes
}
func DelDatanode(host string) int {
	_, conn, err := utils.DialVolMgr(VolMgrHosts)
	if err != nil {
		logger.Error("GetAllDatanode failed,Dial to VolMgrHosts fail :%v", err)
		return -1
	}
	defer conn.Close()
	vc := vp.NewVolMgrClient(conn)

	pDelDataNodeReq := &vp.DelDataNodeReq{
		Host: host,
	}
	ctx, _ := context.WithTimeout(context.Background(), 5*time.Second)
	ack, err := vc.DelDataNode(ctx, pDelDataNodeReq)
	if err != nil {
		logger.Error("DelDataNode failed,grpc func err :%v", err)
		return -1
	}
	if ack.Ret != 0 {
		logger.Error("DelDataNode failed,grpc func ret :%v", ack.Ret)
		return -1
	}
	return 0
}

// CreateVol volume
func CreateVol(name string, capacity string, tier string, copies string) int32 {

	_, conn, err := utils.DialVolMgr(VolMgrHosts)
	if err != nil {
		logger.Error("CreateVol failed,Dial to VolMgrHosts fail :%v", err)
		return -1
	}
	defer conn.Close()
	vc := vp.NewVolMgrClient(conn)

	spaceQuota, _ := strconv.Atoi(capacity)
	pCreateVolReq := &vp.CreateVolReq{
		VolName:    name,
		SpaceQuota: int32(spaceQuota),
		Tier:       tier,
		Copies:     copies,
	}
	ctx, _ := context.WithTimeout(context.Background(), 10*time.Second)
	ack, err := vc.CreateVol(ctx, pCreateVolReq)
	if err != nil {
		logger.Error("CreateVol failed, VolMgr Leader return failed,  err:%v", err)
		if ack != nil && ack.UUID != "" {
			DeleteVol(ack.UUID)
		}
		return -1
	}
	if ack.Ret != 0 {
		logger.Error("CreateVol failed, VolMgr Leader return failed, ret:%v", ack.Ret)
		if ack.UUID != "" {
			DeleteVol(ack.UUID)
		}
		return ack.Ret
	}

	fmt.Println(ack.UUID)
	return 0
}

/*  TODO:
// Expand volume once for fuseclient
func ExpandVolRS(UUID string, MtPath string) int32 {
	path := MtPath + "/expanding"

	fd, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0666)
	if err != nil {
		return -2
	}
	defer fd.Close()

	conn, err := DialMeta("Cluster")
	if err != nil {
		logger.Error("ExpandVolRS failed,Dial to Cluster leader metanode fail :%v", err)
		return -1
	}
	defer conn.Close()
	mc := mp.NewMetaNodeClient(conn)

	pExpandVolRSReq := &mp.ExpandVolRSReq{
		VolID: UUID,
	}

	ctx, _ := context.WithTimeout(context.Background(), 10*time.Second)
	pExpandVolRSAck, err := mc.ExpandVolRS(ctx, pExpandVolRSReq)
	if err != nil {
		logger.Error("ExpandVol once volume:%v failed, Cluster leader metanode return error:%v", UUID, err)
		os.Remove(path)
		return -1
	}
	if pExpandVolRSAck.Ret == -1 {
		logger.Error("ExpandVol once volume:%v failed, Cluster leader metanode return -1:%v", UUID)
		os.Remove(path)
		return -1
	} else if pExpandVolRSAck.Ret == 0 {
		logger.Error("ExpandVol volume:%v once failed, Cluster leader metanode return 0 because volume totalsize not enough expand", UUID)
		os.Remove(path)
		return 0
	}

	out := UpdateMetaForExpandVol(UUID, pExpandVolRSAck)

	if out != 0 {
		logger.Error("ExpandVol volume:%v once cluster leader metanode success but update volume leader metanode fail, so rollback cluster leader metanode this expand resource", UUID)
		pDelReq := &mp.DelVolRSForExpandReq{
			UUID: UUID,
			BGPS: pExpandVolRSAck.BGPS,
		}

		pDelAck, err := mc.DelVolRSForExpand(ctx, pDelReq)
		if err != nil || pDelAck.Ret != 0 {
			logger.Error("ExpandVol once volume:%v success but update meta failed, then rollback cluster leader metanode error", UUID)
		}
		os.Remove(path)
		return -1
	}

	os.Remove(path)
	return 1
}

func UpdateMetaForExpandVol(UUID string, ack *mp.ExpandVolRSAck) int {
	var mpBlockGroups []*mp.BlockGroup
	for _, v := range ack.BGPS {
		mpBlockGroup := &mp.BlockGroup{
			BlockGroupID: v.Blocks[0].BGID,
			FreeSize:     utils.BlockGroupSize,
		}
		mpBlockGroups = append(mpBlockGroups, mpBlockGroup)
	}

	logger.Debug("ExpandVolRS volume:%v to leader metanode BlockGroups Info:%v", UUID, mpBlockGroups)
	// Meta handle
	conn2, err := DialMeta(UUID)
	if err != nil {
		logger.Error("ExpandVol volume:%v once volmgr success but Dial to metanode fail :%v", UUID, err)
		return -1
	}
	defer conn2.Close()

	mc := mp.NewMetaNodeClient(conn2)
	pmExpandNameSpaceReq := &mp.ExpandNameSpaceReq{
		VolID:       UUID,
		BlockGroups: mpBlockGroups,
	}
	ctx2, _ := context.WithTimeout(context.Background(), 10*time.Second)
	pmExpandNameSpaceAck, err := mc.ExpandNameSpace(ctx2, pmExpandNameSpaceReq)
	if err != nil {
		logger.Error("ExpandVol volume:%v once volmgr success but MetaNode return error:%v", UUID, err)
		return -1
	}
	if pmExpandNameSpaceAck.Ret != 0 {
		logger.Error("ExpandVol volume:%v once volmgr success but MetaNode return not equal 0:%v", UUID)
		return -1
	}

	return 0
}
*/

// CreateVol volume
func ExpandVol(uuid string, capacity string) int32 {

	_, conn, err := utils.DialVolMgr(VolMgrHosts)
	if err != nil {
		logger.Error("CreateVol failed,Dial to VolMgrHosts fail :%v", err)
		return -1
	}
	defer conn.Close()
	vc := vp.NewVolMgrClient(conn)

	spaceQuota, _ := strconv.Atoi(capacity)
	pExpandVolReq := &vp.ExpandVolReq{
		UUID:  uuid,
		Space: int32(spaceQuota),
	}
	ctx, _ := context.WithTimeout(context.Background(), 10*time.Second)
	ack, err := vc.ExpandVol(ctx, pExpandVolReq)
	if err != nil {
		logger.Error("ExpandVol failed, VolMgr Leader return failed,  err:%v", err)
		return -1
	}
	if ack.Ret != 0 {
		logger.Error("ExpandVol failed, VolMgr Leader return failed, ret:%v", ack.Ret)
		return -1
	}
	return 0
}

// Migrate bad DataNode blocks data to some Good DataNodes
func Migrate(host string) int32 {
	pMigrateReq := &vp.MigrateReq{
		DataNodeHost: host,
	}
	_, conn, err := utils.DialVolMgr(VolMgrHosts)
	if err != nil {
		logger.Error("Migrate failed,Dial to metanode fail :%v", err)
		return -1
	}
	defer conn.Close()
	vc := vp.NewVolMgrClient(conn)
	ctx, _ := context.WithTimeout(context.Background(), 5*time.Second)
	pMigrateAck, err := vc.Migrate(ctx, pMigrateReq)
	if err != nil {
		logger.Error("Migrate failed: %v", err)
		return -1
	}
	if pMigrateAck.Ret != 0 {
		logger.Error("Migrate failed: %v", pMigrateAck.Ret)
		return -1
	}
	return 0
}

func GetAllVolumeInfos() (int32, []*vp.Volume) {

	_, conn, err := utils.DialVolMgr(VolMgrHosts)
	if err != nil {
		logger.Error("GetAllDatanode failed,Dial to VolMgrHosts fail :%v", err)
		return -1, nil
	}
	defer conn.Close()
	vc := vp.NewVolMgrClient(conn)

	pVolumeInfosReq := &vp.VolumeInfosReq{}
	ctx, _ := context.WithTimeout(context.Background(), 5*time.Second)
	pVolumeInfosAck, err := vc.VolumeInfos(ctx, pVolumeInfosReq)
	if err != nil {
		logger.Error("GetAllVolumeInfos failed,grpc func err :%v", err)
		return -1, nil
	}
	if pVolumeInfosAck.Ret != 0 {
		logger.Error("GetAllVolumeInfos failed,grpc func ret :%v", pVolumeInfosAck.Ret)
		return -1, nil
	}
	return 0, pVolumeInfosAck.Volumes
}

// GetVolInfo volume info
func GetVolInfo(name string) (int32, *vp.GetVolInfoAck) {

	_, conn, err := utils.DialVolMgr(VolMgrHosts)
	if err != nil {
		logger.Error("GetVolInfo failed,Dial to VolMgrHosts fail :%v", err)
		return -1, nil
	}
	defer conn.Close()
	vc := vp.NewVolMgrClient(conn)

	pGetVolInfoReq := &vp.GetVolInfoReq{
		UUID: name,
	}
	ctx, _ := context.WithTimeout(context.Background(), 10*time.Second)
	ack, err := vc.GetVolInfo(ctx, pGetVolInfoReq)
	if err != nil || ack.Ret != 0 {
		return -1, &vp.GetVolInfoAck{}
	}
	return 0, ack
}

//Get blockgroup info
func GetBlockGroupInfo(idStr string) (int32, *vp.GetBlockGroupInfoAck) {

	bgID, err := strconv.ParseUint(idStr, 10, 64)
	if err != nil {
		logger.Error("GetBlockGroupInfo parse bdID failed:%v", err)
		return -1, nil
	}
	_, conn, err := utils.DialVolMgr(VolMgrHosts)
	if err != nil {
		logger.Error("GetBlockGroupInfo failed,Dial to VolMgrHosts fail :%v", err)
		return -1, nil
	}
	defer conn.Close()
	vc := vp.NewVolMgrClient(conn)

	pGetBlockGroupInfoReq := &vp.GetBlockGroupInfoReq{
		BGID: bgID,
	}
	ctx, _ := context.WithTimeout(context.Background(), 10*time.Second)
	ack, err := vc.GetBlockGroupInfo(ctx, pGetBlockGroupInfoReq)
	if err != nil {
		logger.Error("GetBlockGroupInfo failed: %v", err)
		return -1, &vp.GetBlockGroupInfoAck{}
	}
	if ack.Ret != 0 {
		logger.Error("GetBlockGroupInfo failed: %v", ack.Ret)
		return -1, &vp.GetBlockGroupInfoAck{}
	}
	return 0, ack
}

// SnapShootVol ...
func SnapShotVol(uuid string) int32 {
	// send to metadata to delete a  map

	for _, v := range MetaNodeHosts {

		conn, err := utils.Dial(v)
		if err != nil {
			logger.Error("SnapShotVol failed,Dial to MetaNodeHosts %v fail :%v", v, err)
			return -1
		}

		defer conn.Close()

		mc := mp.NewMetaNodeClient(conn)
		pmSnapShotNameSpaceReq := &mp.SnapShotNameSpaceReq{
			VolID: uuid,
		}
		ctx, _ := context.WithTimeout(context.Background(), 100*time.Second)
		pmSnapShotNameSpaceAck, err := mc.SnapShotNameSpace(ctx, pmSnapShotNameSpaceReq)
		if err != nil {
			logger.Error("SnapShotVol failed,grpc func err :%v", err)
			return -1
		}

		if pmSnapShotNameSpaceAck.Ret != 0 {
			logger.Error("SnapShotVol failed,rpc func ret:%v", pmSnapShotNameSpaceAck.Ret)
			return -1
		}
	}

	return 0
}

//Snapshot cluster data on volmgrs
func SnapShotCluster() int32 {

	for _, v := range VolMgrHosts {

		conn, err := utils.Dial(v)
		if err != nil {
			logger.Error("SnapShotVol failed,Dial to MetaNodeHosts %v fail :%v", v, err)
			return -1
		}

		defer conn.Close()

		vc := vp.NewVolMgrClient(conn)
		pSnapShotClusterReq := &vp.SnapShotClusterReq{}
		ctx, _ := context.WithTimeout(context.Background(), 100*time.Second)
		pSnapShotClusterAck, err := vc.SnapShotCluster(ctx, pSnapShotClusterReq)
		if err != nil {
			logger.Error("SnapShotVol failed,grpc func err :%v", err)
			return -1
		}

		if pSnapShotClusterAck.Ret != 0 {
			logger.Error("SnapShotCluster failed,rpc func ret:%v", pSnapShotClusterAck.Ret)
			return -1
		}
	}

	return 0
}

// DeleteVol function
func DeleteVol(uuid string) int32 {

	_, conn, err := utils.DialVolMgr(VolMgrHosts)
	if err != nil {
		logger.Error("DeleteVol failed,Dial to VolMgrHosts fail :%v", err)
		return -1
	}
	defer conn.Close()
	vc := vp.NewVolMgrClient(conn)

	pDeleteVolReq := &vp.DeleteVolReq{
		UUID: uuid,
	}
	ctx, _ := context.WithTimeout(context.Background(), 10*time.Second)
	pDeleteVolAck, err := vc.DeleteVol(ctx, pDeleteVolReq)
	if err != nil {
		return -1
	}

	if pDeleteVolAck.Ret != 0 {
		logger.Error("DeleteVol failed :%v", pDeleteVolAck.Ret)
		return -1
	}

	return 0
}

func GetVolMetaLeader(UUID string) (string, error) {
	_, conn, err := utils.DialVolMgr(VolMgrHosts)
	if err != nil {
		return "", err
	}

	vc := vp.NewVolMgrClient(conn)
	pGetMetaNodeRGReq := &vp.GetMetaNodeRGReq{
		UUID: UUID,
	}
	ctx, _ := context.WithTimeout(context.Background(), 10*time.Second)
	pGetMetaNodeRGAck, err := vc.GetMetaNodeRG(ctx, pGetMetaNodeRGReq)
	if err != nil {
		return "", err
	}

	if pGetMetaNodeRGAck.Ret != 0 {
		return "", fmt.Errorf("GetVolMetaLeader GetMetaNodeRG failed Ret:%v", pGetMetaNodeRGAck.Ret)
	}

	return pGetMetaNodeRGAck.Leader, nil
}
