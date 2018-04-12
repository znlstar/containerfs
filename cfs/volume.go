// Copyright (c) 2017, TIG All rights reserved.
// Use of this source code is governed by a Apache License 2.0 that can be found in the LICENSE file.

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

// GetAllDatanode to get infos of all Datanodes
func GetAllDatanode() (int32, []*vp.DataNode) {
	_, conn, err := utils.DialVolMgr(VolMgrHosts)
	if err != nil {
		logger.Error("GetAllDatanode failed,Dial to VolMgrHosts fail :%v", err)
		return -1, nil
	}
	defer conn.Close()
	vc := vp.NewVolMgrClient(conn)
	pGetDataNodeReq := &vp.GetDataNodeReq{}
	ctx, _ := context.WithTimeout(context.Background(), VOLUME_TIMEOUT_SECONDS*time.Second)
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

// GetAllMetanode to get infos of all Metanodes
func GetAllMetanode() (int32, []*vp.MetaNode) {
	_, conn, err := utils.DialVolMgr(VolMgrHosts)
	if err != nil {
		logger.Error("GetAllDatanode failed,Dial to VolMgrHosts fail :%v", err)
		return -1, nil
	}
	defer conn.Close()
	vc := vp.NewVolMgrClient(conn)
	pGetAllMetaNodeReq := &vp.GetAllMetaNodeReq{}
	ctx, _ := context.WithTimeout(context.Background(), VOLUME_TIMEOUT_SECONDS*time.Second)
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

// DelDatanode to delete a Datanode from cluster
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
	ctx, _ := context.WithTimeout(context.Background(), VOLUME_TIMEOUT_SECONDS*time.Second)
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

// CreateVol to create a new volume
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
	ctx, _ := context.WithTimeout(context.Background(), VOLUME_TIMEOUT_SECONDS*time.Second)
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

// ExpandVol to expand voume capacity
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
	ctx, _ := context.WithTimeout(context.Background(), VOLUME_TIMEOUT_SECONDS*time.Second)
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

// Migrate to migrate blocks of the bad DataNode to some normal DataNodes
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
	ctx, _ := context.WithTimeout(context.Background(), VOLUME_TIMEOUT_SECONDS*time.Second)
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

// GetAllVolumeInfos to get infos of all volumes
func GetAllVolumeInfos() (int32, []*vp.Volume) {

	_, conn, err := utils.DialVolMgr(VolMgrHosts)
	if err != nil {
		logger.Error("GetAllDatanode failed,Dial to VolMgrHosts fail :%v", err)
		return -1, nil
	}
	defer conn.Close()
	vc := vp.NewVolMgrClient(conn)

	pVolumeInfosReq := &vp.VolumeInfosReq{}
	ctx, _ := context.WithTimeout(context.Background(), VOLUME_TIMEOUT_SECONDS*time.Second)
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

// GetVolInfo to get detail infos of a volume by UUID
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
	ctx, _ := context.WithTimeout(context.Background(), VOLUME_TIMEOUT_SECONDS*time.Second)
	ack, err := vc.GetVolInfo(ctx, pGetVolInfoReq)
	if err != nil || ack.Ret != 0 {
		return -1, &vp.GetVolInfoAck{}
	}
	return 0, ack
}

//GetBlockGroupInfo to get block group infos by block group ID
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
	ctx, _ := context.WithTimeout(context.Background(), VOLUME_TIMEOUT_SECONDS*time.Second)
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

// SnapShotVol to snapshot a volume's metadata
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
		ctx, _ := context.WithTimeout(context.Background(), SNAPSHOT_TIMEOUT_SECONDS*time.Second)
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

// SnapShotCluster to snapshot cluster data on volmgrs
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
		ctx, _ := context.WithTimeout(context.Background(), SNAPSHOT_TIMEOUT_SECONDS*time.Second)
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

// DeleteVol to delete a volume by UUID
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
	ctx, _ := context.WithTimeout(context.Background(), VOLUME_TIMEOUT_SECONDS*time.Second)
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

// GetVolMetaLeader to get the leader of volume raft group by UUID
func GetVolMetaLeader(UUID string) (string, error) {
	_, conn, err := utils.DialVolMgr(VolMgrHosts)
	if err != nil {
		return "", err
	}

	vc := vp.NewVolMgrClient(conn)
	pGetMetaNodeRGReq := &vp.GetMetaNodeRGReq{
		UUID: UUID,
	}
	ctx, _ := context.WithTimeout(context.Background(), VOLUME_TIMEOUT_SECONDS*time.Second)
	pGetMetaNodeRGAck, err := vc.GetMetaNodeRG(ctx, pGetMetaNodeRGReq)
	if err != nil {
		return "", err
	}

	if pGetMetaNodeRGAck.Ret != 0 {
		return "", fmt.Errorf("GetVolMetaLeader GetMetaNodeRG failed Ret:%v", pGetMetaNodeRGAck.Ret)
	}

	return pGetMetaNodeRGAck.Leader, nil
}
