package main

import (
	"database/sql"
	"flag"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"github.com/ipdcode/containerfs/logger"
	dp "github.com/ipdcode/containerfs/proto/dp"
	vp "github.com/ipdcode/containerfs/proto/vp"
	"github.com/ipdcode/containerfs/utils"
	dr "github.com/ipdcode/containerfs/volmgr/driver"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
	"net"
	"os"
	"runtime"
	"runtime/debug"
	"strconv"
	"strings"
	"sync"
	"time"
)

type addr struct {
	host string
	port int
	log  string
}

// VolMgrServerAddr ...
var VolMgrServerAddr addr

// Wg ...
var Wg sync.WaitGroup

type mysqlc struct {
	dbhost     string
	dbusername string
	dbpassword string
	dbname     string
}

var mysqlConf mysqlc

// BlkSize : each block size
const (
	BlkSizeG = 5
	BlkSize = 5 * 1024 * 1024 * 1024 /*one blksize 5G*/
	OneExpandSize = 30 * 1024 * 1024 * 1024 /*allocated volumesize 30G for each time*/
)

// Mutex var g_RpcConfig RpcConfigOpts
var Mutex sync.RWMutex
var err string

// VolMgrServer ...
type VolMgrServer struct{}

func checkErr(op string, err error) {
	if err != nil {
		logger.Error("opreation:%v error:%v", op, err)
	}
}

// DatanodeRegistry : datanode registry to disks table of cfs db
func (s *VolMgrServer) DatanodeRegistry(ctx context.Context, in *vp.DatanodeRegistryReq) (*vp.DatanodeRegistryAck, error) {
	ack := vp.DatanodeRegistryAck{}
	dnIP := utils.InetNtoa(in.Ip)
	ip := dnIP.String()

	sql := "insert into disks(ip,port,mount,total, free, statu) values(?, ?, ?, ?, ?, ?)"
	args := utils.ConvertValueToArgs(ip, in.Port, in.MountPoint, in.Capacity, in.Capacity, 0)

	if ret, _ := dr.Exec(sql, args...); ret != 0 {
		logger.Debug("The DataNode(%s:%d:%s) Registry to Db Failed!", ip, in.Port, in.MountPoint)
		ack.Ret = -1
		return &ack, nil
	} else {
		logger.Debug("The DataNode(%s:%d:%s) Registry to Db Success!", ip, in.Port, in.MountPoint)
		ack.Ret = 0
		return &ack, nil
	}
}

// DatanodeHeartbeat : each datanode heartbeat to db
func (s *VolMgrServer) DatanodeHeartbeat(ctx context.Context, in *vp.DatanodeHeartbeatReq) (*vp.DatanodeHeartbeatAck, error) {
	ack := vp.DatanodeHeartbeatAck{}
	ipnr := utils.InetNtoa(in.Ip)
	ip := ipnr.String()

	logger.Debug("The disks(%s:%d) heartbeat info(used:%d -- free:%d -- statu:%d)", ip, in.Port, in.Used, in.Free, in.Status)

	sql := "update disks set used=? where ip=? and port=?"
	args := utils.ConvertValueToArgs(in.Used, ip, in.Port)

	if ret, _ := dr.Exec(sql, args...); ret != 0 {
		logger.Error("The disk(%s:%d) heartbeat update to db error", ip, in.Port)
	}

	checkandupdatediskstatu(ip, int(in.Port), int(in.Status))
	return &ack, nil
}

// CreateVol : Creat a Volume for Users
//CreateVolume ...
type DNode struct {
	Host string
	Port string
}

func (s *VolMgrServer) CreateVol(ctx context.Context, in *vp.CreateVolReq) (*vp.CreateVolAck, error) {
	ack := vp.CreateVolAck{}
	voluuid, err := utils.GenUUID()
	if err != nil {
		logger.Error("Create volume uuid err:%v", err)
		ack.Ret = 1
		return &ack, err
	}

	//the volume need block group total numbers
	var blkgrpnum int32
	if in.SpaceQuota % BlkSizeG == 0 {
		blkgrpnum = in.SpaceQuota/BlkSizeG
	} else {
		blkgrpnum = in.SpaceQuota/BlkSizeG + 1
		in.SpaceQuota = blkgrpnum * BlkSizeG
	}

	if blkgrpnum > 6 {
		blkgrpnum = 6
	}

	// insert the volume info to volumes tables
	sql := "insert into volumes(uuid, name, size, metadomain) values(?, ?, ?, ?)"
	args := utils.ConvertValueToArgs(voluuid, in.VolName, in.SpaceQuota, in.MetaDomain)
	ret, raftgroupid := dr.Exec(sql, args...)
	if ret != 0 {
		logger.Error("Create volume(%s -- %s) insert volumes table error", in.VolName, voluuid)
		ack.Ret = -1
		return &ack, err
	}

	//allocate block group for the volume
	disks_sql := "select ip,port from (select * from disks where free > total*0.1 and statu = 0 order by rand())t  group by ip order by rand() limit 3 for update"
	//disks_sql := "select ip,port from disks where free > total*0.1 and statu = 0 limit 3 for update"
	blk_sql := "insert into blk(hostip, hostport, disabled, volid) values(?, ?, 0, ?)"
	blkgrp_sql := "insert into blkgrp(blks, volume_uuid) values(?, ?)"
	disk_sql := "update disks set free=free-5 where ip=? and port=?"
	
	for i := int32(0); i < blkgrpnum; i++ {
		dNode := DNode{"ip", "port"}
		result, err := dr.Select(disks_sql, &dNode)
		if err != nil {
			logger.Error("Select Replica DataNode Group error:%v for Create Volume:%v", err, voluuid)
			ack.Ret = -1
			return &ack, err
		}

		var count int
		var blks string
		for _, v := range result {
			dnode := v.(DNode)
			args = utils.ConvertValueToArgs(dnode.Host, dnode.Port, voluuid)
			ret, blkid := dr.Exec(blk_sql, args...)
			if ret != 0 {
				logger.Error("insert blk table error for volume:%v", voluuid)
				ack.Ret = -1
				return &ack, nil
			}

			args = utils.ConvertValueToArgs(dnode.Host, dnode.Port)
			if ret, _ = dr.Exec(disk_sql, args...); ret != 0 {
				logger.Error("The disk(%s:%d) update freesize(-5G) to db error", dnode.Host, dnode.Port)
				ack.Ret = -1
				return &ack, nil
			}

			blks = blks + strconv.FormatInt(blkid, 10) + ","
			count++
		}

		logger.Debug("The volume(%s -- %s) one blkgroup have blks:%s", in.VolName, voluuid, blks)

		args = utils.ConvertValueToArgs(blks, voluuid)
		if ret, _ := dr.Exec(blkgrp_sql, args...); ret != 0 {
			logger.Error("Creat Volume:%v insert blks to blkgrp err", voluuid)
			ack.Ret = -1
			return &ack, nil
		}

		if count != 3 {
			logger.Error("Create The volume(%s -- %s) one blkgroup not equal 3 blk(%s), so create volume failed!", in.VolName, voluuid, count)
			cleanRS(voluuid)
			ack.Ret = -1
			return &ack, err
		}
	}

	ack.Ret = 0 //success
	ack.UUID = voluuid
	ack.RaftGroupID = uint64(raftgroupid)
	return &ack, nil
}

// ExpandVol : extent a Volume real size for fuseclient
type Volume struct {
	TSize  string
} 
func (s *VolMgrServer) ExpandVolRS(ctx context.Context, in *vp.ExpandVolRSReq) (*vp.ExpandVolRSAck, error) {
	ack := vp.ExpandVolRSAck{}
	voluuid := in.VolID
	urs := in.UsedRS

	sql := "select size from volumes where uuid=?"
	tVolume := Volume{"size"}
	targ := utils.ConvertValueToArgs(voluuid)
	r, err := dr.Select(sql, &tVolume, targ...)
	if err != nil {
		return &ack, err
	}
	ts := r[0].(Volume)
	trs, _ := strconv.Atoi(ts.TSize)

	bgNums := urs / OneExpandSize + 1
	volsize := uint64(trs) * 1024*1024*1024 - bgNums * OneExpandSize
	if volsize <= 0 {
		ack.Ret = 0
		return &ack, nil
	}

	//the volume need block group total numbers
	var blkgrpnum uint64
	
	if volsize%BlkSize == 0 {
		blkgrpnum = volsize/BlkSize
	} else {
		blkgrpnum = volsize/BlkSize + 1
	}

	if  blkgrpnum > 6 {
		volsize = OneExpandSize
		blkgrpnum = 6
	}

	pBlockGroups := []*vp.BlockGroup{}
	//allocate block group for the volume
	disks_sql := "select ip,port from (select * from disks where free > total*0.1 and statu = 0 order by rand())t  group by ip order by rand() limit 3 for update"
	//disks_sql := "select ip,port from disks where free > total*0.1 and statu = 0 limit 3 for update"
	blk_sql := "insert into blk(hostip, hostport, disabled, volid) values(?, ?, 0, ?)"
	blkgrp_sql := "insert into blkgrp(blks, volume_uuid) values(?, ?)"
	disk_sql := "update disks set free=free-5 where ip=? and port=?"

	for i := uint64(0); i < blkgrpnum; i++ {
		dNode := DNode{"ip", "port"}
		result, err := dr.Select(disks_sql, &dNode)
		if err != nil {
			logger.Error("Select Replica DataNode Group error:%v for Expand Volume:%v", err, voluuid)
			cleanBlk("", pBlockGroups)
			ack.Ret = -1
			return &ack, err
		}

		var count int
		var blks string
		pBlockInfos := []*vp.BlockInfo{}
		for _, v := range result {
			tmpBlockInfo := vp.BlockInfo{}
			dnode := v.(DNode)
			dport, _ := strconv.Atoi(dnode.Port)
			args := utils.ConvertValueToArgs(dnode.Host, dport, voluuid)
			ret, blkid := dr.Exec(blk_sql, args...)
			if ret != 0 {
				logger.Error("insert blk table error for volume:%v", voluuid)
				cleanBlk("", pBlockGroups)
				ack.Ret = -1
				return &ack, nil
			}

			tmpBlockInfo.BlockID = uint32(blkid)
			ipnr := net.ParseIP(dnode.Host)
			ipint := utils.InetAton(ipnr)
			tmpBlockInfo.DataNodeIP = ipint

			tmpBlockInfo.DataNodePort = int32(dport)
			pBlockInfos = append(pBlockInfos, &tmpBlockInfo)

			blks = blks + strconv.FormatInt(blkid, 10) + ","
			count++

			args = utils.ConvertValueToArgs(dnode.Host, dport)
			if ret, _ = dr.Exec(disk_sql, args...); ret != 0 {
				logger.Error("The disk(%s:%d) update freesize(-5G) for ExpandVol:%v size:%v to db error", dnode.Host, dnode.Port, voluuid, volsize)
				cleanBlk(blks, pBlockGroups)
				ack.Ret = -1
				return &ack, nil
			}
		}
		logger.Debug("The Expand volume:%v once size:%v one blkgroup have blks:%s", voluuid, volsize, blks)

		if count != 3 {
			logger.Error("Expand The volume:%v size:%v one blkgroup not equal 3 blk(%s), so create volume failed!", voluuid, volsize, count)
			ack.Ret = -1
			cleanBlk(blks, pBlockGroups)
			return &ack, err
		}

		args := utils.ConvertValueToArgs(blks, voluuid)
		ret, blkgrpid := dr.Exec(blkgrp_sql, args...)
		if ret != 0 {
			logger.Error("Expand Volume:%v insert blks to blkgrp err", voluuid)
			ack.Ret = -1
			cleanBlk(blks, pBlockGroups)
			return &ack, nil
		}

		tmpBlockGroup := vp.BlockGroup{}
		tmpBlockGroup.BlockGroupID = uint32(blkgrpid)
		tmpBlockGroup.BlockInfos = pBlockInfos
		pBlockGroups = append(pBlockGroups, &tmpBlockGroup)
	}

	logger.Debug("Expand volume:%v once Size:%v Success", voluuid, volsize)
	ack.Ret = 1 //success
	ack.BlockGroups = pBlockGroups
	return &ack, nil
}

// Expand volume total size for CLI
func (s *VolMgrServer) ExpandVolTS(ctx context.Context, in *vp.ExpandVolTSReq) (*vp.ExpandVolTSAck, error) {
	ack := vp.ExpandVolTSAck{}
	voluuid := in.VolID
	volsize := in.ExpandQuota

	var blkgrpnum int32
	if volsize % BlkSizeG == 0 {
		blkgrpnum = volsize/BlkSizeG
	} else {
		blkgrpnum = volsize/BlkSizeG + 1
	}

	volsize = blkgrpnum * BlkSizeG

	// update the volume info to volumes tables
	sql := "update volumes set size = size + ? where uuid = ?"
	args := utils.ConvertValueToArgs(volsize, voluuid)
	if ret, _ := dr.Exec(sql, args...); ret != 0 {
		logger.Error("Extent volume:%v Size:%v Update volumes table error", voluuid, volsize)
		ack.Ret = -1
		return &ack, nil
	}

	logger.Debug("Expand volume:%v Size:%v to db Success", voluuid, volsize)
	ack.Ret = 0 //success
	return &ack, nil
}

func updateBlkOnDiskFreeSize(blkid uint32) int {
	sql := "select hostip, hostport from blk where blkid=?"
	dNode := DNode{"hostip", "hostport"}
	args := utils.ConvertValueToArgs(blkid)
	result, err := dr.Select(sql, &dNode, args...)
	if err != nil || len(result) == 0 {
		logger.Error("select blk ip and port for updateBlkOnDiskFreeSize error:%v",err)
		return -1
	}

	dnode := result[0].(DNode)
	dport, _ := strconv.Atoi(dnode.Port)
	disk_sql := "update disks set free=free+5 where ip=? and port=?"
	args = utils.ConvertValueToArgs(dnode.Host, dport)
	if ret, _ := dr.Exec(disk_sql, args...); ret != 0 {
		logger.Error("delete blk:%v update the blk on disk freesize error", blkid)
		return -1
	}
	return 0
}

func cleanBlk(blks string, pBlockGroups []*vp.BlockGroup) int {
	blk_sql := "delete from blk where blkid=?"
	blkgrp_sql := "delete from blkgrp where blkgrpid=?"

	if blks != "" {
		blkids := strings.Split(blks, ",")
		for _, ele := range blkids {
			if ele == "" {
				continue
			}
			blkid, _ := strconv.Atoi(ele)

			updateBlkOnDiskFreeSize(uint32(blkid))

			args := utils.ConvertValueToArgs(blkid)
			if ret, _ := dr.Exec(blk_sql, args...); ret != 0 {
				logger.Error("delete blk:%v from blk tables err", blkid)
				return -1
			}
		}
	}

	for k, v := range pBlockGroups {
		args := utils.ConvertValueToArgs(v.BlockGroupID)
		if ret, _ := dr.Exec(blkgrp_sql, args...); ret != 0 {
			logger.Error("delete blkgrp:%v from blkgrp tables err", v.BlockGroupID)
			return -1
		}
		
		updateBlkOnDiskFreeSize(v.BlockInfos[k].BlockID)

		args = utils.ConvertValueToArgs(v.BlockInfos[k].BlockID)
		if ret, _ := dr.Exec(blk_sql, args...); ret != 0 {
			logger.Error("delete blk:%v from blk tables err", v.BlockInfos[k].BlockID)
			return -1
		}
	}
	return 0
}

func cleanRS(volid string) int {
	//delete blkgroup table
	sql := "delete from blkgrp where volume_uuid=?"
	args := utils.ConvertValueToArgs(volid)
	if ret, _ := dr.Exec(sql, args...); ret != 0 {
		logger.Error("delete volume:%v from blkgrp tables err", volid)
		return -1
	}

	sql = "select hostip, hostport from blk where volid=?"
	dNode := DNode{"hostip", "hostport"}
	args = utils.ConvertValueToArgs(volid)
	result, err := dr.Select(sql, &dNode, args...)
	if err != nil {
		return -1
	}

	for _, v := range result {
		dnode := v.(DNode)
		dport, _ := strconv.Atoi(dnode.Port)
		disk_sql := "update disks set free=free+5 where ip=? and port=?"
		arg := utils.ConvertValueToArgs(dnode.Host, dport)
		if ret, _ := dr.Exec(disk_sql, arg...); ret != 0 {
			logger.Error("delete volume:%v update all blk on disks freesize error", volid)
			return -1
		}
	}

	//delete blk table
	sql = "delete from blk where volid=?"
	if ret, _ := dr.Exec(sql, args...); ret != 0 {
		logger.Error("delete volume:%v from blk tables err", volid)
		return -1
	}

	//delete volumes table
	sql = "delete from volumes where uuid=?"
	if ret, _ := dr.Exec(sql, args...); ret != 0 {
		logger.Error("delete volume:%v from volumes tables err", volid)
		return -1
	}

	logger.Debug("== Delete db tables data success for volume:%v", volid)
	return 0
}

//DeleteVol : Delete a Volume for User
func (s *VolMgrServer) DeleteVol(ctx context.Context, in *vp.DeleteVolReq) (*vp.DeleteVolAck, error) {
	ack := vp.DeleteVolAck{}
	volid := in.UUID

	if ret := cleanRS(volid); ret != 0 {
		logger.Debug("== Delete db tables data failed for volume:%v", volid)
		ack.Ret = -1
	} else {
		logger.Debug("== Delete db tables data success for volume:%v", volid)
		ack.Ret = 0
	}

	return &ack, nil
}

//UpdateChunkInfo : Meta send need repair chunk, if the chunk have repair complete, ack to Meta
func (s *VolMgrServer) UpdateChunkInfo(ctx context.Context, in *vp.UpdateChunkInfoReq) (*vp.UpdateChunkInfoAck, error) {
	ack := vp.UpdateChunkInfoAck{}

	sql := "insert into repair(volid,blkgrpid,blkid,blkip,blkport,chkid,status,position,inode) values(?, ?, ?, ?, ?, ?, ?, ?,?)"
	args := utils.ConvertValueToArgs(in.VolID, in.BlockGroupID, in.BlockID, in.Ip, in.Port, in.ChunkID, in.Status, in.Position, in.Inode)
	if ret, _ := dr.Exec(sql, args...); ret != 0 {
		logger.Error("insert need repair volid:%v - blk:%v - chunk:%v to repair table error!", in.VolID, in.BlockID, in.ChunkID)
		ack.Ret = -1
		return &ack, nil
	}

	ack.Ret = 0
	return &ack, nil
}

//GetVolInfo : Get a Volume Info for User
type VolInfo struct {
	Name       string
	Size       string
	MetaDomain string
}

type BlkGrpInfo struct {
	BGrpID string
	Blks   string
}

type BlkInfo struct {
	Ip   string
	Port string
}

func (s *VolMgrServer) GetVolInfo(ctx context.Context, in *vp.GetVolInfoReq) (*vp.GetVolInfoAck, error) {
	ack := vp.GetVolInfoAck{}
	var volInfo vp.VolInfo
	voluuid := in.UUID

	sql := "select name,size,metadomain from volumes where uuid = ?"
	args := utils.ConvertValueToArgs(voluuid)
	tVolInfo := VolInfo{"name", "size", "metadomain"}
	result, err := dr.Select(sql, &tVolInfo, args...)
	if err != nil || len(result) != 1 {
		logger.Error("Get volume(%s) from db error:%s", voluuid, err)
		ack.Ret = 1
		return &ack, err
	}

	v := result[0].(VolInfo)
	volInfo.VolID = voluuid
	volInfo.VolName = v.Name
	size, _ := strconv.Atoi(v.Size)
	volInfo.SpaceQuota = int32(size)
	volInfo.MetaDomain = v.MetaDomain

	sql = "select blkgrpid,blks from blkgrp where volume_uuid = ?"
	tBGrpInfo := BlkGrpInfo{"blkgrpid", "blks"}
	result, err = dr.Select(sql, &tBGrpInfo, args...)
	if err != nil {
		logger.Error("Get blkgroups for volume(%s) error:%s", voluuid, err)
		ack.Ret = 1
		return &ack, err
	}

	pBlockGroups := []*vp.BlockGroup{}
	for _, v := range result {
		tbgpinfo := v.(BlkGrpInfo)

		logger.Debug("Get blks:%s in blkgroup:%v for volume(%s)", tbgpinfo.Blks, tbgpinfo.BGrpID, voluuid)
		blkids := strings.Split(tbgpinfo.Blks, ",")

		pBlockInfos := []*vp.BlockInfo{}
		for _, ele := range blkids {
			if ele == "" {
				continue
			}
			blkid, _ := strconv.Atoi(ele)
			blk_sql := "select hostip, hostport from blk where blkid = ?"
			arg := utils.ConvertValueToArgs(blkid)
			tBlkInfo := BlkInfo{"hostip", "hostport"}
			ret, err := dr.Select(blk_sql, &tBlkInfo, arg...)
			if err != nil || len(ret) != 1 {
				logger.Error("Get each blk:%d on which host error:%s for volume(%s)", blkid, err, voluuid)
				ack.Ret = 1
				return &ack, nil
			}
			tblkinfo := ret[0].(BlkInfo)

			tmpBlockInfo := vp.BlockInfo{}
			tmpBlockInfo.BlockID = uint32(blkid)
			ipnr := net.ParseIP(tblkinfo.Ip)
			ipint := utils.InetAton(ipnr)
			tmpBlockInfo.DataNodeIP = ipint
			port, _ := strconv.Atoi(tblkinfo.Port)
			tmpBlockInfo.DataNodePort = int32(port)
			pBlockInfos = append(pBlockInfos, &tmpBlockInfo)
		}
		tmpBlockGroup := vp.BlockGroup{}
		bgrpid, _ := strconv.Atoi(tbgpinfo.BGrpID)
		tmpBlockGroup.BlockGroupID = uint32(bgrpid)
		tmpBlockGroup.BlockInfos = pBlockInfos
		pBlockGroups = append(pBlockGroups, &tmpBlockGroup)
	}
	volInfo.BlockGroups = pBlockGroups
	logger.Debug("Get info:%v for the volume(%s)", volInfo, voluuid)
	ack = vp.GetVolInfoAck{Ret: 0, VolInfo: &volInfo}
	return &ack, nil
}

type Vols struct {
	RaftGroupID string
	VolID       string
}

//GetVolList : get all volume list
func (s *VolMgrServer) GetVolList(ctx context.Context, in *vp.GetVolListReq) (*vp.GetVolListAck, error) {
	ack := vp.GetVolListAck{}

	sql := "select raftgroupid,uuid from volumes"
	tVols := Vols{"raftgrpid", "uuid"}
	result, err := dr.Select(sql, &tVols)
	if err != nil {
		logger.Error("Get volumes from db error:%v", err)
		ack.Ret = 1
		return &ack, err
	}

	pVolIDs := []*vp.VolIDs{}
	for _, v := range result {
		tvols := v.(Vols)

		tmpVolIDs := vp.VolIDs{}
		tmpVolIDs.UUID = tvols.VolID
		raftgrpid, _ := strconv.Atoi(tvols.RaftGroupID)
		tmpVolIDs.RaftGroupID = uint64(raftgrpid)
		pVolIDs = append(pVolIDs, &tmpVolIDs)
	}
	ack.Ret = 0
	ack.VolIDs = pVolIDs
	return &ack, nil
}

type Disk struct {
	Status string
}

func checkandupdatediskstatu(ip string, port int, statu int) {
	sql := "select statu from disks where ip=? and port=?"
	args := utils.ConvertValueToArgs(ip, port)
	tDisk := Disk{"statu"}
	result, err := dr.Select(sql, &tDisk, args...)
	if err != nil {
		logger.Error("Get from disks table for check disk status error:%s", err)
		return
	}
	tdisk := result[0].(Disk)
	dbstatu, _ := strconv.Atoi(tdisk.Status)

	if dbstatu == 0 && statu == 2 {
		updateDataNodeStatu(ip, port, 2)
	} else if dbstatu == 2 && statu == 0 {
		updateDataNodeStatu(ip, port, 0)
	} else {
		return
	}
}

func detectdatanode(ip string, port int, statu int) {
	dnAddr := ip + ":" + strconv.Itoa(port)
	conn, err := grpc.Dial(dnAddr, grpc.WithInsecure())
	if err != nil {
		logger.Error("Detect DataNode:%v failed : Dial to datanode failed !", dnAddr)
		if statu == 0 {
			updateDataNodeStatu(ip, port, 1)
		}
		Wg.Add(-1)
		return
	}
	defer conn.Close()
	c := dp.NewDataNodeClient(conn)
	var DatanodeHealthCheckReq dp.DatanodeHealthCheckReq
	pDatanodeHealthCheckAck, err := c.DatanodeHealthCheck(context.Background(), &DatanodeHealthCheckReq)
	if err != nil {
		if statu == 0 {
			updateDataNodeStatu(ip, port, 1)
		}
		Wg.Add(-1)
		return
	}
	if pDatanodeHealthCheckAck.Ret == 1 && statu == 1 {
		updateDataNodeStatu(ip, port, 0)
		Wg.Add(-1)
		return
	}
}

func updateDataNodeStatu(ip string, port int, statu int) {
	sql := "update disks set statu=? where ip=? and port=?"
	args := utils.ConvertValueToArgs(statu, ip, port)
	if ret, _ := dr.Exec(sql, args...); ret != 0 {
		logger.Error("The disk(%s:%d) update statu:%v to db error", ip, port, statu)
		return
	}

	args = utils.ConvertValueToArgs(ip, port)
	if statu == 1 || statu == 2 {
		sql = "update blk set disabled=1 where hostip=? and hostport=?"
		if ret, _ := dr.Exec(sql, args...); ret != 0 {
			logger.Error("The disk(%s:%d) bad statu:%d update blk table disabled error", ip, port, statu)
			return
		}
		logger.Debug("The disk(%s:%d) bad statu:%d, so make it all blks is disabled, and update metadata for allocated blks", ip, port, statu)
	} else if statu == 0 {
		sql = "update blk set disabled=0 where hostip=? and hostport=?"
		if ret, _ := dr.Exec(sql, args...); ret != 0 {
			logger.Error("The disk(%s:%d) recovy , but update blk table able error", ip, port, statu)
			return
		}
		logger.Debug("The disk(%s:%d) recovy,so update from 1 to 0, make it all blks is able", ip, port, statu)
	}
	return
}

type Disks struct {
	Ip     string
	Port   string
	Status string
}

func detectDataNodes() {
	sql := "select ip,port,statu from disks"
	tDisks := Disks{"ip", "port", "statu"}
	result, err := dr.Select(sql, &tDisks)
	if err != nil {
		logger.Error("Get from disks table for all disks error:%s", err)
		return
	}

	for _, v := range result {
		tdisks := v.(Disks)
		port, _ := strconv.Atoi(tdisks.Port)
		statu, _ := strconv.Atoi(tdisks.Status)
		Wg.Add(1)
		go detectdatanode(tdisks.Ip, port, statu)
	}
}

// StartVolMgrService ...
func StartVolMgrService() {

	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", VolMgrServerAddr.port))
	if err != nil {
		panic(fmt.Sprintf("Failed to listen on:%v", VolMgrServerAddr.port))
	}
	s := grpc.NewServer()
	vp.RegisterVolMgrServer(s, &VolMgrServer{})
	// Register reflection service on gRPC server.
	reflection.Register(s)
	if err := s.Serve(lis); err != nil {
		panic("Failed to serve")
	}
}

// MdcServer ...
type MdcServer struct{}

//FetchMeters ...
func (s *MdcServer) FetchMeters(ctx context.Context, in *vp.MdcRequest) (*vp.Meters, error) {
	ack := vp.Meters{}
	meter := vp.Meter{}

	meter.Name = "Service Status"
	meter.Volume = 0
	meter.Resource = "volmgr#" + VolMgrServerAddr.host
	meter.IP = VolMgrServerAddr.host
	meter.Timestamp = ""
	meter.Type = "gague"

	ack.Meters = append(ack.Meters, &meter)
	return &ack, nil
}

// StarMdcService ...
func StarMdcService() {

	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", VolMgrServerAddr.port+10))
	if err != nil {
		panic(fmt.Sprintf("Failed to listen on:%v", VolMgrServerAddr.port))
	}
	s := grpc.NewServer()
	vp.RegisterMdcServiceServer(s, &MdcServer{})
	// Register reflection service on gRPC server.
	reflection.Register(s)
	if err := s.Serve(lis); err != nil {
		panic("Failed to serve")
	}
}

func init() {

	flag.StringVar(&VolMgrServerAddr.host, "host", "127.0.0.1", "ContainerFS VolMgr Host")
	flag.IntVar(&VolMgrServerAddr.port, "port", 8000, "ContainerFS VolMgr Port")
	flag.StringVar(&VolMgrServerAddr.log, "log", "/export/Logs/containerfs/logs/", "ContainerFS VolMgr logpath")
	loglevel := flag.String("loglevel", "error", "ContainerFS VolMgr log level")
	flag.StringVar(&mysqlConf.dbhost, "sqlhost", "127.0.0.1:3306", "ContainerFS DBHOST")
	flag.StringVar(&mysqlConf.dbusername, "sqluser", "root", "ContainerFS DBUSER")
	flag.StringVar(&mysqlConf.dbpassword, "sqlpasswd", "root", "ContainerFS DBPASSWD")
	flag.StringVar(&mysqlConf.dbname, "sqldb", "containerfs", "ContainerFS DB")

	flag.Parse()

	os.MkdirAll(VolMgrServerAddr.log, 0777)

	logger.SetConsole(true)
	logger.SetRollingFile(VolMgrServerAddr.log, "volmgr.log", 10, 100, logger.MB) //each 100M rolling
	switch *loglevel {
	case "error":
		logger.SetLevel(logger.ERROR)
	case "debug":
		logger.SetLevel(logger.DEBUG)
	case "info":
		logger.SetLevel(logger.INFO)
	default:
		logger.SetLevel(logger.ERROR)
	}
	var err error
	dr.DB, err = sql.Open("mysql", mysqlConf.dbusername+":"+mysqlConf.dbpassword+"@tcp("+mysqlConf.dbhost+")/"+mysqlConf.dbname+"?charset=utf8")
	checkErr("init open db", err)
	err = dr.DB.Ping()
	checkErr("init ping db", err)

}
func main() {

	//for multi-cpu scheduling
	numCPU := runtime.NumCPU()
	runtime.GOMAXPROCS(numCPU)

	defer func() {
		if err := recover(); err != nil {
			logger.Error("panic !!! :%v", err)
		}
		logger.Error("stacks:%v", string(debug.Stack()))
	}()

	ticker := time.NewTicker(time.Second * 60)
	go func() {
		for range ticker.C {
			detectDataNodes()
		}
	}()
	Wg.Wait()
	defer dr.DB.Close()
	go StartVolMgrService()
	go StarMdcService()

	loop := make(chan int)
	<-loop
}
