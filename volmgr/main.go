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
	BlkSize = 10 /*G*/
)

// Mutex var g_RpcConfig RpcConfigOpts
var Mutex sync.RWMutex
var err string

// VolMgrServer ...
type VolMgrServer struct{}

// VolMgrDB ...
var VolMgrDB *sql.DB

func checkErr(err error) {
	if err != nil {
		logger.Error("%s", err)
	}
}

// DatanodeRegistry : datanode registry to disks table of cfs db
func (s *VolMgrServer) DatanodeRegistry(ctx context.Context, in *vp.DatanodeRegistryReq) (*vp.DatanodeRegistryAck, error) {
	ack := vp.DatanodeRegistryAck{}
	dnIP := utils.InetNtoa(in.Ip)
	ip := dnIP.String()
	dnPort := in.Port
	dnMount := in.MountPoint
	dnCapacity := in.Capacity

	disk, err := VolMgrDB.Prepare("INSERT INTO disks(ip,port,mount,total,statu) VALUES(?, ?, ?, ?, ?)")
	if err != nil {
		logger.Error("DataNode(%v:%v) Registry insert into disks table prepare err:%v", ip, dnPort, err)
		ack.Ret = -1
		return &ack, err
	}
	defer disk.Close()

	_, err = disk.Exec(ip, dnPort, dnMount, dnCapacity, 0)
	if err != nil {
		logger.Error("DataNode(%v:%v) Registry insert into disks table exec err:%v", ip, dnPort, err)
		ack.Ret = -1
		return &ack, err
	}

	logger.Debug("The disk(%s:%s) mount:%s have registry success", ip, dnPort, dnMount)
	ack.Ret = 0 //success
	return &ack, nil
}

// DatanodeHeartbeat : each datanode heartbeat to db
func (s *VolMgrServer) DatanodeHeartbeat(ctx context.Context, in *vp.DatanodeHeartbeatReq) (*vp.DatanodeHeartbeatAck, error) {
	ack := vp.DatanodeHeartbeatAck{}
	port := in.Port
	used := in.Used
	free := in.Free
	statu := in.Status
	ipnr := utils.InetNtoa(in.Ip)
	ip := ipnr.String()

	logger.Debug("The disks(%s:%d) heartbeat info(used:%d -- free:%d -- statu:%d)", ip, port, used, free, statu)
	disk, err := VolMgrDB.Prepare("UPDATE disks SET used=?,free=? WHERE ip=? and port=?")
	checkErr(err)
	defer disk.Close()
	_, err = disk.Exec(used, free, ip, port)
	if err != nil {
		logger.Error("The disk(%s:%d) heartbeat update to db error:%s", ip, port, err)
		return &ack, nil
	}

	checkandupdatediskstatu(ip, int(port), int(statu))
	return &ack, nil
}

// CreateVol : Creat a Volume for Users
func (s *VolMgrServer) CreateVol(ctx context.Context, in *vp.CreateVolReq) (*vp.CreateVolAck, error) {
	ack := vp.CreateVolAck{}
	volname := in.VolName
	volsize := in.SpaceQuota
	metadomain := in.MetaDomain
	voluuid, err := utils.GenUUID()
	if err != nil {
		logger.Error("Create volume uuid err:%v", err)
		ack.Ret = 1
		return &ack, err
	}

	//the volume need block group total numbers
	var blkgrpnum int32
	if volsize < BlkSize {
		blkgrpnum = 1
	} else if volsize%BlkSize == 0 {
		blkgrpnum = volsize / BlkSize
	} else {
		blkgrpnum = volsize/BlkSize + 1
	}

	// insert the volume info to volumes tables
	vol, err := VolMgrDB.Prepare("INSERT INTO volumes(uuid, name, size,metadomain) VALUES(?, ?, ?, ?)")
	if err != nil {
		logger.Error("Create volume(%s -- %s) insert volumes table error:%s", volname, voluuid, err)
		ack.Ret = 1 // db error
		return &ack, err
	}
	defer vol.Close()
	r, err := vol.Exec(voluuid, volname, volsize, metadomain)
	if err != nil {
		ack.Ret = 1
		return &ack, err
	}
	raftgroupid, err := r.LastInsertId()
	if err != nil {
		ack.Ret = 1
		return &ack, err
	}

	//allocate block group for the volume
	for i := int32(0); i < blkgrpnum; i++ {
		rows, err := VolMgrDB.Query("select ip,port from (select * from disks WHERE free > 10 and statu = 0 order by rand())t  group by ip order BY rand() limit 3 for update")
		if err != nil {
			logger.Error("Create volume(%s -- %s) select blk for the %dth blkgroup error:%s", volname, voluuid, i, err)
			ack.Ret = 1
			return &ack, err
		}
		defer rows.Close()

		var ip string
		var port int
		var count int
		var blks string
		for rows.Next() {
			err := rows.Scan(&ip, &port)
			if err != nil {
				ack.Ret = 1
				return &ack, err
			}

			blk, err := VolMgrDB.Prepare("insert into blk(hostip, hostport, disabled, volid) values(?, ?, 0, ?)")
			if err != nil {
				logger.Error("insert blk table error:%s", err)
				ack.Ret = 1
				return &ack, err
			}
			defer blk.Close()
			result, err := blk.Exec(ip, port, voluuid)
			if err != nil {
				ack.Ret = 1
				return &ack, err
			}
			blkid, err := result.LastInsertId()
			if err != nil {
				ack.Ret = 1
				return &ack, err
			}

			blks = blks + strconv.FormatInt(blkid, 10) + ","
			count++
		}
		logger.Debug("The volume(%s -- %s) one blkgroup have blks:%s", volname, voluuid, blks)

		blkgrp, err := VolMgrDB.Prepare("INSERT INTO blkgrp(blks, volume_uuid) VALUES(?, ?)")
		if err != nil {
			logger.Error("Creat Volume:%v insert blks to blkgrp prepare err:%v", voluuid, err)
			ack.Ret = 1
			return &ack, err
		}
		defer blkgrp.Close()
		blkgrp.Exec(blks, voluuid)

		if count != 3 {
			logger.Error("Create The volume(%s -- %s) one blkgroup not equal 3 blk(%s), so create volume failed!", volname, voluuid, count)
			cleanRS(voluuid)
			ack.Ret = 1
			return &ack, err
		}
	}

	ack.Ret = 0 //success
	ack.UUID = voluuid
	ack.RaftGroupID = uint64(raftgroupid)
	return &ack, nil
}

// ExpendVol : extent a Volume size
func (s *VolMgrServer) ExpendVol(ctx context.Context, in *vp.ExpendVolReq) (*vp.ExpendVolAck, error) {
	ack := vp.ExpendVolAck{}
	voluuid := in.VolID
	volsize := in.ExpendQuota

	//the volume need block group total numbers
	var blkgrpnum int32
	if volsize < BlkSize {
		blkgrpnum = 1
	} else if volsize%BlkSize == 0 {
		blkgrpnum = volsize / BlkSize
	} else {
		blkgrpnum = volsize/BlkSize + 1
	}

	pBlockGroups := []*vp.BlockGroup{}
	//allocate block group for the volume
	for i := int32(0); i < blkgrpnum; i++ {
		rows, err := VolMgrDB.Query("select ip,port from (select * from disks WHERE free > 10 and statu = 0 order by rand())t  group by ip order BY rand() limit 3 for update")
		if err != nil {
			logger.Error("Expend volume:%v select blk for the %dth blkgroup error:%s", voluuid, i, err)
			ack.Ret = 1
			return &ack, err
		}
		defer rows.Close()

		var ip string
		var port int
		var count int
		var blks string
		pBlockInfos := []*vp.BlockInfo{}
		for rows.Next() {
			tmpBlockInfo := vp.BlockInfo{}
			err := rows.Scan(&ip, &port)
			if err != nil {
				ack.Ret = 1
				return &ack, err
			}

			blk, err := VolMgrDB.Prepare("insert into blk(hostip, hostport, disabled, volid) values(?, ?, 0, ?)")
			if err != nil {
				logger.Error("Expend insert blk table error:%s", err)
				ack.Ret = 1
				return &ack, err
			}
			defer blk.Close()
			result, err := blk.Exec(ip, port, voluuid)
			if err != nil {
				ack.Ret = 1
				return &ack, err
			}
			blkid, err := result.LastInsertId()
			if err != nil {
				ack.Ret = 1
				return &ack, err
			}

			tmpBlockInfo.BlockID = uint32(blkid)
			ipnr := net.ParseIP(ip)
			ipint := utils.InetAton(ipnr)
			tmpBlockInfo.DataNodeIP = ipint
			tmpBlockInfo.DataNodePort = int32(port)
			pBlockInfos = append(pBlockInfos, &tmpBlockInfo)

			blks = blks + strconv.FormatInt(blkid, 10) + ","
			count++
		}
		logger.Debug("The Expend volume:%v size:%v one blkgroup have blks:%s", voluuid, volsize, blks)

		if count != 3 {
			logger.Error("Expend The volume:%v size:%v one blkgroup not equal 3 blk(%s), so create volume failed!", voluuid, volsize, count)
			ack.Ret = 1
			cleanBlk(blks, pBlockGroups)
			return &ack, err
		}

		blkgrp, err := VolMgrDB.Prepare("insert into blkgrp(blks, volume_uuid) values(?, ?)")
		if err != nil {
			logger.Error("Expend Volume:%v insert blks to blkgrp prepare err:%v", voluuid, err)
			ack.Ret = 1
			cleanBlk(blks, pBlockGroups)
			return &ack, err
		}
		defer blkgrp.Close()
		result, err := blkgrp.Exec(blks, voluuid)
		if err != nil {
			ack.Ret = 1
			cleanBlk(blks, pBlockGroups)
			return &ack, err
		}
		blkgrpid, err := result.LastInsertId()
		if err != nil {
			ack.Ret = 1
			return &ack, err
		}

		tmpBlockGroup := vp.BlockGroup{}
		tmpBlockGroup.BlockGroupID = uint32(blkgrpid)
		tmpBlockGroup.BlockInfos = pBlockInfos
		pBlockGroups = append(pBlockGroups, &tmpBlockGroup)
	}

	// update the volume info to volumes tables
	vol, err := VolMgrDB.Prepare("update volumes set size = size + ? where uuid = ?")
	if err != nil {
		logger.Error("Extent volume:%v Size:%v prepare volumes table error:%s", voluuid, volsize, err)
	}
	defer vol.Close()
	_, err = vol.Exec(volsize, voluuid)
	if err != nil {
		logger.Error("Extent volume:%v Size:%v exec volumes table error:%s", voluuid, volsize, err)
	}

	logger.Debug("Extent volume:%v Size:%v Success", voluuid, volsize)
	ack.Ret = 0 //success
	ack.BlockGroups = pBlockGroups
	return &ack, nil
}

func cleanBlk(blks string, pBlockGroups []*vp.BlockGroup) int {
	if blks != "" {
		blkids := strings.Split(blks, ",")
		for _, ele := range blkids {
			if ele == "," {
				continue
			}
			blkid, err := strconv.Atoi(ele)
			blk, err := VolMgrDB.Prepare("delete from blk where blkid=?")
			if err != nil {
				logger.Error("delete blk:%v from blk tables prepare err:%v", blkid, err)
				return -1
			}
			defer blk.Close()
			_, err = blk.Exec(blkid)
			if err != nil {
				logger.Error("delete blk:%v from blk tables exec err:%v", blkid, err)
				return -1
			}
		}
	}

	for k, v := range pBlockGroups {
		bgrp, err := VolMgrDB.Prepare("delete from blkgrp where blkgrpid=?")
		if err != nil {
			logger.Error("delete blkgrp:%v from blkgrp tables prepare err:%v", v.BlockGroupID, err)
			return -1
		}
		defer bgrp.Close()
		_, err = bgrp.Exec(v.BlockGroupID)
		if err != nil {
			logger.Error("delete blkgrp:%v from blkgrp tables exec err:%v", v.BlockGroupID, err)
			return -1
		}

		blk, err := VolMgrDB.Prepare("delete from blk where blkid=?")
		if err != nil {
			logger.Error("delete blk:%v from blk tables prepare err:%v", v.BlockInfos[k].BlockID, err)
			return -1
		}
		defer blk.Close()
		_, err = blk.Exec(v.BlockInfos[k].BlockID)
		if err != nil {
			logger.Error("delete blk:%v from blk tables exec err:%v", v.BlockInfos[k].BlockID, err)
			return -1
		}
	}
	return 0
}

func cleanRS(volid string) int {
	//delete blkgroup table
	bgrp, err := VolMgrDB.Prepare("delete from blkgrp where volume_uuid=?")
	if err != nil {
		logger.Error("delete volume:%v from blkgrp tables err:%v", volid, err)
		return -1
	}
	defer bgrp.Close()
	_, err = bgrp.Exec(volid)
	if err != nil {
		return -1
	}

	//delete blk table
	blk, err := VolMgrDB.Prepare("delete from blk where volid=?")
	if err != nil {
		logger.Error("delete volume:%v from blk tables err:%v", volid, err)
		return -1
	}
	defer blk.Close()
	_, err = blk.Exec(volid)
	if err != nil {
		return -1
	}

	//delete volumes table
	vol, err := VolMgrDB.Prepare("delete from volumes where uuid=?")
	if err != nil {
		logger.Error("delete volume:%v from volumes tables err:%v", volid, err)
		return -1
	}
	defer vol.Close()
	_, err = vol.Exec(volid)
	if err != nil {
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
	ip := in.Ip
	port := in.Port
	volid := in.VolID
	blkgrpid := in.BlockGroupID
	blkid := in.BlockID
	chkid := in.ChunkID
	status := in.Status
	position := in.Position
	inode := in.Inode

	rp, err := VolMgrDB.Prepare("insert into repair(volid,blkgrpid,blkid,blkip,blkport,chkid,status,position,inode) values(?, ?, ?, ?, ?, ?, ?, ?,?)")
	if err != nil {
		logger.Error("insert need repair volid:%v - blk:%v - chunk:%v to repair table prepare error:%v!", volid, blkid, chkid, err)
		ack.Ret = -1
		return &ack, err
	}
	defer rp.Close()
	_, err = rp.Exec(volid, blkgrpid, blkid, ip, port, chkid, status, position, inode)
	if err != nil {
		logger.Error("insert need repair volid:%v - blk:%v - chunk:%v to repair table exec error:%v!", volid, blkid, chkid, err)
		ack.Ret = -1
		return &ack, err
	}
	ack.Ret = 0
	return &ack, nil
}

//GetVolInfo : Get a Volume Info for User
func (s *VolMgrServer) GetVolInfo(ctx context.Context, in *vp.GetVolInfoReq) (*vp.GetVolInfoAck, error) {
	ack := vp.GetVolInfoAck{}
	var volInfo vp.VolInfo

	voluuid := in.UUID

	var name string
	var size int32
	var metadomain string
	vols, err := VolMgrDB.Query("SELECT name,size,metadomain FROM volumes WHERE uuid = ?", voluuid)
	if err != nil {
		logger.Error("Get volume(%s) from db error:%s", voluuid, err)
		ack.Ret = 1
		return &ack, err
	}
	defer vols.Close()
	for vols.Next() {
		err = vols.Scan(&name, &size, &metadomain)
		if err != nil {
			ack.Ret = 1
			return &ack, err
		}
		volInfo.VolID = voluuid
		volInfo.VolName = name
		volInfo.SpaceQuota = size
		volInfo.MetaDomain = metadomain
	}

	var blkgrpid int
	var blks string
	blkgrp, err := VolMgrDB.Query("SELECT blkgrpid,blks FROM blkgrp WHERE volume_uuid = ?", voluuid)
	if err != nil {
		logger.Error("Get blkgroups for volume(%s) error:%s", voluuid, err)
		ack.Ret = 1
		return &ack, err
	}
	defer blkgrp.Close()
	pBlockGroups := []*vp.BlockGroup{}
	for blkgrp.Next() {
		err := blkgrp.Scan(&blkgrpid, &blks)
		if err != nil {
			ack.Ret = 1
			return &ack, err
		}
		logger.Debug("Get blks:%s in blkgroup:%d for volume(%s)", blks, blkgrpid, voluuid)
		blkids := strings.Split(blks, ",")

		pBlockInfos := []*vp.BlockInfo{}
		for _, ele := range blkids {
			if ele == "," {
				continue
			}
			blkid, err := strconv.Atoi(ele)
			var hostip string
			var hostport int
			blk, err := VolMgrDB.Query("SELECT hostip,hostport FROM blk WHERE blkid = ?", blkid)
			if err != nil {
				logger.Error("Get each blk:%d on which host error:%s for volume(%s)", blkid, err, voluuid)
				ack.Ret = 1
				return &ack, err
			}
			defer blk.Close()
			for blk.Next() {
				err = blk.Scan(&hostip, &hostport)
				if err != nil {
					ack.Ret = 1
					return &ack, err
				}
				tmpBlockInfo := vp.BlockInfo{}
				tmpBlockInfo.BlockID = uint32(blkid)
				ipnr := net.ParseIP(hostip)
				ipint := utils.InetAton(ipnr)
				tmpBlockInfo.DataNodeIP = ipint
				tmpBlockInfo.DataNodePort = int32(hostport)
				pBlockInfos = append(pBlockInfos, &tmpBlockInfo)
			}
		}
		tmpBlockGroup := vp.BlockGroup{}
		tmpBlockGroup.BlockGroupID = uint32(blkgrpid)
		tmpBlockGroup.BlockInfos = pBlockInfos
		pBlockGroups = append(pBlockGroups, &tmpBlockGroup)
	}
	volInfo.BlockGroups = pBlockGroups
	logger.Debug("Get info:%v for the volume(%s)", volInfo, voluuid)
	ack = vp.GetVolInfoAck{Ret: 0, VolInfo: &volInfo}
	return &ack, nil
}

//GetVolList : get all volume list
func (s *VolMgrServer) GetVolList(ctx context.Context, in *vp.GetVolListReq) (*vp.GetVolListAck, error) {
	ack := vp.GetVolListAck{}

	vols, err := VolMgrDB.Query("SELECT raftgroupid,uuid FROM volumes")
	if err != nil {
		logger.Error("Get volumes from db error:%v", err)
		ack.Ret = 1
		return &ack, err
	}
	defer vols.Close()

	var name string
	var raftgrpid uint64
	pVolIDs := []*vp.VolIDs{}

	for vols.Next() {
		err = vols.Scan(&raftgrpid, &name)
		if err != nil {
			ack.Ret = 1
			return &ack, err
		}
		tmpVolIDs := vp.VolIDs{}
		tmpVolIDs.UUID = name
		tmpVolIDs.RaftGroupID = raftgrpid
		pVolIDs = append(pVolIDs, &tmpVolIDs)
	}
	ack.Ret = 0
	ack.VolIDs = pVolIDs
	return &ack, nil
}

func checkandupdatediskstatu(ip string, port int, statu int) {
	var dbstatu int
	disks, err := VolMgrDB.Query("SELECT statu FROM disks where ip=? and port=?", ip, port)
	if err != nil {
		logger.Error("Get from disks table for all disks error:%s", err)
		return
	}
	defer disks.Close()
	for disks.Next() {
		err = disks.Scan(&dbstatu)
		if err != nil {
			logger.Error("Scan db for get datanodeAddr error:%v", err)
			continue
		}
	}

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

	disk, err := VolMgrDB.Prepare("UPDATE disks SET statu=? WHERE ip=? and port=?")
	if err != nil {
		return
	}

	defer disk.Close()
	_, err = disk.Exec(statu, ip, port)
	if err != nil {
		logger.Error("The disk(%s:%d) update statu:%v to db error:%s", ip, port, statu, err)
		return
	}
	if statu == 1 || statu == 2 {
		logger.Debug("The disk(%s:%d) bad statu:%d, so make it all blks is disabled, and update metadata for allocated blks", ip, port, statu)
		blk, err := VolMgrDB.Prepare("UPDATE blk SET disabled=1 WHERE hostip=? and hostport=?")
		checkErr(err)
		defer blk.Close()
		_, err = blk.Exec(ip, port)
		if err != nil {
			logger.Error("The disk(%s:%d) bad statu:%d update blk table disabled error:%s", ip, port, statu, err)
		}

	} else if statu == 0 {
		logger.Debug("The disk(%s:%d) recovy,so update from 1 to 0, make it all blks is able", ip, port, statu)
		blk, err := VolMgrDB.Prepare("UPDATE blk SET disabled=0 WHERE hostip=? and hostport=?")
		checkErr(err)
		defer blk.Close()
		_, err = blk.Exec(ip, port)
		if err != nil {
			logger.Error("The disk(%s:%d) recovy , but update blk table able error:%s", ip, port, statu, err)
		}

	}
	return
}

func detectDataNodes() {
	var ip string
	var port int
	var statu int
	disks, err := VolMgrDB.Query("SELECT ip,port,statu FROM disks")
	if err != nil {
		logger.Error("Get from disks table for all disks error:%s", err)
		return
	}
	defer disks.Close()
	for disks.Next() {
		err = disks.Scan(&ip, &port, &statu)
		if err != nil {
			logger.Error("Scan db for get datanodeAddr error:%v", err)
			continue
		}
		Wg.Add(1)
		go detectdatanode(ip, port, statu)
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
	VolMgrDB, err = sql.Open("mysql", mysqlConf.dbusername+":"+mysqlConf.dbpassword+"@tcp("+mysqlConf.dbhost+")/"+mysqlConf.dbname+"?charset=utf8")
	checkErr(err)
	err = VolMgrDB.Ping()
	checkErr(err)

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
	defer VolMgrDB.Close()
	go StartVolMgrService()
	go StarMdcService()

	loop := make(chan int)
	<-loop
}
