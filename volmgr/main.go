package main

import (
	"database/sql"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"github.com/ipdcode/containerfs/logger"
	dp "github.com/ipdcode/containerfs/proto/dp"
	mp "github.com/ipdcode/containerfs/proto/mp"
	vp "github.com/ipdcode/containerfs/proto/vp"
	"github.com/ipdcode/containerfs/utils"
	"github.com/lxmgo/config"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
	"net"
	"os"
	"runtime"
	"runtime/debug"
	"sort"
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

// VolMgrServerAddr
var VolMgrServerAddr addr

// MetaNodeAddr
var MetaNodeAddr string

// Wg
var Wg sync.WaitGroup

type mysqlc struct {
	dbhost     string
	dbusername string
	dbpassword string
	dbname     string
}

var mysqlConf mysqlc

const (
	Blksize = 10 /*G*/
)

// Mutexvar g_RpcConfig RpcConfigOpts
var Mutex sync.RWMutex
var err string

// VolMgrServer
type VolMgrServer struct{}

// VolMgrDB
var VolMgrDB *sql.DB

func checkErr(err error) {
	if err != nil {
		logger.Error("%s", err)
	}
}

// DatanodeRegistry
func (s *VolMgrServer) DatanodeRegistry(ctx context.Context, in *vp.DatanodeRegistryReq) (*vp.DatanodeRegistryAck, error) {
	ack := vp.DatanodeRegistryAck{}
	dn_ip := utils.Inet_ntoa(in.Ip)
	ip := dn_ip.String()
	dn_port := in.Port
	dn_mount := in.MountPoint
	dn_capacity := in.Capacity

	disk, err := VolMgrDB.Prepare("INSERT INTO disks(ip,port,mount,total,statu) VALUES(?, ?, ?, ?, ?)")
	checkErr(err)
	defer disk.Close()

	_, err = disk.Exec(ip, dn_port, dn_mount, dn_capacity, 0)
	checkErr(err)

	blkcount := dn_capacity / Blksize

	hostip := ip
	hostport := strconv.Itoa(int(dn_port))
	blk, err := VolMgrDB.Prepare("INSERT INTO blk(hostip, hostport, allocated, disabled,repair) VALUES(?, ?, ?, ?, ?)")
	checkErr(err)
	defer blk.Close()

	VolMgrDB.Exec("lock tables blk write")

	for i := int32(0); i < blkcount; i++ {
		blk.Exec(hostip, hostport, 0, 0, 0)
	}
	VolMgrDB.Exec("unlock tables")

	blkids := make([]int, 0)
	rows, err := VolMgrDB.Query("SELECT blkid FROM blk WHERE hostip = ? and hostport = ?", hostip, hostport)
	checkErr(err)
	defer rows.Close()
	var blkid int
	for rows.Next() {
		err := rows.Scan(&blkid)
		checkErr(err)
		blkids = append(blkids, blkid)
	}

	sort.Ints(blkids)
	logger.Debug("The disk(%s:%d) mount:%s have blks:%v", hostip, hostport, dn_mount, blkids)
	ack.StartBlockID = int32(blkids[0])
	ack.EndBlockID = int32(blkids[len(blkids)-1])
	ack.Ret = 0 //success
	return &ack, nil
}

// DatanodeHeartbeat
func (s *VolMgrServer) DatanodeHeartbeat(ctx context.Context, in *vp.DatanodeHeartbeatReq) (*vp.DatanodeHeartbeatAck, error) {
	ack := vp.DatanodeHeartbeatAck{}
	port := in.Port
	used := in.Used
	free := in.Free
	statu := in.Status
	ipnr := utils.Inet_ntoa(in.Ip)
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
	/*
		if statu != 0 {
			logger.Debug("The disk(%s:%d) bad statu:%d, so make it all blks is disabled", ip, port, statu)
			blk, err := VolMgrDB.Prepare("UPDATE blk SET disabled=1 WHERE hostip=? and hostport=?")
			checkErr(err)
			defer blk.Close()
			_, err = blk.Exec(ip, port)
			if err != nil {
				logger.Error("The disk(%s:%d) bad statu:%d update blk table disabled error:%s", ip, port, statu, err)
			}
		}*/
	checkandupdatediskstatu(ip, int(port), int(statu))
	return &ack, nil
}

// CreateVol
func (s *VolMgrServer) CreateVol(ctx context.Context, in *vp.CreateVolReq) (*vp.CreateVolAck, error) {
	ack := vp.CreateVolAck{}
	volname := in.VolName
	volsize := in.SpaceQuota
	metadomain := in.MetaDomain
	voluuid, err := utils.GenUUID()

	//the volume need block group total numbers
	var blkgrpnum int32
	if volsize < Blksize {
		blkgrpnum = 1
	} else if volsize%Blksize == 0 {
		blkgrpnum = volsize / Blksize
	} else {
		blkgrpnum = volsize/Blksize + 1
	}

	// insert the volume info to volumes tables
	vol, err := VolMgrDB.Prepare("INSERT INTO volumes(uuid, name, size,metadomain) VALUES(?, ?, ?, ?)")
	if err != nil {
		logger.Error("Create volume(%s -- %s) insert db error:%s", volname, voluuid, err)
		ack.Ret = 1 // db error
		return &ack, nil
	}
	defer vol.Close()
	vol.Exec(voluuid, volname, volsize, metadomain)

	//allocate block group for the volume
	for i := int32(0); i < blkgrpnum; i++ {
		rows, err := VolMgrDB.Query("SELECT blkid FROM blk WHERE allocated = 0 and disabled=0 group by hostip ORDER BY rand() LIMIT 3 FOR UPDATE")
		if err != nil {
			logger.Error("Create volume(%s -- %s) select blk for the %dth blkgroup error:%s", volname, voluuid, i, err)
			ack.Ret = 1
			return &ack, nil
		}
		defer rows.Close()

		var blkid int
		var blks string = ""
		var count int = 0
		for rows.Next() {
			err := rows.Scan(&blkid)
			if err != nil {
				ack.Ret = 1
				return &ack, nil
			}

			//tx, err := VolMgrDB.Begin()
			//defer tx.Rollback()
			blk, err := VolMgrDB.Prepare("UPDATE blk SET allocated=1 WHERE blkid=?")
			if err != nil {
				logger.Error("update blk:%d have allocated error:%s", blkid)
				ack.Ret = 1
				return &ack, nil
			}
			defer blk.Close()
			_, err = blk.Exec(blkid)
			if err != nil {
				ack.Ret = 1
				return &ack, nil
			}
			//err = tx.Commit()
			blks = blks + strconv.Itoa(blkid) + ","
			count += 1
		}
		logger.Debug("The volume(%s -- %s) one blkgroup have blks:%s", volname, voluuid, blks)
		if count < 1 || count > 3 {
			logger.Debug("The volume(%s -- %s) one blkgroup no enough or over 3th blks:%s, so create volume failed!", volname, voluuid, count)
			ack.Ret = 1
			return &ack, nil
		}

		blkgrp, err := VolMgrDB.Prepare("INSERT INTO blkgrp(blks, volume_uuid) VALUES(?, ?)")
		if err != nil {
			ack.Ret = 1
			return &ack, nil
		}
		defer blkgrp.Close()
		blkgrp.Exec(blks, voluuid)
	}

	ack.Ret = 0 //success
	ack.UUID = voluuid
	return &ack, nil
}

func (s *VolMgrServer) DeleteVol(ctx context.Context, in *vp.DeleteVolReq) (*vp.DeleteVolAck,error) {
	ack := vp.DeleteVolAck{}
	volid := in.UUID

	//delete volumes table
	vol, err := VolMgrDB.Prepare("delete from volumes where uuid=?")
	if err != nil {
                logger.Error("delete volume:%v from volumes tables err:%v",volid,err)
		ack.Ret = -1
                return &ack, err
        }
        defer vol.Close()
        _, err = vol.Exec(volid)
	if err != nil {
		ack.Ret = -1
		return &ack, err
	}

	//update blk table for delete
	blkgrp, err := VolMgrDB.Query("SELECT blks FROM blkgrp WHERE volume_uuid=?",volid)
        if err != nil {
                logger.Error("select blks for delete volume:%v error:%s", volid, err)
                ack.Ret = -1
                return &ack, err
        }
        defer blkgrp.Close()

        var blks string
        for blkgrp.Next() {
                err := blkgrp.Scan(&blks)
                if err != nil {
                        ack.Ret = -1
                        return &ack, err
                }

		s := strings.Split(blks,",")
		for _,v := range s[:len(s)-1] {
			blkid,_ := strconv.Atoi(v)
			blk, err := VolMgrDB.Prepare("UPDATE blk SET allocated=0, repair=0 WHERE blkid=?")
			if err != nil {
                                logger.Error("update blk:%d allocated=0 error:%s for delete volume:%v", blkid,volid)
                                ack.Ret = -1
                                return &ack, err
                        }
                        defer blk.Close()
                        _, err = blk.Exec(blkid)
                        if err != nil {
                                ack.Ret = -1
                                return &ack, err
                        }
		}
	}

	//delete blkgrp table
	bgrp, err := VolMgrDB.Prepare("delete from blkgrp where volume_uuid=?")
        if err != nil {
                logger.Error("delete volume:%v from blkgrp tables err:%v",volid,err)
                ack.Ret = -1
                return &ack, err
        }
        defer bgrp.Close()
        _, err = bgrp.Exec(volid)
        if err != nil {
                ack.Ret = -1
                return &ack, err
        }
	logger.Debug("== Delete db tables data success for volume:%v",volid)
	ack.Ret = 0
	return &ack,nil
}

func (s *VolMgrServer) SetBlockGroupStatus(ctx context.Context, in *vp.SetBlockGroupStatusReq) (*vp.SetBlockGroupStatusAck, error) {
	ack := vp.SetBlockGroupStatusAck{}
	blkgrpid := in.BlockGroupID
	statu := in.Status
	blkgrp, err := VolMgrDB.Prepare("UPDATE blkgrp SET statu=? WHERE blkgrpid=?")
	if err != nil {
		logger.Error("update blkgrpid:%v statu:%v prepare error:%v", blkgrpid, statu, err)
		return &ack, err
	}
	defer blkgrp.Close()
	_, err = blkgrp.Exec(statu, blkgrpid)
	if err != nil {
		logger.Error("update blkgrpid:%v statu:%v exec error:%v", blkgrpid, statu, err)
		return &ack, err
	}
	return &ack, nil
}

// GetVolInfo
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
		return &ack, nil
	}
	defer vols.Close()
	for vols.Next() {
		err = vols.Scan(&name, &size, &metadomain)
		if err != nil {
			ack.Ret = 1
			return &ack, nil
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
		return &ack, nil
	}
	defer blkgrp.Close()
	pBlockGroups := []*vp.BlockGroup{}
	for blkgrp.Next() {
		err := blkgrp.Scan(&blkgrpid, &blks)
		if err != nil {
			ack.Ret = 1
			return &ack, nil
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
				return &ack, nil
			}
			defer blk.Close()
			for blk.Next() {
				err = blk.Scan(&hostip, &hostport)
				if err != nil {
					ack.Ret = 1
					return &ack, nil
				}
				tmpBlockInfo := vp.BlockInfo{}
				tmpBlockInfo.BlockID = int32(blkid)
				ipnr := net.ParseIP(hostip)
				ipint := utils.Inet_aton(ipnr)
				tmpBlockInfo.DataNodeIP = ipint
				tmpBlockInfo.DataNodePort = int32(hostport)
				pBlockInfos = append(pBlockInfos, &tmpBlockInfo)
			}
		}
		tmpBlockGroup := vp.BlockGroup{}
		tmpBlockGroup.BlockGroupID = int32(blkgrpid)
		tmpBlockGroup.BlockInfos = pBlockInfos
		pBlockGroups = append(pBlockGroups, &tmpBlockGroup)
	}
	volInfo.BlockGroups = pBlockGroups
	logger.Debug("Get info:%v for the volume(%s)", volInfo, voluuid)
	ack = vp.GetVolInfoAck{Ret: 0, VolInfo: &volInfo}
	return &ack, nil
}

// GetVolList
func (s *VolMgrServer) GetVolList(ctx context.Context, in *vp.GetVolListReq) (*vp.GetVolListAck, error) {
	ack := vp.GetVolListAck{}

	vols, err := VolMgrDB.Query("SELECT uuid FROM volumes")
	if err != nil {
		logger.Error("Get volumes from db error:%v", err)
		ack.Ret = 1
		return &ack, nil
	}
	defer vols.Close()

	var name string
	for vols.Next() {
		err = vols.Scan(&name)
		if err != nil {
			ack.Ret = 1
			return &ack, nil
		}
		ack.VolIDs = append(ack.VolIDs, name)
	}
	ack.Ret = 0
	return &ack, nil
}

func checkandupdatediskstatu(ip string, port int, statu int) {
	var dbstatu int = 0
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

	conn, err := DialMeta()
	if err != nil {
		logger.Error("Dial to metanode fail :%v for update blkds\n", err)
		return
	}
	defer conn.Close()
	mc := mp.NewMetaNodeClient(conn)

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
		blk, err := VolMgrDB.Prepare("UPDATE blk SET disabled=1, repair=1 WHERE hostip=? and hostport=?")
		checkErr(err)
		defer blk.Close()
		_, err = blk.Exec(ip, port)
		if err != nil {
			logger.Error("The disk(%s:%d) bad statu:%d update blk table disabled error:%s", ip, port, statu, err)
		}

		UpdateMeta(mc, ip, port, statu)

	} else if statu == 0 {
		logger.Debug("The disk(%s:%d) recovy,so update from 1 to 0, make it all blks is able", ip, port, statu)
		blk, err := VolMgrDB.Prepare("UPDATE blk SET disabled=0, repair=(CASE WHEN allocated=0 THEN 0 ELSE 1 END) WHERE hostip=? and hostport=?")
		checkErr(err)
		defer blk.Close()
		_, err = blk.Exec(ip, port)
		if err != nil {
			logger.Error("The disk(%s:%d) recovy , but update blk table able error:%s", ip, port, statu, err)
		}

		//UpdateMeta(mc, ip, port, statu) // if repair complete, do this
	}
	return
}

// UpdateMeta
func UpdateMeta(mc mp.MetaNodeClient, ip string, port int, statu int) {
	var blkid int
	var badblks []int
	blks, err := VolMgrDB.Query("SELECT blkid FROM blk WHERE hostip=? and hostport=? and allocated=1", ip, port)
	if err != nil {
		logger.Error("Get from blk table for all bad node blks error:%s", err)
	}
	defer blks.Close()
	for blks.Next() {
		err = blks.Scan(&blkid)
		if err != nil {
			logger.Error("Scan db for get bad blk error:%v", err)
			continue
		}
		badblks = append(badblks, blkid)
	}

	pmUpdateBlkGrpReq := &mp.UpdateBlkGrpReq{}
	var volid string
	var blkgrpid int
	var m map[string][]*mp.UpdateBlkGrpInfo
	m = make(map[string][]*mp.UpdateBlkGrpInfo)

	for _, id := range badblks {
		//str := "%" + strconv.Itoa(id) + ",%"
		blkgrp, err := VolMgrDB.Query("SELECT blkgrpid,volume_uuid FROM blkgrp WHERE FIND_IN_SET(?,blks)", id)
		if err != nil {
			logger.Error("Get from blk table for all bad node blks error:%s", err)
			continue
		}
		defer blkgrp.Close()
		for blkgrp.Next() {
			err = blkgrp.Scan(&blkgrpid, &volid)
			if err != nil {
				logger.Error("Scan db for get bad blk error:%v", err)
				continue
			}
			pmUpdateBlkGrpInfo := &mp.UpdateBlkGrpInfo{
				BlkGrpID: int32(blkgrpid),
				BlockID:  int32(id),
				Status:   int32(statu),
			}

			m[volid] = append(m[volid], pmUpdateBlkGrpInfo)
		}
	}

	for k, v := range m {
		pmUpdateBlkGrpReq.VolID = k
		pmUpdateBlkGrpReq.UpdateBlkGrpInfo = v
		logger.Debug("=== Need update volid:%v updateblkinfo:%v ===", k, v)
		_, ret := mc.UpdateBlkGrp(context.Background(), pmUpdateBlkGrpReq)
		if ret != nil {
			logger.Error("Update blk for volid:%v to metadata err:%v", k, ret)
			continue
		}
	}

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

// StartVolMgrService
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

// DialMeta
func DialMeta() (*grpc.ClientConn, error) {
	var conn *grpc.ClientConn
	var err error

	conn, err = grpc.Dial(MetaNodeAddr, grpc.WithInsecure())

	if err != nil {
		time.Sleep(300 * time.Millisecond)
		logger.Error("DialMeta 1: addr:%v", MetaNodeAddr)

		conn, err = grpc.Dial(MetaNodeAddr, grpc.WithInsecure())
		if err != nil {
			time.Sleep(300 * time.Millisecond)
			logger.Error("DialMeta 1: addr:%v", MetaNodeAddr)
			conn, err = grpc.Dial(MetaNodeAddr, grpc.WithInsecure())
		}
	}
	return conn, err
}

func init() {
	c, err := config.NewConfig(os.Args[1])
	if err != nil {
		fmt.Println("NewConfig err")
		os.Exit(1)
	}
	port, _ := c.Int("port")
	VolMgrServerAddr.port = port
	VolMgrServerAddr.log = c.String("log")
	VolMgrServerAddr.host = c.String("host")
	MetaNodeAddr = c.String("metanode::host")
	os.MkdirAll(VolMgrServerAddr.log, 0777)

	mysqlConf.dbhost = c.String("mysql::host")
	mysqlConf.dbusername = c.String("mysql::user")
	mysqlConf.dbpassword = c.String("mysql::passwd")
	mysqlConf.dbname = c.String("mysql::db")

	logger.SetConsole(true)
	logger.SetRollingFile(VolMgrServerAddr.log, "volmgr.log", 10, 100, logger.MB) //each 100M rolling
	switch level := c.String("loglevel"); level {
	case "error":
		logger.SetLevel(logger.ERROR)
	case "debug":
		logger.SetLevel(logger.DEBUG)
	case "info":
		logger.SetLevel(logger.INFO)
	default:
		logger.SetLevel(logger.ERROR)
	}

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
		for _ = range ticker.C {
			detectDataNodes()
		}
	}()
	Wg.Wait()
	defer VolMgrDB.Close()
	StartVolMgrService()
}
