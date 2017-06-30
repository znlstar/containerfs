package main

import (
	"bufio"
	"database/sql"
	"errors"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"github.com/ipdcode/containerfs/logger"
	mp "github.com/ipdcode/containerfs/proto/mp"
	rp "github.com/ipdcode/containerfs/proto/rp"
	"github.com/lxmgo/config"
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

// RepairServerAddr ...
var RepairServerAddr addr

// MetaNodeAddr ...
var MetaNodeAddr string

// MetaNodePeers ...
var MetaNodePeers []string

// Wg ...
var Wg sync.WaitGroup

type mysqlc struct {
	dbhost     string
	dbusername string
	dbpassword string
	dbname     string
}

var mysqlConf mysqlc

var err string

// RepairServer ...
type RepairServer struct{}

// VolMgrDB ...
var VolMgrDB *sql.DB

func checkErr(err error) {
	if err != nil {
		logger.Error("%s", err)
	}
}

func getNeedRepairChunks() {
	var volid string
	var blkgrpid int
	var blkid int
	var blkport int
	var chkid int
	var position int
	rpr, err := VolMgrDB.Query("select volid,blkgrpid,blkid,blkport,chkid, position from repair where blkip=? and status=2 limit 10", RepairServerAddr.host)
	if err != nil {
		logger.Error("Get from blk table for need repair blkds in this node error:%s", err)
	}
	defer rpr.Close()
	for rpr.Next() {
		err = rpr.Scan(&volid, &blkgrpid, &blkid, &blkport, &chkid, &position)
		if err != nil {
			logger.Error("Scan db for get need repair chunks error:%v", err)
			continue
		}
		Wg.Add(1)
		go repairchk(volid, blkgrpid, blkid, blkport, chkid, position)
	}
}

func repairchk(volid string, blkgrpid int, blkid int, blkport int, chkid int, position int) {
	logger.Debug("=== Begin repair blkgrp:%v - blk:%v - chk:%v", blkgrpid, blkid, chkid)

	//if disk bad(I/O error)
	var status int
	var path string
	disk, err := VolMgrDB.Query("select mount,statu from disks where ip=? and port=?", RepairServerAddr.host, blkport)
	if err != nil {
		logger.Error("Get from disk table for this node bad chunk error:%s", err)
		Wg.Add(-1)
		return
	}
	defer disk.Close()
	for disk.Next() {
		err = disk.Scan(&path, &status)
		if err != nil {
			logger.Error("Scan db for get bad blk error:%v", err)
			Wg.Add(-1)
			return
		}
	}
	if status == 2 {
		logger.Debug("The blk:%v bad chunk:%v on disk:%v-%v I/O error, so not repair util the disk recover", blkid, chkid, RepairServerAddr.host, blkport)
		Wg.Add(-1)
		return
	}

	var blks string
	var bakcnt int
	blkgrp, err := VolMgrDB.Query("SELECT blks FROM blkgrp WHERE blkgrpid=?", blkgrpid)
	if err != nil {
		logger.Error("Get from blkgrp table for  bad chunk error:%s", err)
		Wg.Add(-1)
		return
	}
	defer blkgrp.Close()
	for blkgrp.Next() {
		err = blkgrp.Scan(&blks)
		if err != nil {
			logger.Error("Scan db for get bak blk error:%v", err)
			Wg.Add(-1)
			return
		}
		s := strings.Split(blks, ",")
		for _, v := range s[:len(s)-1] {
			srcblkid, _ := strconv.Atoi(v)
			if srcblkid != blkid {
				var srcip string
				var srcport int
				var disabled int
				blk, err := VolMgrDB.Query("SELECT hostip,hostport,disabled FROM blk WHERE blkid=?", srcblkid)
				if err != nil {
					logger.Error("Get from blk table bakblk:%v for need repair chunk:%v error:%s", srcblkid, chkid, err)
					bakcnt ++
					continue
				}
				defer blk.Close()
				for blk.Next() {
					err = blk.Scan(&srcip, &srcport, &disabled)
					if err != nil {
						logger.Error("Scan db for get need repair blk:%v - bakblk:%v chunk:%v error:%v", blkid, srcblkid, chkid, err)
					}
				}
				if err != nil || disabled != 0 {
					bakcnt ++
					continue
				}
				if bakcnt == 2 {
					logger.Debug("The bad chunk no good bakchunk(bakchunk badcnt:%v) for repair", bakcnt)
					Wg.Add(-1)
					return
				}
				ret := beginRepairchunk(volid, srcip, srcport, srcblkid, path, blkid, chkid, position)
				if ret != 0 {
					continue
				} else {
					rpr, err := VolMgrDB.Prepare("delete from repair where blkid=? and chkid=?")
					if err != nil {
						logger.Error("delete have repaire complete blk:%v-chunk:%v from repair tables prepare err:%v", blkid, chkid, err)
						continue
					}
					defer rpr.Close()
					_, err = rpr.Exec(blkid, chkid)
					if err != nil {
						logger.Error("delete have repaire complete blk:%v-chunk:%v from repair tables exec err:%v", blkid, chkid, err)
						continue
					}
					logger.Debug("Repair volume:%v-blkgrp:%v-blk:%v-chunk:%v have all step complete!", volid, blkgrpid, blkid, chkid)
					Wg.Add(-1)
					return
				}
			}
		}
	}
}

func beginRepairchunk(volid string, srcip string, srcport int, srcblkid int, path string, blkid int, chkid int, position int) (ret int) {
	logger.Debug("Begin repair chunkfile path:%v-%v from srcip:%v-srcport:%v-srcblk:%v", path, chkid, srcip, srcport, srcblkid)
	srcAddr := srcip + ":" + strconv.Itoa(RepairServerAddr.port)
	conn, err := grpc.Dial(srcAddr, grpc.WithInsecure())
	if err != nil {
		logger.Error("Connect Src Repair Server:%v failed : Dial to failed, reason:%v !", srcAddr, err)
		return -1
	}
	defer conn.Close()
	c := rp.NewRepairClient(conn)
	getSrcDataReq := &rp.GetSrcDataReq{
		BlkId:   int32(srcblkid),
		SrcIp:   srcip,
		SrcPort: int32(srcport),
		ChkId:   int32(chkid),
	}
	stream, err := c.GetSrcData(context.Background(), getSrcDataReq)
	if err != nil {
		logger.Error("Repair chunkfile:%v-%v-%v from srcblk:%v fail err:%v", path, blkid, chkid, srcblkid, err)
		return -1
	}

	dfile := path + "/block-" + strconv.FormatInt(int64(blkid), 10) + "/chunk-" + strconv.FormatInt(int64(chkid), 10)
	f, err := os.OpenFile(dfile, os.O_RDWR|os.O_TRUNC|os.O_CREATE, 0666)
	if err != nil {
		logger.Error("Open repair blk chunk:%v error:%v", dfile, err)
		return -1
	}
	defer f.Close()
	w := bufio.NewWriter(f)

	ack, err := stream.Recv()
	if err != nil || len(ack.Databuf) == 0 || len(ack.Databuf) != 64*1024*1024 {
		return -1
	}
	n, err := w.Write(ack.Databuf)
	if err != nil || n != 64*1024*1024 {
		return -1
	}

	//update meta for the repair complete blk
	conn, err = DialMeta(volid)
	if err != nil {
		logger.Error("Dial to metanode fail :%v for update blkds\n", err)
		return -1
	}
	defer conn.Close()
	mc := mp.NewMetaNodeClient(conn)
	pmUpdateChunkInfo := &mp.UpdateChunkInfoReq{
		VolID:    volid,
		ChunkID:  int64(chkid),
		Position: int32(position),
		Status:   int32(0),
	}
	_, err = mc.UpdateChunkInfo(context.Background(), pmUpdateChunkInfo)
	if err != nil {
		logger.Error("Update have repaire complete chunk to metadata err:%v", err)
		return -1
	}
	logger.Debug("Repair chunkfile:%v-%v-%v from srcblk:%v and Update it to metadata finished!!!", path, blkid, chkid, srcblkid)
	return 0
}

// GetSrcData Repair bad chunk get data from good backup chunk
func (s *RepairServer) GetSrcData(in *rp.GetSrcDataReq, stream rp.Repair_GetSrcDataServer) error {
	var ack rp.GetSrcDataAck
	srcid := in.BlkId
	srcip := in.SrcIp
	srcport := in.SrcPort
	chkid := in.ChkId

	var srcmp string

	disk, err := VolMgrDB.Query("SELECT mount FROM disks WHERE ip=? and port=?", srcip, srcport)
	if err != nil {
		logger.Error("Get srcblk:%v mountpath for repair error:%s", srcid, err)
		return err
	}
	defer disk.Close()
	for disk.Next() {
		err = disk.Scan(&srcmp)
		if err != nil {
			logger.Error("Scan db for get need repair chunk:%v srcblk:%v mountpath error:%v", chkid, srcid, err)
			return err
		}
	}
	srcchkpath := srcmp + "/block-" + strconv.FormatInt(int64(srcid), 10) + "/chunk-" + strconv.FormatInt(int64(chkid), 10)
	fi, err := os.Stat(srcchkpath)
	if err != nil {
		logger.Error("Read SrcChkPath:%v error:%v", srcchkpath, err)
		return err
	}
	if fi.Size() != 64*1024*1024 {
		return err
	}

	f, err := os.Open(srcchkpath)
	if err != nil {
		return err
	}
	defer f.Close()
	buf := make([]byte, 64*1024*1024)
	r := bufio.NewReader(f)
	for {
		n, err := r.Read(buf)
		if err != nil {
			return err
		}
		ack.Databuf = buf[:n]
		if err := stream.Send(&ack); err != nil {
			return err
		}
	}
	return nil
}

// GetLeader Get Metadata Leader Node
func GetLeader(volumeID string) (string, error) {

	var leader string
	var flag bool
	for _, ip := range MetaNodePeers {
		conn, err := grpc.Dial(ip, grpc.WithInsecure(), grpc.FailOnNonTempDialError(true))
		if err != nil {
			continue
		}
		defer conn.Close()
		mc := mp.NewMetaNodeClient(conn)
		pmGetMetaLeaderReq := &mp.GetMetaLeaderReq{
			VolID: volumeID,
		}
		ctx, _ := context.WithTimeout(context.Background(), 5*time.Second)
		pmGetMetaLeaderAck, err1 := mc.GetMetaLeader(ctx, pmGetMetaLeaderReq)
		if err1 != nil {
			continue
		}
		if pmGetMetaLeaderAck.Ret != 0 {
			continue
		}
		leader = pmGetMetaLeaderAck.Leader
		flag = true
		break
	}
	if !flag {
		return "", errors.New("Get leader failed")
	}
	return leader, nil

}

// DialMeta ...
func DialMeta(volumeID string) (*grpc.ClientConn, error) {
	var conn *grpc.ClientConn
	var err error

	MetaNodeAddr, _ = GetLeader(volumeID)
	conn, err = grpc.Dial(MetaNodeAddr, grpc.WithInsecure(), grpc.FailOnNonTempDialError(true))
	if err != nil {
		time.Sleep(500 * time.Millisecond)
		MetaNodeAddr, _ = GetLeader(volumeID)
		conn, err = grpc.Dial(MetaNodeAddr, grpc.WithInsecure(), grpc.FailOnNonTempDialError(true))
		if err != nil {
			time.Sleep(500 * time.Millisecond)
			MetaNodeAddr, _ = GetLeader(volumeID)
			conn, err = grpc.Dial(MetaNodeAddr, grpc.WithInsecure(), grpc.FailOnNonTempDialError(true))
		}
	}
	return conn, err
}

// StartRepairService ...
func StartRepairService() {

	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", RepairServerAddr.port))
	if err != nil {
		panic(fmt.Sprintf("Failed to listen on:%v", RepairServerAddr.port))
	}
	s := grpc.NewServer()
	rp.RegisterRepairServer(s, &RepairServer{})
	// Register reflection service on gRPC server.
	reflection.Register(s)
	if err := s.Serve(lis); err != nil {
		panic("Failed to serve")
	}
}

func init() {
	c, err := config.NewConfig(os.Args[1])
	if err != nil {
		fmt.Println("NewConfig err")
		os.Exit(1)
	}
	port, _ := c.Int("port")
	RepairServerAddr.port = port
	RepairServerAddr.log = c.String("log")
	RepairServerAddr.host = c.String("host")
	//EtcdAddrs = c.Strings("etcd::hosts")
	os.MkdirAll(RepairServerAddr.log, 0777)

	mysqlConf.dbhost = c.String("mysql::host")
	mysqlConf.dbusername = c.String("mysql::user")
	mysqlConf.dbpassword = c.String("mysql::passwd")
	mysqlConf.dbname = c.String("mysql::db")

	MetaNodePeers = c.Strings("metanode::host")

	logger.SetConsole(true)
	logger.SetRollingFile(RepairServerAddr.log, "repair.log", 10, 100, logger.MB) //each 100M rolling
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
			getNeedRepairChunks()
		}
	}()
	Wg.Wait()
	defer VolMgrDB.Close()
	StartRepairService()
}
