package main

import (
	"bufio"
	"database/sql"
	"errors"
	"flag"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"github.com/ipdcode/containerfs/logger"
	mp "github.com/ipdcode/containerfs/proto/mp"
	rp "github.com/ipdcode/containerfs/proto/rp"
	dr "github.com/ipdcode/containerfs/volmgr/driver"
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

func checkErr(op string, err error) {
	if err != nil {
		logger.Error("opreation:%v error:%v", op, err)
	}
}

type Repair struct {
	VolID string
	BlkGrpID string
	BlkID string
	BlkPort string
    ChunkID string
    Position string
    Inode string
}

func getNeedRepairChunks() {
	sql := "select volid,blkgrpid,blkid,blkport,chkid, position,inode from repair where blkip=? and status=2 limit 10"
	args := utils.ConvertValueToArgs(RepairServerAddr.host)
	tRepair := Repair{"volid", "blkgrpid", "blkid", "blkport", "chkid", "position", "inode"}
	result, err := dr.Select(sql, &tRepair, args...)
	if err != nil {
		logger.Error("Get from blk table for need repair blkds in this node error:%s", err)
		return
	}

	for _, v := range result {
		trepair := v.(Repair)
		blkgrpid, _ := strconv.Atoi(trepair.BlkGrpID)
		blkid, _ := strconv.Atoi(trepair.BlkID)
		blkport, _ := strconv.Atoi(trepair.BlkPort)
		chkid, _ := strconv.Atoi(trepair.ChunkID)
		position, _ := strconv.Atoi(trepair.Position)
		inode, _ := strconv.Atoi(trepair.Inode)
	
		Wg.Add(1)
		go repairchk(trepair.VolID, uint32(blkgrpid), uint32(blkid), blkport, uint64(chkid), position, uint64(inode))
	}
}

type DNode struct {
	Path string
	Status string
}

type BlkGrp struct {
	Blks string
}

type Blk struct {
	Ip string
	Port string
	Disabled string
}

func repairchk(volid string, blkgrpid uint32, blkid uint32, blkport int, chkid uint64, position int, inode uint64) {
	logger.Debug("=== Begin repair blkgrp:%v - blk:%v - chk:%v", blkgrpid, blkid, chkid)

	//if disk bad(I/O error)
	sql := "select mount,statu from disks where ip=? and port=?"
	args := utils.ConvertValueToArgs(RepairServerAddr.host, blkport)
	tDnode := DNode{"mount", "statu"}
	result, err := dr.Select(sql, &tDnode, args...)
	if err != nil || len(result) != 1 {
		logger.Error("Get from disk table for this node bad chunk error:%s", err)
		Wg.Add(-1)
		return
	}

	tdnode := result[0].(DNode)
	path := tdnode.Path
	status, _ := strconv.Atoi(tdnode.Status)
	if status == 2 {
		logger.Debug("The blk:%v bad chunk:%v on disk:%v-%v I/O error, so not repair util the disk recover", blkid, chkid, RepairServerAddr.host, blkport)
		Wg.Add(-1)
		return
	}

	sql = "select blks from blkgrp where blkgrpid=?"
	args = utils.ConvertValueToArgs(blkgrpid)
	tBlkGrp := BlkGrp{"blks"}
	result, err = dr.Select(sql, &tBlkGrp, args...)
	if err != nil || len(result) != 1 {
		logger.Error("Get from blkgrp table for  bad chunk error:%s", err)
		Wg.Add(-1)
		return
	}
	
	tblkgrp := result[0].(BlkGrp)

	s := strings.Split(tblkgrp.Blks, ",")
	var bakcnt int
	for _, v := range s[:len(s)-1] {
		srcblkid, _ := strconv.Atoi(v)
		if uint32(srcblkid) != blkid {
			var srcip string
			var srcport int
			var disabled int
			blksql := "select hostip,hostport,disabled from blk where blkid=?"
			arg := utils.ConvertValueToArgs(srcblkid)
			tBlk := Blk{"hostip", "hostport", "disabled"}
			r, err := dr.Select(blksql, &tBlk, arg...)
			if err != nil || len(r) != 1 {
				logger.Error("Get from blk table bakblk:%v for need repair chunk:%v error:%s", srcblkid, chkid, err)
				bakcnt++
				continue
			}
			
			tblk := r[0].(Blk)
			srcport, _ = strconv.Atoi(tblk.Port)
			disabled, _ = strconv.Atoi(tblk.Disabled)
			srcip = tblk.Ip
			if disabled != 0 {
				bakcnt++
				continue
			}
			if bakcnt == 2 {
				logger.Debug("The bad chunk no good bakchunk(bakchunk badcnt:%v) for repair", bakcnt)
				Wg.Add(-1)
				return
			}
			ret := beginRepairchunk(volid, srcip, srcport, uint32(srcblkid), path, blkid, chkid, position, inode)
			if ret != 0 {
				continue
			} else {
				rsql := "delete from repair where blkid=? and chkid=?"
				rarg := utils.ConvertValueToArgs(blkid, chkid)
				if ret, _ := dr.Exec(rsql, rarg...); ret != 0 {
					logger.Error("delete have repaire complete blk:%v-chunk:%v from repair tables err", blkid, chkid)
					continue
				}
				logger.Debug("Repair volume:%v-blkgrp:%v-blk:%v-chunk:%v have all step complete!", volid, blkgrpid, blkid, chkid)
				Wg.Add(-1)
				return
			}
		}
	}
}

func beginRepairchunk(volid string, srcip string, srcport int, srcblkid uint32, path string, blkid uint32, chkid uint64, position int, inode uint64) (ret int) {
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
		BlkId:   srcblkid,
		SrcIp:   srcip,
		SrcPort: int32(srcport),
		ChkId:   chkid,
	}
	stream, err := c.GetSrcData(context.Background(), getSrcDataReq)
	if err != nil {
		logger.Error("Repair chunkfile:%v-%v-%v from srcblk:%v fail err:%v", path, blkid, chkid, srcblkid, err)
		return -1
	}

	dpath := path + "/block-" + strconv.FormatUint(uint64(blkid), 10)
	dfile := dpath + "/chunk-" + strconv.FormatUint(chkid, 10)

	if ok, err := utils.LocalPathExists(dpath); !ok && err == nil {
		os.MkdirAll(dpath, 0777)
	}

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
		ChunkID:  chkid,
		Position: int32(position),
		Status:   int32(0),
		Inode:    inode,
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

	sql := "select mount,statu from disks where ip=? and port=?"
	args := utils.ConvertValueToArgs(in.SrcIp, in.SrcPort)
	tDnode := DNode{"mount", "statu"}
	result, err := dr.Select(sql, &tDnode, args...)
	if err != nil {
		logger.Error("Get srcblk:%v mountpath for repair error:%s", in.BlkId, err)
		return err
	}

	tdnode := result[0].(DNode)
	srcchkpath := tdnode.Path + "/block-" + strconv.FormatInt(int64(in.BlkId), 10) + "/chunk-" + strconv.FormatInt(int64(in.ChkId), 10)
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
}

// GetLeader Get Metadata Leader Node
func GetLeader(volumeID string) (string, error) {

	var leader string
	var flag bool
	for _, ip := range MetaNodePeers {
		conn, err := grpc.Dial(ip, grpc.WithInsecure(), grpc.WithBlock(), grpc.WithTimeout(time.Millisecond*300), grpc.FailOnNonTempDialError(true))
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

	flag.StringVar(&RepairServerAddr.host, "host", "127.0.0.1", "ContainerFS Repair Host")
	flag.IntVar(&RepairServerAddr.port, "port", 8000, "ContainerFS Repair Port")
	flag.StringVar(&RepairServerAddr.log, "log", "/export/Logs/containerfs/logs/", "ContainerFS Repair logpath")
	loglevel := flag.String("loglevel", "error", "ContainerFS Repair log level")
	flag.StringVar(&mysqlConf.dbhost, "sqlhost", "127.0.0.1:3306", "ContainerFS DBHOST")
	flag.StringVar(&mysqlConf.dbusername, "sqluser", "root", "ContainerFS DBUSER")
	flag.StringVar(&mysqlConf.dbpassword, "sqlpasswd", "root", "ContainerFS DBPASSWD")
	flag.StringVar(&mysqlConf.dbname, "sqldb", "containerfs", "ContainerFS DB")
	addr := flag.String("metanode", "127.0.0.1:9903,127.0.0.1:9913,127.0.0.1:9923", "ContainerFS metanode hosts")

	flag.Parse()

	MetaNodePeers = strings.Split(*addr, ",")

	logger.SetConsole(true)
	logger.SetRollingFile(RepairServerAddr.log, "repair.log", 10, 100, logger.MB) //each 100M rolling
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
	checkErr("repair init open db", err)
	err = dr.DB.Ping()
	checkErr("repair init ping db", err)

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
			getNeedRepairChunks()
		}
	}()
	Wg.Wait()
	defer dr.DB.Close()
	StartRepairService()
}

