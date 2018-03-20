package cfs

import (
	"bytes"
	"errors"
	"fmt"
	"github.com/tiglabs/containerfs/logger"
	"github.com/tiglabs/containerfs/proto/dp"
	"github.com/tiglabs/containerfs/proto/mp"
	"github.com/tiglabs/containerfs/proto/vp"
	"github.com/tiglabs/containerfs/utils"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
	"io"
	"os"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

// chunksize for write
const (
	chunkSize      = 64 * 1024 * 1024
	//oneExpandSize  = 30 * 1024 * 1024 * 1024
	BlockGroupSize = 16 * 1024 * 1024 * 1024
)

const (
	FileNormal = 0
	FileError  = 2
)

// BufferSize ...
var BufferSize int32
var WriteBufferSize int

var VolMgrHosts []string
var MetaNodeHosts []string

// CFS ...
type CFS struct {
	VolID string

	VolMgrConn   *grpc.ClientConn
	VolMgrLeader string

	MetaNodeConn   *grpc.ClientConn
	MetaNodeLeader string
}

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
func CreateVol(name string, capacity string, tier string) int32 {

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
			FreeSize:     BlockGroupSize,
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

// OpenFileSystem ...
func OpenFileSystem(uuid string) *CFS {
	cfs := CFS{VolID: uuid}

	cfs.GetVolumeMetaPeers(uuid)

	err := cfs.GetLeaderInfo(uuid)
	if err != nil {
		logger.Error("OpenFileSystem GetLeaderConn Failed err:%v", err)
		return nil
	}
	cfs.CheckLeaderConns()
	return &cfs
}

func (cfs *CFS) GetLeaderHost() (volMgrLeader string, metaNodeLeader string, err error) {

	volMgrLeader, err = utils.GetVolMgrLeader(VolMgrHosts)
	if err != nil {
		logger.Error("GetLeaderHost failed: %v", err)
		return "", "", err
	}
	metaNodeLeader, err = utils.GetMetaNodeLeader(MetaNodeHosts, cfs.VolID)
	if err != nil {
		logger.Error("GretLeaderHost failed: %v", err)
		return "", "", err
	}
	return volMgrLeader, metaNodeLeader, nil
}

func (cfs *CFS) GetLeaderInfo(uuid string) error {

	var err error
	cfs.VolMgrLeader, cfs.VolMgrConn, err = utils.DialVolMgr(VolMgrHosts)
	if err != nil {
		return err
	}

	vc := vp.NewVolMgrClient(cfs.VolMgrConn)
	pGetMetaNodeRGReq := &vp.GetMetaNodeRGReq{
		UUID: uuid,
	}
	ctx, _ := context.WithTimeout(context.Background(), 10*time.Second)
	pGetMetaNodeRGAck, err := vc.GetMetaNodeRG(ctx, pGetMetaNodeRGReq)
	if err != nil {
		return err
	}

	if pGetMetaNodeRGAck.Ret != 0 {
		logger.Error("GetLeaderConn GetMetaNodeRG failed :%v", pGetMetaNodeRGAck.Ret)
		return fmt.Errorf("GetMetaNodeRG Failed Ret:%v", pGetMetaNodeRGAck.Ret)
	}

	cfs.MetaNodeLeader = pGetMetaNodeRGAck.Leader
	cfs.MetaNodeConn, err = utils.Dial(cfs.MetaNodeLeader)
	if err != nil {
		return err
	}
	return nil
}

func (cfs *CFS) GetVolumeMetaPeers(uuid string) error {

	_, conn, err := utils.DialVolMgr(VolMgrHosts)
	if err != nil {
		logger.Error("DialVolMgr failed: %v", err)
		return err
	}

	vc := vp.NewVolMgrClient(conn)
	pGetMetaNodeRGReq := &vp.GetMetaNodeRGReq{
		UUID: uuid,
	}
	ctx, _ := context.WithTimeout(context.Background(), 10*time.Second)
	pGetMetaNodeRGAck, err := vc.GetMetaNodeRG(ctx, pGetMetaNodeRGReq)
	if err != nil {
		return err
	}

	if pGetMetaNodeRGAck.Ret != 0 {
		logger.Error("GetLeaderConn GetMetaNodeRG failed :%v", pGetMetaNodeRGAck.Ret)
		return fmt.Errorf("GetMetaNodeRG Failed Ret:%v", pGetMetaNodeRGAck.Ret)
	}

	for _, v := range pGetMetaNodeRGAck.MetaNodes {
		MetaNodeHosts = append(MetaNodeHosts, v.Host+":9901")
	}
	return nil
}

func (cfs *CFS) CheckLeaderConns() {

	ticker := time.NewTicker(time.Millisecond * 500)
	go func() {
		for range ticker.C {
			vLeader, mLeader, err := cfs.GetLeaderHost()
			if err != nil {
				logger.Error("CheckLeaderConns GetLeaderHost err %v", err)
				continue
			}
			if vLeader != cfs.VolMgrLeader {
				logger.Error("VolMgr Leader Change! Old Leader %v,New Leader %v", cfs.VolMgrLeader, vLeader)

				if cfs.VolMgrConn != nil {
					cfs.VolMgrConn.Close()
					cfs.VolMgrConn = nil
				}

				cfs.VolMgrConn, err = utils.Dial(vLeader)
				cfs.VolMgrLeader = vLeader
			}
			if mLeader != cfs.MetaNodeLeader || cfs.MetaNodeConn == nil {
				logger.Error("MetaNode Leader Change! Old Leader %v,New Leader %v", cfs.MetaNodeLeader, mLeader)

				if cfs.MetaNodeConn != nil {
					cfs.MetaNodeConn.Close()
					cfs.MetaNodeConn = nil
				}

				cfs.MetaNodeConn, err = utils.Dial(mLeader)
				cfs.MetaNodeLeader = mLeader
			}
			if cfs.MetaNodeConn != nil && cfs.MetaNodeLeader != "" && cfs.MetaNodeConn.GetState() == connectivity.TransientFailure {
				logger.Debug("Need to close bad grpc connection of state TransientFailure")
				cfs.MetaNodeConn.Close()
				cfs.MetaNodeConn = nil
				cfs.MetaNodeConn, err = utils.Dial(cfs.MetaNodeLeader)
				if err != nil {
					continue
				}
			}
		}
	}()

}

// GetFSInfo ...
func (cfs *CFS) GetFSInfo() (int32, *mp.GetFSInfoAck) {

	mc := mp.NewMetaNodeClient(cfs.MetaNodeConn)
	pGetFSInfoReq := &mp.GetFSInfoReq{
		VolID: cfs.VolID,
	}
	ctx, _ := context.WithTimeout(context.Background(), 5*time.Second)
	pGetFSInfoAck, err := mc.GetFSInfo(ctx, pGetFSInfoReq)
	if err != nil {
		logger.Error("GetFSInfo failed,grpc func err :%v", err)
		return 1, nil
	}
	if pGetFSInfoAck.Ret != 0 {
		logger.Error("GetFSInfo failed,grpc func ret :%v", pGetFSInfoAck.Ret)
		return 1, nil
	}
	return 0, pGetFSInfoAck
}

func (cfs *CFS) checkMetaConn() int32 {
	for i := 0; cfs.MetaNodeConn == nil && i < 10; i++ {
		time.Sleep(300 * time.Millisecond)
	}

	if cfs.MetaNodeConn == nil {
		return -1
	}
	return 0
}

// CreateDirDirect ...
func (cfs *CFS) CreateDirDirect(pinode uint64, name string) (int32, uint64) {

	ret := cfs.checkMetaConn()
	if ret != 0 {
		return -1, 0
	}

	mc := mp.NewMetaNodeClient(cfs.MetaNodeConn)
	pCreateDirDirectReq := &mp.CreateDirDirectReq{
		PInode: pinode,
		Name:   name,
		VolID:  cfs.VolID,
	}
	ctx, _ := context.WithTimeout(context.Background(), 5*time.Second)
	pCreateDirDirectAck, err := mc.CreateDirDirect(ctx, pCreateDirDirectReq)
	if err != nil {

		time.Sleep(time.Second)

		ret := cfs.checkMetaConn()
		if ret != 0 {
			return -1, 0
		}

		mc = mp.NewMetaNodeClient(cfs.MetaNodeConn)
		ctx, _ = context.WithTimeout(context.Background(), 5*time.Second)
		pCreateDirDirectAck, err = mc.CreateDirDirect(ctx, pCreateDirDirectReq)
		if err != nil {
			return -1, 0
		}

	}
	return pCreateDirDirectAck.Ret, pCreateDirDirectAck.Inode
}

// GetInodeInfoDirect ...
func (cfs *CFS) GetInodeInfoDirect(pinode uint64, name string) (int32, uint64, *mp.InodeInfo) {

	ret := cfs.checkMetaConn()
	if ret != 0 {
		return -1, 0, nil
	}

	mc := mp.NewMetaNodeClient(cfs.MetaNodeConn)
	pGetInodeInfoDirectReq := &mp.GetInodeInfoDirectReq{
		PInode: pinode,
		Name:   name,
		VolID:  cfs.VolID,
	}
	ctx, _ := context.WithTimeout(context.Background(), 5*time.Second)
	pGetInodeInfoDirectAck, err := mc.GetInodeInfoDirect(ctx, pGetInodeInfoDirectReq)
	if err != nil {

		time.Sleep(time.Second)

		ret := cfs.checkMetaConn()
		if ret != 0 {
			return -1, 0, nil
		}

		mc = mp.NewMetaNodeClient(cfs.MetaNodeConn)
		ctx, _ := context.WithTimeout(context.Background(), 5*time.Second)
		pGetInodeInfoDirectAck, err = mc.GetInodeInfoDirect(ctx, pGetInodeInfoDirectReq)
		if err != nil {
			return -1, 0, nil
		}

	}
	return pGetInodeInfoDirectAck.Ret, pGetInodeInfoDirectAck.Inode, pGetInodeInfoDirectAck.InodeInfo
}

// GetSymLinkInfoDirect ...
func (cfs *CFS) GetSymLinkInfoDirect(pinode uint64, name string) (int32, uint64) {

	ret := cfs.checkMetaConn()
	if ret != 0 {
		return -1, 0
	}

	mc := mp.NewMetaNodeClient(cfs.MetaNodeConn)
	pGetSymLinkInfoDirectReq := &mp.GetSymLinkInfoDirectReq{
		PInode: pinode,
		Name:   name,
		VolID:  cfs.VolID,
	}
	ctx, _ := context.WithTimeout(context.Background(), 5*time.Second)
	pGetSymLinkInfoDirectAck, err := mc.GetSymLinkInfoDirect(ctx, pGetSymLinkInfoDirectReq)
	if err != nil {

		time.Sleep(time.Second)

		ret := cfs.checkMetaConn()
		if ret != 0 {
			return -2, 0
		}

		mc = mp.NewMetaNodeClient(cfs.MetaNodeConn)
		ctx, _ := context.WithTimeout(context.Background(), 5*time.Second)
		pGetSymLinkInfoDirectAck, err = mc.GetSymLinkInfoDirect(ctx, pGetSymLinkInfoDirectReq)
		if err != nil {
			return -3, 0
		}

	}
	return pGetSymLinkInfoDirectAck.Ret, pGetSymLinkInfoDirectAck.Inode
}

// StatDirect ...
func (cfs *CFS) StatDirect(pinode uint64, name string) (int32, uint32, uint64) {

	ret := cfs.checkMetaConn()
	if ret != 0 {
		return -1, 0, 0
	}

	mc := mp.NewMetaNodeClient(cfs.MetaNodeConn)
	pStatDirectReq := &mp.StatDirectReq{
		PInode: pinode,
		Name:   name,
		VolID:  cfs.VolID,
	}
	ctx, _ := context.WithTimeout(context.Background(), 5*time.Second)
	pStatDirectAck, err := mc.StatDirect(ctx, pStatDirectReq)
	if err != nil {

		time.Sleep(time.Second)

		ret := cfs.checkMetaConn()
		if ret != 0 {
			return -1, 0, 0
		}

		mc = mp.NewMetaNodeClient(cfs.MetaNodeConn)
		ctx, _ := context.WithTimeout(context.Background(), 5*time.Second)
		pStatDirectAck, err = mc.StatDirect(ctx, pStatDirectReq)
		if err != nil {
			return -1, 0, 0
		}
	}
	return pStatDirectAck.Ret, pStatDirectAck.InodeType, pStatDirectAck.Inode
}

// ListDirect ...
func (cfs *CFS) ListDirect(pinode uint64, ginode uint64, name string) (int32, []*mp.DirentN) {

	ret := cfs.checkMetaConn()
	if ret != 0 {
		return -1, nil
	}

	mc := mp.NewMetaNodeClient(cfs.MetaNodeConn)
	pListDirectReq := &mp.ListDirectReq{
		PInode: pinode,
		VolID:  cfs.VolID,
		Name:   name,
		GInode: ginode,
	}
	ctx, _ := context.WithTimeout(context.Background(), 60*time.Second)
	pListDirectAck, err := mc.ListDirect(ctx, pListDirectReq)
	if err != nil {
		return -1, nil
	}

	return pListDirectAck.Ret, pListDirectAck.Dirents
}

// DeleteDirDirect ...
func (cfs *CFS) DeleteDirDirect(pinode uint64, name string) int32 {

	ret, _, inode := cfs.StatDirect(pinode, name)
	if ret != 0 {
		logger.Debug("DeleteDirDirect StatDirect Failed , no such dir")
		return 0
	}

	ret = cfs.checkMetaConn()
	if ret != 0 {
		return -1
	}

	mc := mp.NewMetaNodeClient(cfs.MetaNodeConn)

	pListDirectReq := &mp.ListDirectReq{
		PInode: inode,
		VolID:  cfs.VolID,
	}
	ctx, _ := context.WithTimeout(context.Background(), 5*time.Second)
	pListDirectAck, err := mc.ListDirect(ctx, pListDirectReq)
	if err != nil {
		logger.Error("DeleteDirDirect ListDirect :%v\n", err)
		return -1
	}

	for _, v := range pListDirectAck.Dirents {

		if v.InodeType == 1 {
			ret := cfs.DeleteDirDirect(inode, v.Name)
			if ret != 0 {
				return ret
			}
		} else if v.InodeType == 2 {
			ret := cfs.DeleteFileDirect(inode, v.Name)
			if ret != 0 {
				return ret
			}
		} else if v.InodeType == 3 {
			ret := cfs.DeleteSymLinkDirect(inode, v.Name)
			if ret != 0 {
				return ret
			}
		}

	}

	pDeleteDirDirectReq := &mp.DeleteDirDirectReq{
		PInode: pinode,
		Name:   name,
		VolID:  cfs.VolID,
	}
	ctx, _ = context.WithTimeout(context.Background(), 60*time.Second)
	pDeleteDirDirectAck, err := mc.DeleteDirDirect(ctx, pDeleteDirDirectReq)
	if err != nil {
		return -1
	}
	return pDeleteDirDirectAck.Ret
}

// RenameDirect ...
func (cfs *CFS) RenameDirect(oldpinode uint64, oldname string, newpinode uint64, newname string) int32 {

	ret := cfs.checkMetaConn()
	if ret != 0 {
		return -1
	}

	mc := mp.NewMetaNodeClient(cfs.MetaNodeConn)
	pRenameDirectReq := &mp.RenameDirectReq{
		OldPInode: oldpinode,
		OldName:   oldname,
		NewPInode: newpinode,
		NewName:   newname,
		VolID:     cfs.VolID,
	}
	ctx, _ := context.WithTimeout(context.Background(), 5*time.Second)
	pRenameDirectAck, err := mc.RenameDirect(ctx, pRenameDirectReq)
	if err != nil {
		return -1
	}

	return pRenameDirectAck.Ret
}

// CreateFileDirect ...
func (cfs *CFS) CreateFileDirect(pinode uint64, name string, flags int) (int32, *CFile) {
	var writer int32

	if flags&os.O_EXCL != 0 {
		if ret, _, _ := cfs.StatDirect(pinode, name); ret == 0 {
			return 17, nil
		}
	}

	ret, inode := cfs.createFileDirect(pinode, name)
	if ret != 0 {
		return ret, nil
	}

	cfile := CFile{
		OpenFlag:         flags,
		cfs:              cfs,
		Writer:           writer,
		FileSize:         0,
		FileSizeInCache:  0,
		ParentInodeID:    pinode,
		Inode:            inode,
		Name:             name,
		wBuffer:          wBuffer{buffer: new(bytes.Buffer), freeSize: BufferSize},
		isWrite:          int(flags)&os.O_WRONLY != 0 || int(flags)&os.O_RDWR != 0,
		convergeBuffer:   new(bytes.Buffer),
		convergeTimer:    time.NewTimer(time.Second / 10),
		convergeFlushCh:  make(chan struct{}, 1),
		DataCache:        make(map[uint64]*Data),
		DataQueue:        make(chan *chanData, 1),
		CloseSignal:      make(chan struct{}, 10),
		WriteErrSignal:   make(chan bool, 2),
		DataConn:         make(map[string]*grpc.ClientConn),
		errDataNodeCache: make(map[string]bool),
	}
	go cfile.WriteThread()
	go cfile.startFlushConvergeBuffer()

	return 0, &cfile
}

// OpenFileDirect ...
func (cfs *CFS) OpenFileDirect(pinode uint64, name string, flags int) (int32, *CFile) {

	logger.Debug("OpenFileDirect: name: %v, flags: %v\n", name, flags)

	ret, chunkInfos, inode := cfs.GetFileChunksDirect(pinode, name)
	if ret != 0 {
		return ret, nil
	}
	var tmpFileSize int64
	if len(chunkInfos) > 0 {
		for i := range chunkInfos {
			tmpFileSize += int64(chunkInfos[i].ChunkSize)
		}
	}

	cfile := CFile{
		OpenFlag:         flags,
		cfs:              cfs,
		FileSize:         tmpFileSize,
		FileSizeInCache:  tmpFileSize,
		ParentInodeID:    pinode,
		Inode:            inode,
		wBuffer:          wBuffer{buffer: new(bytes.Buffer), freeSize: BufferSize},
		isWrite:          int(flags)&os.O_WRONLY != 0 || int(flags)&os.O_RDWR != 0,
		convergeBuffer:   new(bytes.Buffer),
		convergeTimer:    time.NewTimer(time.Second / 10),
		convergeFlushCh:  make(chan struct{}, 1),
		Name:             name,
		chunks:           chunkInfos,
		DataCache:        make(map[uint64]*Data),
		DataQueue:        make(chan *chanData, 1),
		CloseSignal:      make(chan struct{}, 10),
		WriteErrSignal:   make(chan bool, 2),
		DataConn:         make(map[string]*grpc.ClientConn),
		errDataNodeCache: make(map[string]bool),
	}

	go cfile.WriteThread()
	go cfile.startFlushConvergeBuffer()

	return 0, &cfile
}

// UpdateOpenFileDirect ...
func (cfs *CFS) UpdateOpenFileDirect(pinode uint64, name string, cfile *CFile, flags int) int32 {

	return 0
}

// createFileDirect ...
func (cfs *CFS) createFileDirect(pinode uint64, name string) (int32, uint64) {

	ret := cfs.checkMetaConn()
	if ret != 0 {
		return -1, 0
	}

	mc := mp.NewMetaNodeClient(cfs.MetaNodeConn)
	pCreateFileDirectReq := &mp.CreateFileDirectReq{
		PInode: pinode,
		Name:   name,
		VolID:  cfs.VolID,
	}
	ctx, _ := context.WithTimeout(context.Background(), 5*time.Second)
	pCreateFileDirectAck, err := mc.CreateFileDirect(ctx, pCreateFileDirectReq)
	if err != nil || pCreateFileDirectAck.Ret != 0 {

		time.Sleep(time.Second)

		ret := cfs.checkMetaConn()
		if ret != 0 {
			return -1, 0
		}

		mc = mp.NewMetaNodeClient(cfs.MetaNodeConn)
		ctx, _ := context.WithTimeout(context.Background(), 5*time.Second)
		pCreateFileDirectAck, err = mc.CreateFileDirect(ctx, pCreateFileDirectReq)
		if err != nil {
			logger.Error("CreateFileDirect failed,grpc func failed :%v\n", err)
			return -1, 0
		}
	}
	if pCreateFileDirectAck.Ret == 1 {
		return 1, 0
	}
	if pCreateFileDirectAck.Ret == 2 {
		return 2, 0
	}
	if pCreateFileDirectAck.Ret == 17 {
		return 17, 0
	}
	return 0, pCreateFileDirectAck.Inode
}

func (cfs *CFS) DeleteSymLinkDirect(pinode uint64, name string) int32 {
	ret := cfs.checkMetaConn()
	if ret != 0 {
		return -1
	}

	mc := mp.NewMetaNodeClient(cfs.MetaNodeConn)
	mpDeleteSymLinkDirectReq := &mp.DeleteSymLinkDirectReq{
		PInode: pinode,
		Name:   name,
		VolID:  cfs.VolID,
	}
	ctx, _ := context.WithTimeout(context.Background(), 5*time.Second)
	mpDDeleteSymLinkDirectAck, err := mc.DeleteSymLinkDirect(ctx, mpDeleteSymLinkDirectReq)
	if err != nil || mpDDeleteSymLinkDirectAck.Ret != 0 {
		time.Sleep(time.Second)

		ret := cfs.checkMetaConn()
		if ret != 0 {
			return -1
		}
		mc = mp.NewMetaNodeClient(cfs.MetaNodeConn)
		ctx, _ := context.WithTimeout(context.Background(), 5*time.Second)
		mpDDeleteSymLinkDirectAck, err = mc.DeleteSymLinkDirect(ctx, mpDeleteSymLinkDirectReq)
		if err != nil {
			logger.Error("DeleteFile failed,grpc func err :%v\n", err)
			return -1
		}
	}

	return mpDDeleteSymLinkDirectAck.Ret

}

// DeleteFileDirect ...
func (cfs *CFS) DeleteFileDirect(pinode uint64, name string) int32 {

	ret, chunkInfos, _ := cfs.GetFileChunksDirect(pinode, name)
	if ret == 0 && chunkInfos != nil {
		for _, v1 := range chunkInfos {
			for _, v2 := range v1.BlockGroupWithHost.Hosts {

				conn, err := utils.Dial(v2)
				if err != nil || conn == nil {
					time.Sleep(time.Second)
					conn, err = utils.Dial(v2)
					if err != nil || conn == nil {
						logger.Error("DeleteFile failed,Dial to datanode fail :%v\n", err)
						continue
					}
				}

				dc := dp.NewDataNodeClient(conn)
				dpDeleteChunkReq := &dp.DeleteChunkReq{
					ChunkID:      v1.ChunkID,
					BlockGroupID: v1.BlockGroupWithHost.BlockGroupID,
				}
				ctx, _ := context.WithTimeout(context.Background(), 5*time.Second)
				_, err = dc.DeleteChunk(ctx, dpDeleteChunkReq)
				if err != nil {
					logger.Error("DeleteFile failed,rpc to datanode fail :%v\n", err)
				}
				conn.Close()
			}
		}
	}

	ret = cfs.checkMetaConn()
	if ret != 0 {
		return -1
	}

	mc := mp.NewMetaNodeClient(cfs.MetaNodeConn)
	mpDeleteFileDirectReq := &mp.DeleteFileDirectReq{
		PInode: pinode,
		Name:   name,
		VolID:  cfs.VolID,
	}
	ctx, _ := context.WithTimeout(context.Background(), 5*time.Second)
	mpDeleteFileDirectAck, err := mc.DeleteFileDirect(ctx, mpDeleteFileDirectReq)
	if err != nil || mpDeleteFileDirectAck.Ret != 0 {
		time.Sleep(time.Second)

		ret := cfs.checkMetaConn()
		if ret != 0 {
			return -1
		}
		mc = mp.NewMetaNodeClient(cfs.MetaNodeConn)
		ctx, _ := context.WithTimeout(context.Background(), 5*time.Second)
		mpDeleteFileDirectAck, err = mc.DeleteFileDirect(ctx, mpDeleteFileDirectReq)
		if err != nil {
			logger.Error("DeleteFile failed,grpc func err :%v\n", err)
			return -1
		}
	}

	return mpDeleteFileDirectAck.Ret
}

// GetFileChunksDirect ...
func (cfs *CFS) GetFileChunksDirect(pinode uint64, name string) (int32, []*mp.ChunkInfoWithBG, uint64) {

	ret := cfs.checkMetaConn()
	if ret != 0 {
		logger.Error("GetFileChunksDirect cfs.Conn nil ...")
		return -1, nil, 0
	}

	mc := mp.NewMetaNodeClient(cfs.MetaNodeConn)
	pGetFileChunksDirectReq := &mp.GetFileChunksDirectReq{
		PInode: pinode,
		Name:   name,
		VolID:  cfs.VolID,
	}
	ctx, _ := context.WithTimeout(context.Background(), 5*time.Second)
	pGetFileChunksDirectAck, err := mc.GetFileChunksDirect(ctx, pGetFileChunksDirectReq)
	if err != nil || pGetFileChunksDirectAck.Ret != 0 {

		time.Sleep(time.Second)

		ret := cfs.checkMetaConn()
		if ret != 0 {
			logger.Error("GetFileChunksDirect cfs.Conn nil ...")
			return -1, nil, 0
		}

		mc = mp.NewMetaNodeClient(cfs.MetaNodeConn)
		ctx, _ := context.WithTimeout(context.Background(), 5*time.Second)
		pGetFileChunksDirectAck, err = mc.GetFileChunksDirect(ctx, pGetFileChunksDirectReq)
		if err != nil {
			logger.Error("GetFileChunks failed,grpc func failed :%v\n", err)
			return -1, nil, 0
		}
	}
	return pGetFileChunksDirectAck.Ret, pGetFileChunksDirectAck.ChunkInfos, pGetFileChunksDirectAck.Inode
}

func (cfs *CFS) SymLink(pInode uint64, newName string, target string) (int32, uint64) {

	ret := cfs.checkMetaConn()
	if ret != 0 {
		logger.Error("SymLink cfs.Conn nil ...")
		return -1, 0
	}

	mc := mp.NewMetaNodeClient(cfs.MetaNodeConn)
	pSymLinkReq := &mp.SymLinkReq{
		PInode: pInode,
		Name:   newName,
		Target: target,
		VolID:  cfs.VolID,
	}
	ctx, _ := context.WithTimeout(context.Background(), 5*time.Second)
	pSymLinkAck, err := mc.SymLink(ctx, pSymLinkReq)
	if err != nil || pSymLinkAck.Ret != 0 {

		time.Sleep(time.Second)

		ret := cfs.checkMetaConn()
		if ret != 0 {
			logger.Error("SymLink cfs.Conn nil ...")
			return -1, 0
		}

		mc = mp.NewMetaNodeClient(cfs.MetaNodeConn)
		ctx, _ := context.WithTimeout(context.Background(), 5*time.Second)
		pSymLinkAck, err = mc.SymLink(ctx, pSymLinkReq)
		if err != nil {
			logger.Error("SymLink failed,grpc func failed :%v\n", err)
			return -1, 0
		}
	}
	return pSymLinkAck.Ret, pSymLinkAck.Inode

}

func (cfs *CFS) ReadLink(inode uint64) (int32, string) {

	ret := cfs.checkMetaConn()
	if ret != 0 {
		logger.Error("SymLink cfs.Conn nil ...")
		return -1, ""
	}

	mc := mp.NewMetaNodeClient(cfs.MetaNodeConn)
	pReadLinkReq := &mp.ReadLinkReq{
		Inode: inode,
		VolID: cfs.VolID,
	}
	ctx, _ := context.WithTimeout(context.Background(), 5*time.Second)
	pReadLinkAck, err := mc.ReadLink(ctx, pReadLinkReq)
	if err != nil || pReadLinkAck.Ret != 0 {

		time.Sleep(time.Second)

		ret := cfs.checkMetaConn()
		if ret != 0 {
			logger.Error("ReadLink cfs.Conn nil ...")
			return -1, ""
		}

		mc = mp.NewMetaNodeClient(cfs.MetaNodeConn)
		ctx, _ := context.WithTimeout(context.Background(), 5*time.Second)
		pReadLinkAck, err = mc.ReadLink(ctx, pReadLinkReq)
		if err != nil {
			logger.Error("ReadLink failed,grpc func failed :%v\n", err)
			return -1, ""
		}
	}
	return pReadLinkAck.Ret, pReadLinkAck.Target

}

type Data struct {
	DataBuf *bytes.Buffer
	Status  int32
	timer   *time.Timer
	ID      uint64
}

// ReadCacheT ...
type ReadCache struct {
	LastOffset int64
	readBuf    []byte
	Ch         chan *bytes.Buffer
}

type wBuffer struct {
	freeSize    int32               // chunk size
	chunkInfo   *mp.ChunkInfoWithBG // chunk info
	buffer      *bytes.Buffer       // chunk data
	startOffset int64
	endOffset   int64
}

type chanData struct {
	data []byte
}

type Chunk struct {
	CFile                    *CFile
	ChunkFreeSize            int
	ChunkInfo                *mp.ChunkInfoWithBG
	ChunkWriteStream         dp.DataNode_C2MReplClient
	ChunkWriteRecvExitSignal chan struct{}
}

// CFile ...
type CFile struct {
	cfs           *CFS
	ParentInodeID uint64
	Name          string
	Inode         uint64

	OpenFlag        int
	FileSize        int64
	FileSizeInCache int64
	Status          int32 // 0 ok

	DataConnLocker sync.RWMutex
	DataConn       map[string]*grpc.ClientConn

	// for write
	wBuffer          wBuffer
	wgWriteReps      sync.WaitGroup
	atomicNum        uint64
	curNum           uint64
	Writer           int32
	DataCacheLocker  sync.RWMutex
	DataCache        map[uint64]*Data
	DataQueue        chan *chanData
	WriteErrSignal   chan bool
	WriteRetrySignal chan bool

	isWrite         bool
	convergeBuffer  *bytes.Buffer
	convergeTimer   *time.Timer
	convergeLocker  sync.Mutex
	convergeFlushCh chan struct{}
	Closing     bool
	CloseSignal chan struct{}

	CurChunk *Chunk

	WriteLocker sync.Mutex

	// for read
	//lastoffset int64
	RMutex sync.Mutex
	chunks []*mp.ChunkInfoWithBG // chunkinfo
	//readBuf    []byte
	readCache        ReadCache
	errDataNodeCache map[string]bool
}

type extentInfo struct {
	pos    int32 //pos in chunks of cfile
	offset int32 //offset in chunk
	length int32 //length in chunk
}

func (cfile *CFile) newDataConn(addr string) *grpc.ClientConn {

	cfile.DataConnLocker.RLock()
	if v, ok := cfile.DataConn[addr]; ok {
		cfile.DataConnLocker.RUnlock()
		return v
	}
	cfile.DataConnLocker.RUnlock()

	conn, err := utils.Dial(addr)
	if err != nil || conn == nil {
		logger.Error("Dial to %v failed! err: %v", addr, err)
		return nil
	}

	cfile.DataConnLocker.Lock()
	if v, ok := cfile.DataConn[addr]; ok {
		cfile.DataConnLocker.RUnlock()
		conn.Close()
		return v
	}
	cfile.DataConn[addr] = conn
	cfile.DataConnLocker.Unlock()
	return conn
}

//close and delete conn when err
func (cfile *CFile) delErrDataConn(addr string) {
	cfile.DataConnLocker.Lock()
	if v, ok := cfile.DataConn[addr]; ok {
		v.Close()
		delete(cfile.DataConn, addr)
	}
	cfile.DataConnLocker.Unlock()
}

//only delele all conn when closing file
func (cfile *CFile) delAllDataConn() {
	cfile.DataConnLocker.Lock()
	for k, v := range cfile.DataConn {
		v.Close()
		delete(cfile.DataConn, k)
	}
	cfile.DataConnLocker.Unlock()
}
func (cfile *CFile) streamRead(chunkidx int, ch chan *bytes.Buffer, offset int64, size int64) {
	var conn *grpc.ClientConn
	var buffer *bytes.Buffer
	outflag := 0
	inflag := 0
	idxs := utils.GenerateRandomNumber(0, 3, 3)

	for n := 0; n < 3; n++ {
		i := idxs[n]
		addr := cfile.chunks[chunkidx].BlockGroupWithHost.Hosts[i]
		_, ok := cfile.errDataNodeCache[addr]
		if !ok {
			if n != 0 {
				tmp := idxs[0]
				idxs[0] = i
				idxs[n] = tmp
			}
			break
		}
	}

	for n := 0; n < len(cfile.chunks[chunkidx].BlockGroupWithHost.Hosts); n++ {
		i := idxs[n]

		buffer = new(bytes.Buffer)

		addr := cfile.chunks[chunkidx].BlockGroupWithHost.Hosts[i]

		conn = cfile.newDataConn(addr)
		if conn == nil {
			cfile.errDataNodeCache[addr] = true
			outflag++
			continue
		}

		dc := dp.NewDataNodeClient(conn)
		streamreadChunkReq := &dp.StreamReadChunkReq{
			ChunkID:      cfile.chunks[chunkidx].ChunkID,
			BlockGroupID: cfile.chunks[chunkidx].BlockGroupWithHost.BlockGroupID,
			Offset:       offset,
			Readsize:     size,
		}
		ctx, _ := context.WithTimeout(context.Background(), 10*time.Second)
		stream, err := dc.StreamReadChunk(ctx, streamreadChunkReq)
		if err != nil {
			cfile.delErrDataConn(addr)
			conn = cfile.newDataConn(addr)
			if conn == nil {
				logger.Error("StreamReadChunk return error:%v and re-dial failed, so retry other datanode!", err)
				cfile.errDataNodeCache[addr] = true
				outflag++
				continue
			} else {
				dc = dp.NewDataNodeClient(conn)
				streamreadChunkReq := &dp.StreamReadChunkReq{
					ChunkID:      cfile.chunks[chunkidx].ChunkID,
					BlockGroupID: cfile.chunks[chunkidx].BlockGroupWithHost.BlockGroupID,
					Offset:       offset,
					Readsize:     size,
				}
				ctx, _ = context.WithTimeout(context.Background(), 10*time.Second)
				stream, err = dc.StreamReadChunk(ctx, streamreadChunkReq)
				if err != nil {
					cfile.delErrDataConn(addr)
					logger.Error("StreamReadChunk StreamReadChunk error:%v, so retry other datanode!", err)
					cfile.errDataNodeCache[addr] = true
					outflag++
					continue
				}

			}

		}

		delete(cfile.errDataNodeCache, addr)

		for {
			ack, err := stream.Recv()

			if err == io.EOF {
				break
			}
			if err != nil {
				logger.Error("=== streamreadChunkReq Recv err:%v ===", err)
				inflag++
				outflag++
				break
			}
			if ack != nil {
				if len(ack.Databuf) == 0 {
					continue
				} else {
					buffer.Write(ack.Databuf)
					inflag = 0
				}
			} else {
				continue
			}

		}

		if inflag == 0 {
			ch <- buffer
			break
		} else if inflag == 3 {
			buffer = new(bytes.Buffer)
			buffer.Write([]byte{})
			logger.Error("Stream Read the chunk three copy Recv error")
			ch <- buffer
			break
		} else if inflag < 3 {
			logger.Error("Stream Read the chunk %v copy Recv error, so need retry other datanode!!!", inflag)
			continue
		}
	}
	if outflag >= 3 {
		buffer = new(bytes.Buffer)
		buffer.Write([]byte{})
		logger.Error("Stream Read the chunk three copy Datanode error")
		ch <- buffer
	}
}

func (cfile *CFile) readChunk(eInfo extentInfo, data *[]byte, offset int64) int32 {

	//check if hit readBuf
	readBufOffset := cfile.readCache.LastOffset
	readBufLen := len(cfile.readCache.readBuf)
	if offset >= readBufOffset && offset+int64(eInfo.length) <= readBufOffset+int64(readBufLen) {
		pos := int32(offset - readBufOffset)
		*data = append(*data, cfile.readCache.readBuf[pos:pos+eInfo.length]...)

		logger.Debug("cfile %v hit read buffer, offset:%v len:%v, readBuf offset:%v, len:%v", cfile.Name, offset, eInfo.length, readBufOffset, readBufLen)
		return eInfo.length
	}

	//prepare to read from datanode
	cfile.readCache.readBuf = []byte{}
	buffer := new(bytes.Buffer)
	cfile.readCache.Ch = make(chan *bytes.Buffer)
	readSize := eInfo.length
	if readSize < int32(BufferSize) {
		readSize = int32(BufferSize)
	}

	//go streamRead
	go cfile.streamRead(int(eInfo.pos), cfile.readCache.Ch, int64(eInfo.offset), int64(readSize))
	buffer = <-cfile.readCache.Ch
	bLen := buffer.Len()
	if bLen == 0 {
		logger.Error("try to read %v chunk:%v from datanode size:%v, but return:%v", cfile.Name, eInfo.pos, readSize, bLen)
		return -1
	}
	cfile.readCache.readBuf = buffer.Next(bLen)
	cfile.readCache.LastOffset = offset
	appendLen := eInfo.length
	if appendLen > int32(bLen) {
		appendLen = int32(bLen)
	}
	*data = append(*data, cfile.readCache.readBuf[0:appendLen]...)
	buffer.Reset()
	buffer = nil
	return appendLen
}

func (cfile *CFile) disableReadCache(wOffset int64, wLen int32) {
	readBufOffset := cfile.readCache.LastOffset
	readBufLen := len(cfile.readCache.readBuf)
	if readBufLen == 0 {
		return
	}

	if wOffset >= readBufOffset+int64(readBufLen) || wOffset+int64(wLen) <= readBufOffset {
		return
	}

	//we need disable read buffer here
	cfile.readCache.readBuf = []byte{}
	logger.Debug("cfile %v disableReadCache: offset: %v len %v --> %v", cfile.Name, readBufOffset, readBufLen, len(cfile.readCache.readBuf))
}

//get extent info by [start, end)
func (cfile *CFile) getExtentInfo(start int64, end int64, eInfo *[]extentInfo) {
	var i int32
	var chunkStart, chunkEnd int64
	var tmpInfo extentInfo

	for i = 0; i < int32(len(cfile.chunks)) && start < end; i++ {
		chunkEnd += int64(cfile.chunks[i].ChunkSize) //@chunkEnd is next chunk's @chunkStart

		if start < chunkEnd {
			tmpInfo.pos = i
			tmpInfo.offset = int32(start - chunkStart)
			if chunkEnd < end {
				tmpInfo.length = int32(chunkEnd - start)
				start = chunkEnd //update @start to next chunk
			} else {
				tmpInfo.length = int32(end - start)
				start = end
			}
			*eInfo = append(*eInfo, tmpInfo)
		}
		chunkStart = chunkEnd
	}
}

// Read ...
func (cfile *CFile) Read(data *[]byte, offset int64, readsize int64) int64 {

	if cfile.Status != FileNormal {
		logger.Error("cfile %v status error , read func return -2 ", cfile.Name)
		return -2
	}

	if offset == cfile.FileSizeInCache {
		logger.Debug("cfile:%v read offset:%v  equals file size in cache ", cfile.Name, offset)
		return 0
	} else if offset > cfile.FileSizeInCache {
		logger.Debug("cfile %v unsupport read beyond file size, offset:%v, filesize in cache:%v ", cfile.Name, offset, cfile.FileSizeInCache)
		return 0
	}

	var i int
	var ret int32
	var doneFlag bool
	start := offset
	end := offset + readsize

	logger.Debug("cfile %v Read start: offset: %v, len: %v", cfile.Name, offset, readsize)

	for start < end && cfile.Status == FileNormal {

		eInfo := make([]extentInfo, 0, 4)
		cfile.getExtentInfo(start, end, &eInfo)
		logger.Debug("cfile %v getExtentInfo: offset: %v, len: %v, eInfo: %v", cfile.Name, start, end, eInfo)

		for _, ei := range eInfo {
			ret = cfile.readChunk(ei, data, start)
			if ret != ei.length {
				logger.Error("cfile %v eInfo:%v, readChunk ret %v", cfile.Name, ei, ret)
				doneFlag = true
				break
			}
			start += int64(ret)
		}

		if doneFlag || start == end || start >= cfile.FileSizeInCache {
			break
		}

		//wait append write request in caches
		logger.Debug("cfile %v, start to wait append write..FileSize %v, FileSizeInCache %v", cfile.Name, cfile.FileSize, cfile.FileSizeInCache)
		for i = 0; i < 10; i++ {
			if cfile.FileSize >= end || cfile.FileSize == cfile.FileSizeInCache {
				break
			}
			if cfile.isWrite && cfile.convergeBuffer.Len() > 0 {
				cfile.convergeFlushCh <- struct{}{}
			}
			if len(cfile.DataCache) == 0 {
				logger.Debug("cfile %v, FileSize %v, FileSizeInCache %v, but no DataCache", cfile.Name, cfile.FileSize, cfile.FileSizeInCache)
			}
			time.Sleep(40 * time.Millisecond)
		}
		logger.Debug("cfile %v, end waiting with FileSize %v, FileSizeInCache %v, time %v ms", cfile.Name, cfile.FileSize, cfile.FileSizeInCache, i*100)
	}

	if cfile.Status != FileNormal {
		logger.Error("cfile %v status error , read func return -2 ", cfile.Name)
		return -2
	}

	logger.Debug("cfile %v Read end: return %v", cfile.Name, start-offset)
	return start - offset
}

// Write ...
func (cfile *CFile) Write(buf []byte, offset int64, length int32) int32 {

	if cfile.Status != 0 {
		logger.Error("cfile %v status error , Write func return -2 ", cfile.Name)
		return -2
	}

	if offset > cfile.FileSizeInCache {
		logger.Error("cfile %v unsupport write %v beyond file size %v return -3 ", cfile.Name, offset, cfile.FileSizeInCache)
		return -3
	}

	if offset == cfile.FileSizeInCache {
		logger.Debug("cfile %v write append only: offset %v, length %v", cfile.Name, offset, length)
		return cfile.appendWrite(buf, length, false)
	}

	cfile.disableReadCache(offset, length)

	var i int
	var ret, pos int32
	start := offset
	end := offset + int64(length)

	logger.Debug("cfile %v write start: offset: %v, len: %v", cfile.Name, offset, length)

	for start < end && cfile.Status == FileNormal {

		eInfo := make([]extentInfo, 0, 4)
		cfile.getExtentInfo(start, end, &eInfo)
		logger.Debug("cfile %v getExtentInfo: offset: %v, len: %v, eInfo: %v", cfile.Name, start, end, eInfo)

		for _, ei := range eInfo {
			ret = cfile.seekWrite(ei, buf[pos:(pos+ei.length)])
			if ret < 0 {
				logger.Error("cfile %v seekWrite failed %v", cfile.Name, ei)
				return int32(start - offset)
			}
			start += int64(ei.length)
			pos += ei.length
		}

		if start == end {
			break
		}

		if start == cfile.FileSizeInCache {
			logger.Debug("cfile %v write append only: offset %v, length %v", cfile.Name, start, length-pos)
			ret = cfile.appendWrite(buf[pos:length], length-pos, false)
			if ret < 0 {
				logger.Error("cfile %v appendWrite failed %v", cfile.Name, ret)
				return int32(start - offset)
			}
			start = end
			break
		}

		//wait append write request in caches
		logger.Debug("cfile %v, start to wait append write..FileSize %v, FileSizeInCache %v", cfile.Name, cfile.FileSize, cfile.FileSizeInCache)
		for i = 0; i < 10; i++ {
			if cfile.FileSize >= end || cfile.FileSize == cfile.FileSizeInCache {
				break
			}
			if len(cfile.DataCache) == 0 {
				logger.Debug("cfile %v, FileSize %v, FileSizeInCache %v, but no DataCache", cfile.Name, cfile.FileSize, cfile.FileSizeInCache)
			}
			time.Sleep(100 * time.Millisecond)
		}
		logger.Debug("cfile %v, end waiting with FileSize %v, FileSizeInCache %v, time %v ms", cfile.Name, cfile.FileSize, cfile.FileSizeInCache, i*100)
	}

	logger.Debug("cfile %v Write end: return %v", cfile.Name, start-offset)
	return int32(start - offset)
}

func (cfile *CFile) overwriteBuffer(eInfo extentInfo, buf []byte) int32 {

	//read wBuffer all bytes to tmpBuf
	bufLen := cfile.wBuffer.buffer.Len()
	tmpBuf := cfile.wBuffer.buffer.Next(bufLen)
	if len(tmpBuf) != bufLen {
		logger.Error("cfile %v read wBuffer len: %v return: %v ", cfile.Name, bufLen, len(tmpBuf))
		return -1
	}

	//copy buf to tmpBuf
	n := copy(tmpBuf[eInfo.offset:], buf)
	if n != int(eInfo.length) {
		logger.Error("cfile %v copy to wBuffer len: %v return n: %v", cfile.Name, eInfo.length, n)
		return -1
	}

	//write to wBuffer
	cfile.wBuffer.buffer.Reset()
	n, err := cfile.wBuffer.buffer.Write(tmpBuf)
	if n != int(bufLen) || err != nil {
		logger.Error("cfile %v write wBuffer len: %v return n: %v err %v", cfile.Name, bufLen, n, err)
		return -1
	}

	return 0
}

func (cfile *CFile) seekWriteChunk(addr string, conn *grpc.ClientConn, req *dp.SeekWriteChunkReq, copies *uint64) {

	if conn == nil {
	} else {
		dc := dp.NewDataNodeClient(conn)
		ctx, _ := context.WithTimeout(context.Background(), 5*time.Second)
		ret, err := dc.SeekWriteChunk(ctx, req)
		if err != nil {
			cfile.delErrDataConn(addr)
			logger.Error("SeekWriteChunk err %v", err)
		} else {
			if ret.Ret != 0 {
			} else {
				atomic.AddUint64(copies, 1)
			}
		}
	}
	cfile.wgWriteReps.Add(-1)

}

func (cfile *CFile) seekWrite(eInfo extentInfo, buf []byte) int32 {

	chunkInfo := cfile.chunks[eInfo.pos]
	var copies uint64
	conn := make([]*grpc.ClientConn, 3)

	for i, v := range chunkInfo.BlockGroupWithHost.Hosts {

		conn[i] = cfile.newDataConn(v)
		if conn[i] == nil {
			logger.Error("cfile %v dial %v failed!", cfile.Name, v)
			return -1
		}
	}

	for i, v := range chunkInfo.BlockGroupWithHost.Hosts {

		pSeekWriteChunkReq := &dp.SeekWriteChunkReq{
			ChunkID:      chunkInfo.ChunkID,
			BlockGroupID: chunkInfo.BlockGroupWithHost.BlockGroupID,
			Databuf:      buf,
			ChunkOffset:  int64(eInfo.offset),
		}

		cfile.wgWriteReps.Add(1)

		go cfile.seekWriteChunk(v, conn[i], pSeekWriteChunkReq, &copies)

	}

	cfile.wgWriteReps.Wait()

	if copies < 3 {
		cfile.Status = FileError
		logger.Error("cfile %v seekWriteChunk copies: %v, set error!", cfile.Name, copies)
		return -1
	}
	return 0
}

//flush CFile convergeBuffer ...
func (cfile *CFile) startFlushConvergeBuffer() {

	if !cfile.isWrite || WriteBufferSize <= 0{
		return
	}

	for {
		if cfile.Closing == true {
			return
		}

		select {
		case <-cfile.convergeFlushCh:
			cfile.convergeLocker.Lock()
			if cfile.convergeBuffer.Len() > 0 {
				data := &chanData{}
				data.data = append(data.data, cfile.convergeBuffer.Next(cfile.convergeBuffer.Len())...)
				select {
				case <-cfile.WriteErrSignal:
					logger.Error("Write recv WriteErrSignal ,volumeid %v , pid %v ,fname %v!", cfile.cfs.VolID, cfile.ParentInodeID, cfile.Name)
					return
				case cfile.DataQueue <- data:
				}
			}
			cfile.convergeLocker.Unlock()
		}
	}
}

// Write ...
func (cfile *CFile) appendWrite(buf []byte, length int32, needFlush bool) (ret int32) {

	if cfile.Status == FileError {
		return -2
	}
	
	ret = length

	cfile.FileSizeInCache += int64(length)

	if WriteBufferSize > 0{
		cfile.convergeLocker.Lock()
		if length != 0 {
			cfile.convergeBuffer.Write(buf)
		}
	
		bufferLen := cfile.convergeBuffer.Len()
		if bufferLen < WriteBufferSize && !needFlush {
			cfile.convergeLocker.Unlock()
			return length
		}
		if bufferLen == 0 {
			cfile.convergeLocker.Unlock()
			return 0
		}
	}

	data := &chanData{}
	if WriteBufferSize > 0{
		data.data = append(data.data, cfile.convergeBuffer.Next(cfile.convergeBuffer.Len())...)
	}else{
		data.data = append(data.data, buf...)
	}
	select {
	case <-cfile.WriteErrSignal:
		logger.Error("Write recv WriteErrSignal ,volumeid %v , pid %v ,fname %v!", cfile.cfs.VolID, cfile.ParentInodeID, cfile.Name)
		ret = -2
	case cfile.DataQueue <- data:
	}
	
	if WriteBufferSize > 0{
		cfile.convergeLocker.Unlock()
	}
	return ret
}

func (cfile *CFile) WriteThread() {

	logger.Debug("Write Thread: file %v start writethread!\n", cfile.Name)

	for true {
		select {
		case chanData := <-cfile.DataQueue:
			if chanData == nil {
				logger.Debug("WriteThread file %v recv channel close, wait DataCache...", cfile.Name)
				var ti uint32
				for cfile.Status == FileNormal {
					if len(cfile.DataCache) == 0 {
						break
					}
					ti++
					time.Sleep(time.Millisecond * 5)
				}
				logger.Debug("WriteThread file %v wait DataCache == 0 done. loop times: %v", cfile.Name, ti)

				if cfile.CurChunk != nil {
					if cfile.CurChunk.ChunkWriteStream != nil {
						cfile.CurChunk.ChunkWriteStream.CloseSend()
					}
				}
				cfile.CloseSignal <- struct{}{}
				return
			} else {

				if cfile.Status == FileError {
					continue
				}

				newData := &Data{}
				newData.ID = atomic.AddUint64(&cfile.atomicNum, 1)
				newData.DataBuf = new(bytes.Buffer)
				newData.DataBuf.Write(chanData.data)
				newData.Status = 1

				if err := cfile.WriteHandler(newData); err != nil {
					logger.Error("WriteThread file %v WriteHandler err %v !", cfile.Name, err)
					cfile.Status = FileError
					cfile.WriteErrSignal <- true
				}
			}

		}
	}

}

func (cfile *CFile) WriteHandler(newData *Data) error {

	length := newData.DataBuf.Len()

	logger.Debug("WriteHandler: file %v, num:%v,  length: %v, \n", cfile.Name, cfile.atomicNum, length)

ALLOCATECHUNK:

	if cfile.CurChunk != nil && cfile.CurChunk.ChunkFreeSize-length < 0 {

		if cfile.CurChunk.ChunkWriteStream != nil {
			var ti uint32
			needClose := bool(true)
			logger.Debug("WriteHandler: file %v, begin waiting last chunk: %v\n", cfile.Name, len(cfile.DataCache))
			tmpDataCacheLen := len(cfile.DataCache)

			for cfile.Status == FileNormal {
				if tmpDataCacheLen == 0 {
					break
				}
				time.Sleep(time.Millisecond * 10)
				if tmpDataCacheLen == len(cfile.DataCache) {
					ti++
				} else {
					tmpDataCacheLen = len(cfile.DataCache)
					ti = 0
				}

				if ti == 500 {
					if cfile.CurChunk.ChunkWriteStream != nil {
						logger.Error("WriteHandler: file %v, dataCacheLen: %v  wait last chunk timeout, CloseSend\n", cfile.Name, len(cfile.DataCache))
						cfile.CurChunk.ChunkWriteStream.CloseSend()
						needClose = false
					}
				}
			}
			if cfile.Status == FileError {
				return errors.New("file status err")
			}
			logger.Debug("WriteHandler: file %v, end wait after loop times %v\n", cfile.Name, ti)

			if needClose && cfile.CurChunk.ChunkWriteStream != nil {
				cfile.CurChunk.ChunkWriteStream.CloseSend()
			}
		}

		cfile.CurChunk = nil
	}

	if cfile.CurChunk == nil {
		for retryCnt := 0; retryCnt < 5; retryCnt++ {
			cfile.CurChunk = cfile.AllocateChunk(true)
			if cfile.CurChunk == nil {
				logger.Error("WriteHandler: file %v, alloc chunk failed for %v times\n", cfile.Name, retryCnt+1)
				time.Sleep(time.Millisecond * 500)
				continue
			}
			break
		}
		if cfile.CurChunk == nil {
			return errors.New("AllocateChunk failed for 5 times")
		}
	}

	cfile.DataCacheLocker.Lock()
	cfile.DataCache[cfile.atomicNum] = newData
	cfile.DataCacheLocker.Unlock()

	req := &dp.StreamWriteReq{
		ChunkID:      cfile.CurChunk.ChunkInfo.ChunkID,
		Master:       cfile.CurChunk.ChunkInfo.BlockGroupWithHost.Hosts[0],
		Slave:        cfile.CurChunk.ChunkInfo.BlockGroupWithHost.Hosts[1],
		Backup:       cfile.CurChunk.ChunkInfo.BlockGroupWithHost.Hosts[2],
		Databuf:      newData.DataBuf.Bytes(),
		DataLen:      uint32(length),
		CommitID:     cfile.atomicNum,
		BlockGroupID: cfile.CurChunk.ChunkInfo.BlockGroupWithHost.BlockGroupID,
	}

	if cfile.CurChunk != nil {
		if cfile.CurChunk.ChunkWriteStream != nil {
			if err := cfile.CurChunk.ChunkWriteStream.Send(req); err != nil {
				logger.Debug("WriteHandler: send file %v, chunk %v len: %v failed\n", cfile.Name, cfile.CurChunk, length)
				cfile.CurChunk.ChunkFreeSize = 0
			} else {
				logger.Debug("WriteHandler: send file %v, chunk %v len: %v success\n", cfile.Name, cfile.CurChunk, length)
				cfile.CurChunk.ChunkFreeSize -= length
			}
		} else {
			logger.Error("WriteHandler: file %v, CurChunk %v has no write stream\n", cfile.Name, cfile.CurChunk.ChunkInfo.ChunkID)
			goto ALLOCATECHUNK
		}
	} else {
		logger.Error("WriteHandler: file %v, CurChunk is nil\n", cfile.Name)
		goto ALLOCATECHUNK
	}

	return nil
}

// AllocateChunk ...
func (cfile *CFile) AllocateChunk(IsStream bool) *Chunk {

	logger.Debug("AllocateChunk file: %v begin\n", cfile.Name)

	ret := cfile.cfs.checkMetaConn()
	if ret != 0 {
		logger.Error("AllocateChunk file: %v failed\n", cfile.Name)
		return nil
	}

	mc := mp.NewMetaNodeClient(cfile.cfs.MetaNodeConn)
	pAllocateChunkReq := &mp.AllocateChunkReq{
		VolID: cfile.cfs.VolID,
	}
	ctx, _ := context.WithTimeout(context.Background(), 5*time.Second)
	pAllocateChunkAck, err := mc.AllocateChunk(ctx, pAllocateChunkReq)
	if err != nil || pAllocateChunkAck.Ret != 0 {
		time.Sleep(time.Second * 2)

		ret := cfile.cfs.checkMetaConn()
		if ret != 0 {
			logger.Error("AllocateChunk file: %v failed\n", cfile.Name)
			return nil
		}

		mc = mp.NewMetaNodeClient(cfile.cfs.MetaNodeConn)
		ctx, _ := context.WithTimeout(context.Background(), 5*time.Second)
		pAllocateChunkAck, err = mc.AllocateChunk(ctx, pAllocateChunkReq)
		if err != nil || pAllocateChunkAck.Ret != 0 {
			logger.Error("AllocateChunk file: %v failed, err: %v ret: %v\n", cfile.Name, err, pAllocateChunkAck.Ret != 0)
			return nil
		}
	}

	curChunk := &Chunk{}
	curChunk.CFile = cfile
	curChunk.ChunkInfo = pAllocateChunkAck.ChunkInfo

	logger.Debug("AllocateChunk file: %v from metanode chunk info:%v\n", cfile.Name, curChunk.ChunkInfo)

	if IsStream {

		err := utils.TryDial(curChunk.ChunkInfo.BlockGroupWithHost.Hosts[1])
		if err != nil {
			logger.Error("AllocateChunk file: %v new conn to %v failed, err: %v\n", cfile.Name, curChunk.ChunkInfo.BlockGroupWithHost.Hosts[1], err)
			return nil
		}

		err = utils.TryDial(curChunk.ChunkInfo.BlockGroupWithHost.Hosts[2])
		if err != nil {
			logger.Error("AllocateChunk file: %v new conn to %v failed, err: %v\n", cfile.Name, curChunk.ChunkInfo.BlockGroupWithHost.Hosts[2], err)
			return nil
		}

		C2Mconn := cfile.newDataConn(curChunk.ChunkInfo.BlockGroupWithHost.Hosts[0])
		if C2Mconn == nil {
			logger.Error("AllocateChunk file: %v new conn to %v failed\n", cfile.Name, curChunk.ChunkInfo.BlockGroupWithHost.Hosts[0])
			return nil
		}
		C2Mclient := dp.NewDataNodeClient(C2Mconn)
		curChunk.ChunkWriteStream, err = C2Mclient.C2MRepl(context.Background())
		if err != nil {
			cfile.delErrDataConn(curChunk.ChunkInfo.BlockGroupWithHost.Hosts[0])
			logger.Error("AllocateChunk file: %v create stream to %v failed, err: %v\n", cfile.Name, curChunk.ChunkInfo.BlockGroupWithHost.Hosts[0], err)
			return nil
		}

		curChunk.ChunkFreeSize = chunkSize
		curChunk.ChunkWriteRecvExitSignal = make(chan struct{})

		go curChunk.C2MRecv()
	}

	logger.Debug("AllocateChunk file: %v success\n", cfile.Name)

	return curChunk
}

func (chunk *Chunk) Retry() {

	chunk.CFile.DataCacheLocker.Lock()
	defer chunk.CFile.DataCacheLocker.Unlock()

	if len(chunk.CFile.DataCache) == 0 {
		logger.Debug("C2MRecv thread end success for file %v chunk %v", chunk.CFile.Name, chunk.ChunkInfo.ChunkID)
		return
	}

	logger.Debug("C2MRecv thread Retry write file %v chunk %v start", chunk.CFile.Name, chunk.ChunkInfo.ChunkID)

	retrySuccess := false
	var err error
	for retryCnt := 0; retryCnt < 5; retryCnt++ {
		err = chunk.WriteRetryHandle()
		if err != nil {
			logger.Error("WriteRetryHandle file %v chunk %v err: %v, try again for %v times!", chunk.CFile.Name, chunk.ChunkInfo.ChunkID, err, retryCnt+1)
			time.Sleep(time.Millisecond * 500)
			continue
		} else {
			retrySuccess = true
			break
		}
	}

	if !retrySuccess {
		chunk.CFile.Status = FileError
		chunk.CFile.WriteErrSignal <- true
		logger.Error("C2MRecv thread Retry write file %v chunk %v failed, set FileError!", chunk.CFile.Name, chunk.ChunkInfo.ChunkID)
	} else {
		chunk.CFile.DataCache = make(map[uint64]*Data)
		chunk.ChunkFreeSize = 0
		chunk.ChunkWriteStream = nil
		logger.Debug("C2MRecv thread Retry write file %v chunk %v success", chunk.CFile.Name, chunk.ChunkInfo.ChunkID)
	}
}

func (chunk *Chunk) C2MRecv() {
	logger.Debug("C2MRecv thread started success for file %v chunk %v", chunk.CFile.Name, chunk.ChunkInfo.ChunkID)

	defer chunk.Retry()

	for {
		in, err := chunk.ChunkWriteStream.Recv()
		if err == io.EOF {
			logger.Debug("C2MRecv: file %v chunk %v stream %v EOF\n", chunk.CFile.Name, chunk.ChunkInfo.ChunkID, chunk.ChunkWriteStream)
			break
		}
		if err != nil {
			logger.Debug("C2MRecv: file %v chunk %v stream %v error return : %v\n", chunk.CFile.Name, chunk.ChunkInfo.ChunkID, chunk.ChunkWriteStream, err)
			break
		}

		if in.Ret == -1 {
			logger.Error("C2MRecv: file %v chunk %v ack.Ret -1 , means M2S2B stream err", chunk.CFile.Name, chunk.ChunkInfo.ChunkID)
			break
		}

		chunk.CFile.curNum = atomic.AddUint64(&chunk.CFile.curNum, 1)
		if in.CommitID != chunk.CFile.curNum {
			logger.Error("C2MRecv: write failed! file: %v, ID；%v != curNum: %v, chunk: %v, len: %v\n", chunk.CFile.Name, in.CommitID, chunk.CFile.curNum, in.ChunkID, in.DataLen)
			break
		}

		// update to metanode
		logger.Debug("C2MRecv: Write success! try to update metadata file: %v, ID；%v, chunk: %v, len: %v\n",
			chunk.CFile.Name, in.CommitID, in.ChunkID, in.DataLen)

		mc := mp.NewMetaNodeClient(chunk.CFile.cfs.MetaNodeConn)
		pAsyncChunkReq := &mp.AsyncChunkReq{
			VolID:         chunk.CFile.cfs.VolID,
			ParentInodeID: chunk.CFile.ParentInodeID,
			Name:          chunk.CFile.Name,
			ChunkID:       in.ChunkID,
			CommitSize:    in.DataLen,
			BlockGroupID:  in.BlockGroupID,
		}
		ctx, _ := context.WithTimeout(context.Background(), 5*time.Second)
		_, err2 := mc.AsyncChunk(ctx, pAsyncChunkReq)
		if err2 != nil {
			break
		}

		// comfirm data
		chunk.CFile.DataCacheLocker.Lock()
		//cfile.DataCache[in.CommitID].timer.Stop()
		delete(chunk.CFile.DataCache, in.CommitID)
		chunk.CFile.DataCacheLocker.Unlock()

		chunk.CFile.updateChunkSize(chunk.ChunkInfo, int32(in.DataLen))
	}
}

func (chunk *Chunk) WriteRetryHandle() error {

	length := len(chunk.CFile.DataCache)
	if length == 0 {
		return nil
	}

	tmpchunk := chunk.CFile.AllocateChunk(false)
	if tmpchunk == nil {
		return errors.New("AllocateChunk error")
	}

	sortedKeys := make([]int, 0)

	for k := range chunk.CFile.DataCache {
		sortedKeys = append(sortedKeys, int(k))
	}
	sort.Ints(sortedKeys)
	logger.Debug("WriteRetryHandle AllocateChunk success, begin to retry item num:%v, commitIDs: %v", length, sortedKeys)

	var chunkSize int

	for _, vv := range sortedKeys {

		bufLen := chunk.CFile.DataCache[uint64(vv)].DataBuf.Len()
		req := dp.WriteChunkReq{ChunkID: tmpchunk.ChunkInfo.ChunkID,
			BlockGroupID: tmpchunk.ChunkInfo.BlockGroupWithHost.BlockGroupID,
			Databuf:      chunk.CFile.DataCache[uint64(vv)].DataBuf.Bytes(),
			CommitID:     uint64(vv),
		}
		for _, v := range tmpchunk.ChunkInfo.BlockGroupWithHost.Hosts {

			conn := chunk.CFile.newDataConn(v)
			if conn == nil {
				logger.Error("WriteRetryHandle newDataConn Failed err")
				return fmt.Errorf("WriteRetryHandle newDataConn Failed")
			}
			dc := dp.NewDataNodeClient(conn)
			ctx, _ := context.WithTimeout(context.Background(), 5*time.Second)
			_, err := dc.WriteChunk(ctx, &req)
			if err != nil {
				logger.Error("WriteRetryHandle WriteChunk to DataNode Host Failed err %v", err)
				chunk.CFile.delErrDataConn(v)
				return err
			}
		}

		logger.Debug("WriteRetryHandle write CommitID %v bufLen %v success", vv, bufLen)
		chunkSize += bufLen
		chunk.CFile.curNum = uint64(vv)
	}

	mc := mp.NewMetaNodeClient(chunk.CFile.cfs.MetaNodeConn)
	pAsyncChunkReq := &mp.AsyncChunkReq{
		VolID:         chunk.CFile.cfs.VolID,
		ParentInodeID: chunk.CFile.ParentInodeID,
		Name:          chunk.CFile.Name,
		ChunkID:       tmpchunk.ChunkInfo.ChunkID,
		CommitSize:    uint32(chunkSize),
		BlockGroupID:  tmpchunk.ChunkInfo.BlockGroupWithHost.BlockGroupID,
	}
	ctx, _ := context.WithTimeout(context.Background(), 5*time.Second)
	_, err2 := mc.AsyncChunk(ctx, pAsyncChunkReq)
	if err2 != nil {
		logger.Error("WriteRetryHandle AsyncChunk to MetaNode Failed err %v", err2)
		return err2
	}
	logger.Debug("WriteRetryHandle success with ChunkID %v ChunkSize %v", tmpchunk.ChunkInfo.ChunkID, chunkSize)

	chunk.CFile.updateChunkSize(tmpchunk.ChunkInfo, int32(chunkSize))
	return nil
}

// update ChunkSize and FileSize only if chunk's data has be writted to datanode and syn to metanode
func (cfile *CFile) updateChunkSize(chunkinfo *mp.ChunkInfoWithBG, length int32) {

	chunkNum := len(cfile.chunks)
	if chunkNum != 0 && cfile.chunks[chunkNum-1].ChunkID == chunkinfo.ChunkID {
		cfile.chunks[chunkNum-1].ChunkSize += length
	} else {
		newchunkinfo := &mp.ChunkInfoWithBG{ChunkID: chunkinfo.ChunkID, ChunkSize: length, BlockGroupWithHost: chunkinfo.BlockGroupWithHost}
		cfile.chunks = append(cfile.chunks, newchunkinfo)
	}
	cfile.FileSize += int64(length)
}

// Sync ...
func (cfile *CFile) Sync() int32 {
	if cfile.Status == FileError {
		return -1
	}

	cfile.appendWrite(nil, 0, true)

	return 0
}

// Sync ...
func (cfile *CFile) Flush() int32 {
	if cfile.isWrite == false{
		return 0
	}
	
	if cfile.Status == FileError {
		return -1
	}
	
	cfile.appendWrite(nil, 0, true)

	return 0
}

// Close ...
func (cfile *CFile) CloseWrite() int32 {
	/*if cfile.Status == FileError {
		return -1
	} */

	cfile.appendWrite(nil, 0, true)

	cfile.Closing = true
	logger.Debug("CloseWrite close cfile.DataQueue")
	close(cfile.DataQueue)
	<-cfile.CloseSignal
	logger.Debug("CloseWrite recv CloseSignal!")

	return 0
}

// Close ...
func (cfile *CFile) Close() int32 {
	cfile.delAllDataConn()
	return 0
}
