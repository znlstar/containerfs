package main

import (
	"errors"
	"fmt"
	"github.com/tiglabs/containerfs/logger"
	"github.com/tiglabs/containerfs/proto/dp"
	"github.com/tiglabs/containerfs/proto/mp"
	"github.com/tiglabs/containerfs/proto/vp"
	"github.com/tiglabs/containerfs/raftopt"
	"github.com/tiglabs/containerfs/utils"
	"github.com/tiglabs/raft/storage/wal"
	"golang.org/x/net/context"

	"strings"
	"sync"
	"time"
)

const (
	//BlockGroupSize 5GB
	BlockGroupSize = 5
)

const (
	blockGroupFree = 0
	blockGroupFull = 2
)

var errNotLeader = fmt.Errorf("not leader")

type cluster struct {
	sync.Mutex
	RaftGroup   *raftopt.ClusterKvStateMachine
	RaftStorage *wal.Storage
	IsLoaded    bool
}

//todo: not implemented yet
func (vs VolMgrServer) delDataNode(host string) error {
	return nil
}

// func (vs VolMgrServer) getAllVolume() ([]*vp.Volume, error) {
// 	v, _ := vs.Cluster.RaftGroup.VolumeGetAll(1)
// 	var vols []*vp.Volume
// 	for _, vv := range v {
// 		volume := vp.Volume{}
// 		err := pbproto.Unmarshal(vv.V, &volume)
// 		if err != nil {
// 			return []*vp.Volume{}, err
// 		}
// 		vols = append(vols, &volume)
// 	}
// 	return vols, nil
// }

// for DataNode registry
func (s *VolMgrServer) DataNodeRegistry(ctx context.Context, in *vp.DataNode) (*vp.DataNodeRegistryAck, error) {
	ack := vp.DataNodeRegistryAck{}
	logger.Debug("datanode registry:%v", in)
	err := s.Cluster.RaftGroup.DataNodeSet(1, in.Host, in)
	if err != nil {
		logger.Error("DataNode(%v) Register to MetaNode failed:%v", in.Host, err)
		return &ack, err
	}

	logger.Debug("DataNode(%v) Register to MetaNode success", in.Host)
	return &ack, nil
}

func (s *VolMgrServer) GetDataNode(ctx context.Context, in *vp.GetDataNodeReq) (*vp.GetDataNodeAck, error) {
	ack := vp.GetDataNodeAck{}
	v, err := s.Cluster.RaftGroup.DataNodeGetAll(1)
	if err != nil {
		logger.Error("GetAllDataNode Info failed:%v", err)
		return &ack, err
	}

	ack.DataNodes = v
	ack.Ret = 0
	return &ack, nil
}

func (s *VolMgrServer) DelDataNode(ctx context.Context, in *vp.DelDataNodeReq) (*vp.DelDataNodeAck, error) {
	ack := vp.DelDataNodeAck{}
	err := s.delDataNode(in.Host)
	if err != nil {
		logger.Error("Delete DataNode(%v) failed, err:%v", in.Host, err)
		return &ack, err
	}
	ack.Ret = 0
	return &ack, nil
}

//todo: check leader
func (s *VolMgrServer) CreateVol(ctx context.Context, in *vp.CreateVolReq) (*vp.CreateVolAck, error) {

	s.Lock()
	defer s.Unlock()
	ack := vp.CreateVolAck{}

	//todo: check raft leader
	if !s.RaftServer.IsLeader(1) {
		ack.Ret = -1
		return &ack, errNotLeader
	}

	voluuid, err := utils.GenUUID()
	if err != nil {
		logger.Error("Create volume uuid err:%v", err)
		return &ack, err
	}

	rgID, err := s.Cluster.RaftGroup.RGIDGET(1)
	if err != nil {
		logger.Error("Create Volume name:%v uuid:%v raftGroupID error:%v", voluuid, in.VolName, err)
		return &ack, err
	}
	//choose 3 metanodes as a raftgroup
	mng, err := s.chooseMetaNodes()
	if err != nil {
		logger.Error("Create Volume name:%v  error:%v", in.VolName, err)
		return &ack, err
	}

	//the volume need block group total numbers
	var blkgrpnum int32
	if in.SpaceQuota%BlkSizeG == 0 {
		blkgrpnum = in.SpaceQuota / BlkSizeG
	} else {
		blkgrpnum = in.SpaceQuota/BlkSizeG + 1
		in.SpaceQuota = blkgrpnum * BlkSizeG
	}
	if blkgrpnum > 6 {
		blkgrpnum = 6
	}

	v, err := s.Cluster.RaftGroup.DataNodeGetAll(1)
	if err != nil {
		logger.Error("GetAllDataNode Info failed:%v for CreateVol", err)
		return &ack, err
	}
	for _, vv := range v {
		logger.Debug("DataNode: %v", vv.Host)
	}
	inuseNodes := make(map[string][]*vp.DataNode)
	allip := make([]string, 0)

	// pass bad status and free not enough datanodes
	for _, vv := range v {
		if vv.Status != 0 || vv.Free < 30 || vv.Tier != in.Tier {
			continue
		}
		tmp := strings.Split(vv.Host, ":")
		k := tmp[0]
		inuseNodes[k] = append(inuseNodes[k], vv)
	}

	for k, _ := range inuseNodes {
		allip = append(allip, k)
	}

	if len(allip) < 3 {
		logger.Error("Create Volume:%v Tier:%v but DataNode nums:%v less than 3, so forbid CreateVol", voluuid, in.Tier, len(allip))
		ack.Ret = -1
		return &ack, nil
	}

	vol := &vp.Volume{
		UUID:          voluuid,
		Name:          in.VolName,
		Tier:          in.Tier,
		TotalSize:     in.SpaceQuota,
		AllocatedSize: blkgrpnum * 5,
		RGID:          rgID,
	}

	dataNodesUsedMap := make(map[string][]uint64)
	var blockGroups []*mp.BlockGroup
	for i := int32(0); i < blkgrpnum; i++ {
		bgID, err := s.Cluster.RaftGroup.BGIDGET(1)
		if err != nil {
			logger.Error("AllocateBGID for CreateVol failed, err:%v", err)
			return &ack, err
		}

		idxs := utils.GenerateRandomNumber(0, len(allip), 3)
		if len(idxs) != 3 {
			ack.Ret = -1
			return &ack, nil
		}

		var hosts []string
		bg := &vp.BlockGroup{BlockGroupID: bgID, FreeSize: BlockGroupSize * 1024 * 1024 * 1024}
		for n := 0; n < 3; n++ {
			ipkey := allip[idxs[n]]
			idx := utils.GenerateRandomNumber(0, len(inuseNodes[ipkey]), 1)
			if len(idx) <= 0 {
				ack.Ret = -1
				return &ack, nil
			}
			host := inuseNodes[ipkey][idx[0]].Host
			bg.Hosts = append(bg.Hosts, host)
			hosts = append(hosts, host)

		}
		err = s.Cluster.RaftGroup.BlockGroupSet(bgID, bg)
		if err != nil {
			logger.Error("Create Volume:%v Set blockgroup:%v blocks:%v failed:%v", voluuid, bgID, bg, err)
			return &ack, err
		}
		//to set hosts blockgroups
		for _, host := range hosts {
			dataNodesUsedMap[host] = append(dataNodesUsedMap[host], bgID)
			if err = s.Cluster.RaftGroup.DataNodeBGPAddBGP(host, bgID); err != nil {
				logger.Error("failed to add bgp to datandoe: %v", err)
				return &ack, err
			}
		}
		logger.Debug("Create Volume:%v Tier:%v Set one blockgroup:%v blocks:%v to Cluster Map success", voluuid, in.Tier, bgID, bg)
		vol.BlockGroups = append(vol.BlockGroups, bgID)
		blockGroup := &mp.BlockGroup{}
		blockGroup.BlockGroupID = bgID
		blockGroup.FreeSize = BlockGroupSize * 1024 * 1024 * 1024
		blockGroups = append(blockGroups, blockGroup)
	}

	s.Cluster.RaftGroup.VolumeSet(1, voluuid, vol)
	ack.Ret = 0
	ack.UUID = voluuid
	ack.RaftGroupID = rgID

	var volumeValue mp.VolumeValue
	var volumePeers []*mp.VolumePeer

	for _, mn := range mng {
		volumePeers = append(volumePeers, &mp.VolumePeer{NodeID: mn.Id, Host: mn.Host})
	}
	volumeValue.RaftGroupID = rgID
	volumeValue.VolumePeers = volumePeers
	volumeValue.BlockGroups = blockGroups
	flag := true
	for _, mn := range mng {
		conn, err := utils.Dial(mn.Host + ":9901")
		if err != nil {
			logger.Error("Dial metanode[%v] failed: %v", mn.Host, err)
			flag = false
			break
		}

		mc := mp.NewMetaNodeClient(conn)

		pCreateNameSpaceReq := &mp.CreateNameSpaceReq{
			Volume: &volumeValue,
			VolID:  voluuid,
		}
		ctx, _ := context.WithTimeout(context.Background(), 5*time.Second)
		pCreateNameSpaceAck, err := mc.CreateNameSpace(ctx, pCreateNameSpaceReq)
		if err != nil {
			logger.Error("CreateVolume CreateNameSpace failed host : %v err : %v", mn.Host, err)
			flag = false
			break
		}
		if pCreateNameSpaceAck.Ret != 0 {
			logger.Error("CreateVolume CreateNameSpace failed host : %v ret : %v", mn.Host, pCreateNameSpaceAck.Ret)
			flag = false
			break
		}

	}

	//clean kv data on fail
	if !flag {
		//clean DataNodeBGP kv
		for host, _ := range dataNodesUsedMap {
			s.Cluster.RaftGroup.DataNodeBGPDelBGP(host, dataNodesUsedMap[host])
		}
	}

	metaNodeRG := &vp.MetaNodeRG{}
	metaNodeRG.RGID = rgID
	metaNodeRG.UUID = voluuid
	for _, mn := range mng {
		metaNode := &vp.MetaNode{Id: mn.Id}
		metaNodeRG.MetaNodes = append(metaNodeRG.MetaNodes, metaNode)
	}
	err = s.Cluster.RaftGroup.MetaNodeRGSet(rgID, metaNodeRG)
	if err != nil {
		logger.Error("set kv failed: %v", err)
		return &ack, err
	}
	return &ack, nil
}

//todo: not implemented yet
func (s *VolMgrServer) ExpandVolTS(ctx context.Context, in *vp.ExpandVolTSReq) (*vp.ExpandVolTSAck, error) {
	ack := vp.ExpandVolTSAck{}
	ack.Ret = 0
	return &ack, nil
}

//todo: not implemented yet
func (s *VolMgrServer) ExpandVolRS(ctx context.Context, in *vp.ExpandVolRSReq) (*vp.ExpandVolRSAck, error) {

	s.Lock()
	defer s.Unlock()

	ack := vp.ExpandVolRSAck{}
	ack.Ret = 0
	return &ack, nil
}

//todo: not implemented yet
func (s *VolMgrServer) DelVolRSForExpand(ctx context.Context, in *vp.DelVolRSForExpandReq) (*vp.DelVolRSForExpandAck, error) {
	ack := vp.DelVolRSForExpandAck{}
	ack.Ret = 0
	return &ack, nil
}

func (s *VolMgrServer) DeleteVol(ctx context.Context, in *vp.DeleteVolReq) (*vp.DeleteVolAck, error) {
	ack := vp.DeleteVolAck{}

	volume, err := s.Cluster.RaftGroup.VolumeGet(1, in.UUID)
	if err != nil {
		logger.Error("Get BGPS map from Cluster MetaNodeAddr for DeleteVol:%v failed, err:%v", in.UUID, err)
		return &ack, err
	}

	delBlockGroupBadNum := 0
	var bgp *vp.BlockGroup
	for _, v := range volume.BlockGroups {
		if bgp, err = s.Cluster.RaftGroup.BlockGroupGet(v); err != nil {
			logger.Error("DeleteVol BlockGroupGet failed: %v, bgID: %v", err, v)
			delBlockGroupBadNum = delBlockGroupBadNum + 1
			continue
		}
		delDataNodeBlkBadNum := 0
		for _, vv := range bgp.Hosts {
			datanodeKey := vv
			tmpDataNode, err := s.Cluster.RaftGroup.DataNodeGet(1, datanodeKey)
			if err != nil {
				logger.Error("Get DataNode:%v map from Cluster MetaNodeAddr for Update Free+5:%v failed for DeleteVol, err:%v", datanodeKey, err)
				delDataNodeBlkBadNum += 1
				continue
			}

			tmpDataNode.Free = tmpDataNode.Free + 5
			err = s.Cluster.RaftGroup.DataNodeSet(1, datanodeKey, tmpDataNode)
			if err != nil {
				logger.Error("Set DataNode:%v map value.Free Add 5G from Cluster MetaNodeAddr for failed for DeleteVol, err:%v", datanodeKey, err)
				delDataNodeBlkBadNum += 1
				continue
			}
		}

		bgKey := in.UUID + fmt.Sprintf("-%d", bgp.BlockGroupID)
		err = s.Cluster.RaftGroup.BlockGroupDel(1, bgKey)
		if err != nil || delDataNodeBlkBadNum != 0 {
			logger.Error("Delete BG:%v from Cluster MetaNodeAddr failed", bgKey)
			delBlockGroupBadNum += 1
		}
	}
	if delBlockGroupBadNum != 0 {
		ack.Ret = -1
		return &ack, nil
	}
	err = s.Cluster.RaftGroup.VolumeDel(1, in.UUID)
	if err != nil {
		logger.Error("Delete Volume:%v map from Cluster MetaNodeAddr failed, err:%v", in.UUID, err)
		return &ack, err
	}

	logger.Debug("Delete Volume:%v from Cluster MetadataAddr Success", in.UUID)
	return &ack, nil
}

//todo: not implemented yet
func (s *VolMgrServer) Migrate(ctx context.Context, in *vp.MigrateReq) (*vp.MigrateAck, error) {
	ack := vp.MigrateAck{}

	//DataNode
	dataNode, err := s.Cluster.RaftGroup.DataNodeGet(1, in.DataNodeHost)
	if err != nil {
		logger.Error("DataNodeGet for datanode[%v] failed: %v", in.DataNodeHost, err)
		return &ack, err
	}

	tier := dataNode.Tier

	//DataNodeBGPS
	dataNodeBGPS, err := s.Cluster.RaftGroup.DataNodeBGPGet(in.DataNodeHost)
	if err != nil {
		logger.Error("DataNodeBGPGet for datanode[%v] failed: %v", in.DataNodeHost, err)
		return &ack, err
	}
	go func() {
		var ret int
		for _, bg := range dataNodeBGPS.BGPS {
			logger.Debug("Begin Migrage BlockGroup %v ...", bg)
			ret = s.BeginMigrate(bg, in.DataNodeHost, tier)
			logger.Debug("End Migrage BlockGroup %v ret %v ...", bg, ret)
		}
	}()

	ack.Ret = 0
	return &ack, nil
}

//todo: not implemented yet
func (s *VolMgrServer) BeginMigrate(bgID uint64, badHost string, tier string) int {
	var shost string

	blockGroup, err := s.Cluster.RaftGroup.BlockGroupGet(bgID)
	if err != nil {
		logger.Error("BlockGroupGet[%v] faild: %v", bgID, err)
		return -1
	}
	for _, host := range blockGroup.Hosts {
		if host != badHost {
			shost = host
			break
		}
	}
	//All DataNodes
	var dataNodes []*vp.DataNode
	v, err := s.Cluster.RaftGroup.DataNodeGetAll(1)
	if err != nil {
		logger.Error("DataNodeGetAll failed: %v", err)
		return -3
	}
	for _, vv := range v {
		flag := true
		for _, host := range blockGroup.Hosts {
			if host == vv.Host {
				flag = false
				break
			}
		}
		logger.Debug("DataNode: %v", vv)
		if flag && vv.Status == 0 && vv.Tier == tier && vv.Free > BlockGroupSize {
			dataNodes = append(dataNodes, vv)
		}
	}

	if len(dataNodes) < 1 {
		logger.Error("No enough free datanode hosts available")
		return -5
	}

	idxs := utils.GenerateRandomNumber(0, len(dataNodes), 1)
	dHost := dataNodes[idxs[0]].Host
	//migrate blockgroup
	ret := s.migrateBlockGroup(bgID, shost, dHost)
	if ret != 0 {
		return -6
	}

	//update datanode/datanodebgps/blockgroup
	dataNode, err := s.Cluster.RaftGroup.DataNodeGet(1, dHost)
	if err != nil {
		logger.Error("DataNodeGet[%v] failed: %v", dHost, err)
		return -7
	}
	dataNode.Free = dataNode.Free - BlockGroupSize

	if err = s.Cluster.RaftGroup.DataNodeSet(1, dHost, dataNode); err != nil {
		logger.Error("DataNodeSet failed: %v", err)
		return -10
	}
	dataNodeBGPS, err := s.Cluster.RaftGroup.DataNodeBGPGet(dHost)
	if err != nil && err != raftopt.ErrKeyNotFound {
		logger.Error("DataNodeBGPGet failed: %v", err)
		return -11
	}
	if err == raftopt.ErrKeyNotFound {
		dataNodeBGPS = &vp.DataNodeBGPS{Host: dHost}
	}
	flag := true
	// for _, v := range dataNodeBGPS.BGPS {
	// 	if v == bgID {
	// 		flag = false
	// 		break
	// 	}
	// }
	if flag {
		dataNodeBGPS.BGPS = append(dataNodeBGPS.BGPS, bgID)
		err = s.Cluster.RaftGroup.DataNodeBGPSet(dHost, dataNodeBGPS)
		if err != nil {
			logger.Error("DataNodeBGPSet failed: %v", err)
			return -14
		}
	}

	for i, _ := range blockGroup.Hosts {
		if blockGroup.Hosts[i] == badHost {
			blockGroup.Hosts[i] = dHost
		}
	}
	if err = s.Cluster.RaftGroup.BlockGroupSet(bgID, blockGroup); err != nil {
		logger.Error("BlockGroupSet failed: %v", err)
		return -15
	}
	return 0
}

func (s *VolMgrServer) migrateBlockGroup(bgID uint64, shost string, dhost string) int32 {
	//migrate
	pRecvMigrateReq := &dp.RecvMigrateReq{BlockGroupID: bgID, DstHost: dhost}
	conn, err := utils.Dial(shost)
	if err != nil {
		logger.Error("Dial datanode[%v] failed: %v", shost, err)
		return -1
	}
	defer conn.Close()
	dc := dp.NewDataNodeClient(conn)
	pRecvMigrateAck, err := dc.RecvMigrateMsg(context.Background(), pRecvMigrateReq)
	if err != nil {
		logger.Error("RecvMigrateMsg failed: %v", err)
		return -2
	}
	if pRecvMigrateAck.Ret != 0 {
		logger.Error("RecvMigrateMsg Ret[%v] failed", pRecvMigrateAck.Ret)
		return -3
	}
	return 0

}

func (s *VolMgrServer) GetBlockGroupByID(ctx context.Context, in *vp.GetBlockGroupByIDReq) (*vp.GetBlockGroupByIDAck, error) {
	ack := vp.GetBlockGroupByIDAck{}

	blockGroup, err := s.Cluster.RaftGroup.BlockGroupGet(in.BlockGroupID)
	if err != nil {
		return &ack, err
	}
	ack.BlockGroup = blockGroup
	return &ack, nil
}

func (s *VolMgrServer) DetectDataNodes() {

	vv, err := s.Cluster.RaftGroup.DataNodeGetAll(1)
	if err != nil {
		logger.Error("GetAllDataNode Info failed:%v for detectDataNodes", err)
		return
	}

	for _, v := range vv {
		go s.DetectDataNode(v)
	}
}

func (s *VolMgrServer) DetectDataNode(v *vp.DataNode) {
	dnAddr := v.Host
	conn, err := utils.Dial(dnAddr)
	if err != nil {
		if v.Status == 0 {
			logger.Error("Detect DataNode:%v failed : Dial to DataNode failed !", dnAddr)
			v.Status = 1
			s.SetDataNodeMap(v)
			logger.Debug("Detect Datanode(%v) status from good to bad, set DataNode map success", dnAddr)
			//UpdateBlock(metaServer, v.Ip, v.Port, 1)
		}
		return
	}
	defer conn.Close()
	c := dp.NewDataNodeClient(conn)
	var DataNodeHealthCheckReq dp.DataNodeHealthCheckReq
	pDataNodeHealthCheckAck, err := c.DataNodeHealthCheck(context.Background(), &DataNodeHealthCheckReq)
	if err != nil {
		if v.Status == 0 {
			v.Status = 1
			s.SetDataNodeMap(v)
			logger.Debug("Detect DataNode(%v) status from good to bad, set DataNode map success", dnAddr)
			//UpdateBlock(metaServer, v.Ip, v.Port, 1)
		}
		return
	}

	if pDataNodeHealthCheckAck.Status != 0 {
		if v.Status == 0 {
			v.Status = pDataNodeHealthCheckAck.Status
			v.Used = pDataNodeHealthCheckAck.Used
			s.SetDataNodeMap(v)
			logger.Debug("Detect DataNode(%v) status from good to bad, set DataNode map success", dnAddr)
			//UpdateBlock(metaServer, v.Ip, v.Port, 1)
		}
		return
	} else {
		v.Status = 0
	}

	v.Used = pDataNodeHealthCheckAck.Used
	s.SetDataNodeMap(v)
	return
}

func (s *VolMgrServer) SetDataNodeMap(v *vp.DataNode) int {
	key := v.Host
	err := s.Cluster.RaftGroup.DataNodeSet(1, key, v)
	if err != nil {
		logger.Error("Datanode  failed:%v", err)
		return -1
	}
	dataNodeBGP, err := s.Cluster.RaftGroup.DataNodeBGPGet(v.Host)
	if err == raftopt.ErrKeyNotFound {
		return 0
	}
	if err != nil {
		logger.Error("SetDataNodeMap [%v %v] failed: %v", v.Host, v.Status, err)
		return -2
	}
	for _, bgId := range dataNodeBGP.BGPS {
		blockGroup, err := s.Cluster.RaftGroup.BlockGroupGet(bgId)
		if err != nil {
			logger.Error("SetDataNodeMap failed: %v", err)
			return -3
		}
		blockGroup.Status = v.Status
		if err = s.Cluster.RaftGroup.BlockGroupSet(bgId, blockGroup); err != nil {
			logger.Error("SetDataNodeMap failed: %v", err)
			return -4
		}
	}
	return 0
}

//todo: not implemented yet
func (s *VolMgrServer) delMetaNode(host string) error {
	//todo:update all involved metanode raftgroups
	//todo:delte metanode from kvsm
	return nil
}
func (s *VolMgrServer) chooseMetaNodes() ([]*vp.MetaNode, error) {
	mnv, err := s.Cluster.RaftGroup.MetaNodeGetAll(1)
	if err != nil {
		logger.Error("VolMgrServer.chooseMetaNodes failed:%v", err)
		return nil, err
	}

	//kickout bad metanodes
	var tmpV []*vp.MetaNode
	for _, node := range mnv {
		if node.Status == 0 {
			tmpV = append(tmpV, node)
		}
	}

	//at least 3 metanodes
	idxs := utils.GenerateRandomNumber(0, len(tmpV), 3)
	if len(idxs) < 3 {
		err = errors.New("less than 3 metanodes available")
		return nil, err
	}

	var mns []*vp.MetaNode
	for _, idx := range idxs {
		mns = append(mns, tmpV[idx])
	}
	return mns, nil
}

// for MetaNode registry
func (s *VolMgrServer) MetaNodeRegistry(ctx context.Context, in *vp.MetaNode) (*vp.MetaNodeRegistryAck, error) {
	ack := vp.MetaNodeRegistryAck{}

	err := s.Cluster.RaftGroup.MetaNodeSet(1, in.Id, in)
	if err != nil {
		logger.Error("MetaNode(%v),Id:%v Register to VolMgr failed:%v", in.Host, in.Id, err)
		return &ack, err
	}

	logger.Debug("MetaNode(%v) Register to VolMgr success", in.Host)
	return &ack, nil
}

//todo: not implemented yet
func (s *VolMgrServer) DetectMetaNodes() {
	return
}

//todo: not implemented yet
func (s *VolMgrServer) DetectMetaNode(v *vp.MetaNode) {
	return
}

//todo: not implemented yet
func (s *VolMgrServer) SetMetaNodeMap(v *vp.MetaNode) int {
	return 0
}

// GetMetaLeader ...
func (s *VolMgrServer) GetVolMgrRG(ctx context.Context, in *vp.GetVolMgrRGReq) (*vp.GetVolMgrRGAck, error) {
	ack := vp.GetVolMgrRGAck{}
	leaderID, _ := s.RaftServer.LeaderTerm(1)
	if leaderID <= 0 {
		ack.Ret = 1
		return &ack, nil
	}
	ack.Ret = 0
	ack.Leader = raftopt.ClusterAddrDatabase[leaderID].Grpc
	for _, rft := range raftopt.ClusterAddrDatabase {
		ack.Peers = append(ack.Peers, rft.Grpc)
	}
	return &ack, nil
}

// GetMetaLeader ...
func (s *VolMgrServer) GetMetaNodeRG(ctx context.Context, in *vp.GetMetaNodeRGReq) (*vp.GetMetaNodeRGAck, error) {
	ack := vp.GetMetaNodeRGAck{}
	vol, err := s.Cluster.RaftGroup.VolumeGet(1, in.UUID)
	if err != nil {
		logger.Error("get metanode raftgroup failed: %v", err)
		ack.Ret = -1
		return &ack, err
	}

	logger.Debug("vol rgid:%v", vol.RGID)
	metaNodeRG, err := s.Cluster.RaftGroup.MetaNodeRGGet(vol.RGID)
	if err != nil {
		logger.Error("get metanode raftgroup failed: %v", err)
		ack.Ret = -3
		return &ack, err
	}

	metaNodes, err := s.getMetaNodesViaIds(metaNodeRG.MetaNodes)
	if err != nil {
		logger.Error("getMetaNodesViaIds failed: %v", err)
		ack.Ret = -5
		return &ack, err
	}
	ack.MetaNodes = metaNodes
	pGetMetaNodeLeaderReq := &mp.GetMetaLeaderReq{VolID: in.UUID}
	for _, mn := range metaNodes {
		metaCon, err := utils.Dial(mn.Host + ":9901")
		if err != nil {
			logger.Error("dail metanode %s failed: %v", mn.Host, err)
			continue
		}
		defer metaCon.Close()
		mc := mp.NewMetaNodeClient(metaCon)
		mnAck, err := mc.GetMetaLeader(context.Background(), pGetMetaNodeLeaderReq)
		if err != nil {
			logger.Error("GetMetaLeader from %s failed: %v", mn.Host, err)
			continue
		}
		logger.Debug("ack.Leader:%v, ack.Ret:%v", mnAck.Leader, mnAck.Ret)
		ack.Leader = mnAck.Leader
		break
	}
	// if ack.Leader == "" {
	// 	ack.Ret = -6
	// }
	return &ack, nil
}

// GetMetaNodeBGPS ...
func (s *VolMgrServer) GetMetaNodeRGPeers(ctx context.Context, in *vp.GetMetaNodeRGPeersReq) (*vp.GetMetaNodeRGPeersAck, error) {
	ack := vp.GetMetaNodeRGPeersAck{}
	v, _ := s.Cluster.RaftGroup.MetaNodeRGGetAll()
	for _, rg := range v {
		metaNodes, err := s.getMetaNodesViaIds(rg.MetaNodes)
		if err != nil {
			logger.Error("getMetaNodesViaIds failed: %v", err)
			ack.Ret = -2
			return &ack, err
		}
		rg.MetaNodes = metaNodes
		for _, v := range rg.MetaNodes {
			logger.Debug("metanode id: %v", v.Id)
			if v.Id == in.MetaNodeID {
				ack.RaftGroups = append(ack.RaftGroups, rg)
				break
			}
		}
	}
	return &ack, nil
}

func (s *VolMgrServer) getMetaNodesViaIds(in []*vp.MetaNode) ([]*vp.MetaNode, error) {
	var metaNodes []*vp.MetaNode
	if len(in) < 1 {
		return metaNodes, nil
	}
	metaNodes = make([]*vp.MetaNode, len(in))

	for i, mn := range in {
		mID := mn.Id
		metaNode, err := s.Cluster.RaftGroup.MetaNodeGet(1, mID)
		if err != nil {
			logger.Error("get metanode via id failed: %v", err)
			return metaNodes, err
		}
		metaNodes[i] = metaNode
	}
	return metaNodes, nil
}
