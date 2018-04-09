//package main
package volmgr

import (
	"fmt"
	"path"
	"strconv"
	"strings"
	"time"

	"github.com/tiglabs/containerfs/logger"
	"github.com/tiglabs/containerfs/proto/dp"
	"github.com/tiglabs/containerfs/proto/mp"
	"github.com/tiglabs/containerfs/proto/vp"
	"github.com/tiglabs/containerfs/raftopt"
	"github.com/tiglabs/containerfs/utils"
	"golang.org/x/net/context"
)

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

	logger.Error("Create Volume name:%v uuid:%v size:%v", in.VolName, voluuid, in.SpaceQuota)
	//choose 3 metanodes as a raftgroup
	mng, err := s.chooseMetaNodes()
	if err != nil {
		logger.Error("Create Volume name:%v  error:%v", in.VolName, err)
		return &ack, err
	}

	//the volume need block group total numbers
	var blkgrpnum int32
	if in.SpaceQuota%utils.BlkSizeG == 0 {
		blkgrpnum = in.SpaceQuota / utils.BlkSizeG
	} else {
		blkgrpnum = in.SpaceQuota/utils.BlkSizeG + 1
		in.SpaceQuota = blkgrpnum * utils.BlkSizeG
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

	copies, _ := strconv.Atoi(in.Copies)

	if len(allip) < copies {
		logger.Error("Create Volume:%v Tier:%v Copies:%v but DataNode nums:%v less than Copies:%v, so forbid CreateVol", voluuid, in.Tier, copies, len(allip), copies)
		ack.Ret = -1
		return &ack, nil
	}

	vol := &vp.Volume{
		UUID:          voluuid,
		Name:          in.VolName,
		Tier:          in.Tier,
		Copies:        int32(copies),
		TotalSize:     in.SpaceQuota,
		AllocatedSize: blkgrpnum * 5,
		RGID:          rgID,
	}

	dataNodesUsedMap := make(map[string][]uint64)
	dataNodesForUpdate := make(map[string]*vp.DataNode)
	var blockGroups []*mp.BlockGroup
	for i := int32(0); i < blkgrpnum; i++ {
		bgID, err := s.Cluster.RaftGroup.BGIDGET(1)
		if err != nil {
			logger.Error("AllocateBGID for CreateVol failed, err:%v", err)
			return &ack, err
		}

		idxs := utils.GenerateRandomNumber(0, len(allip), copies)
		if len(idxs) != copies {
			ack.Ret = -5
			return &ack, nil
		}
		var hosts []string
		bg := &vp.BlockGroup{BlockGroupID: bgID,
			RGID:     rgID,
			VolID:    voluuid,
			FreeSize: utils.BlockGroupSize}

		for n := 0; n < copies; n++ {
			ipkey := allip[idxs[n]]
			idx := utils.GenerateRandomNumber(0, len(inuseNodes[ipkey]), 1)
			if len(idx) <= 0 {
				ack.Ret = -1
				return &ack, nil
			}
			dataNode := inuseNodes[ipkey][idx[0]]
			bg.Hosts = append(bg.Hosts, dataNode.Host)
			hosts = append(hosts, dataNode.Host)
			dataNode.Free = dataNode.Free - 5
			dataNodesForUpdate[dataNode.Host] = dataNode
			if inuseNodes[ipkey][idx[0]].Free < 30 {
				inuseNodes[ipkey] = append(inuseNodes[ipkey][:idx[0]], inuseNodes[ipkey][idx[0]+1:]...)
			}
		}
		var newAllIp []string
		for _, ip := range allip {
			if len(inuseNodes[ip]) <= 0 {
				delete(inuseNodes, ip)
			} else {
				newAllIp = append(newAllIp, ip)
			}
		}
		allip = newAllIp

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
		blockGroup.FreeSize = utils.BlockGroupSize
		blockGroups = append(blockGroups, blockGroup)
	}

	logger.Error("CreateVolume =============== %v", vol)
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

	// update datanode kv data
	for host, dataNode := range dataNodesForUpdate {
		s.Cluster.RaftGroup.DataNodeSet(1, host, dataNode)
	}
	return &ack, nil
}

//todo: check leader
func (s *VolMgrServer) ExpandVol(ctx context.Context, in *vp.ExpandVolReq) (*vp.ExpandVolAck, error) {

	s.Lock()
	defer s.Unlock()
	ack := vp.ExpandVolAck{}

	//todo: check raft leader
	if !s.RaftServer.IsLeader(1) {
		ack.Ret = -1
		return &ack, errNotLeader
	}

	vol, err := s.Cluster.RaftGroup.VolumeGet(1, in.UUID)
	if err != nil {
		logger.Error("Get volume info[%v] faield: %v", in.UUID, err)
		return &ack, err
	}

	//the volume need block group total numbers
	var blkgrpnum int32
	if in.Space%utils.BlkSizeG == 0 {
		blkgrpnum = in.Space / utils.BlkSizeG
	} else {
		blkgrpnum = in.Space/utils.BlkSizeG + 1
		in.Space = blkgrpnum * utils.BlkSizeG
	}
	if blkgrpnum > 6 {
		blkgrpnum = 6
	}
	vol.TotalSize = vol.TotalSize + in.Space
	vol.AllocatedSize = vol.AllocatedSize + blkgrpnum*5
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
		if vv.Status != 0 || vv.Free < 30 || vv.Tier != vol.Tier {
			continue
		}
		tmp := strings.Split(vv.Host, ":")
		k := tmp[0]
		inuseNodes[k] = append(inuseNodes[k], vv)
	}

	for k, _ := range inuseNodes {
		allip = append(allip, k)
	}

	if len(allip) < int(vol.Copies) {
		logger.Error("Expand Volume:%v Tier:%v but DataNode nums:%v less than Copies:%v, so forbid CreateVol", vol.UUID, vol.Tier, len(allip), vol.Copies)
		ack.Ret = -1
		return &ack, nil
	}

	dataNodesUsedMap := make(map[string][]uint64)
	var blockGroups []*mp.BlockGroup
	for i := int32(0); i < blkgrpnum; i++ {
		bgID, err := s.Cluster.RaftGroup.BGIDGET(1)
		if err != nil {
			logger.Error("AllocateBGID for CreateVol failed, err:%v", err)
			return &ack, err
		}

		idxs := utils.GenerateRandomNumber(0, len(allip), int(vol.Copies))
		if len(idxs) != int(vol.Copies) {
			ack.Ret = -1
			return &ack, nil
		}

		var hosts []string
		bg := &vp.BlockGroup{BlockGroupID: bgID, FreeSize: utils.BlockGroupSize}
		for n := 0; n < int(vol.Copies); n++ {
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
			logger.Error("Expand Volume:%v Set blockgroup:%v blocks:%v failed:%v", vol.UUID, bgID, bg, err)
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
		logger.Debug("Expand Volume:%v Tier:%v Set one blockgroup:%v blocks:%v to Cluster Map success", vol.UUID, vol.Tier, bgID, bg)
		vol.BlockGroups = append(vol.BlockGroups, bgID)
		blockGroup := &mp.BlockGroup{}
		blockGroup.BlockGroupID = bgID
		blockGroup.FreeSize = utils.BlockGroupSize
		blockGroups = append(blockGroups, blockGroup)
	}

	if err = s.Cluster.RaftGroup.VolumeSet(1, vol.UUID, vol); err != nil {
		ack.Ret = -1
		for host, _ := range dataNodesUsedMap {
			s.Cluster.RaftGroup.DataNodeBGPDelBGP(host, dataNodesUsedMap[host])
		}
	}

	metaNodeRG, err := s.Cluster.RaftGroup.MetaNodeRGGet(vol.RGID)
	if err != nil {
		logger.Error("MetaNodeRGGet failed: %v", err)
		return &ack, err
	}
	metaNodes, err := s.getMetaNodesViaIds(metaNodeRG.MetaNodes)
	if err != nil {
		logger.Error("MetaNodes Get failed: %v", err)
		return &ack, err
	}
	var mHosts []string
	for _, m := range metaNodes {
		mHosts = append(mHosts, m.Host+":9901")
	}
	mLeader, err := utils.GetMetaNodeLeader(mHosts, in.UUID)
	if err != nil {
		logger.Error("GetMetaNodeLeader[ hosts: %v] failed: %v", mHosts, err)
		return &ack, err
	}
	mConn, err := utils.Dial(mLeader)
	if err != nil {
		logger.Error("Dial metanode leader[%v] failed: %v", mLeader, err)
		return &ack, err
	}
	defer mConn.Close()
	mc := mp.NewMetaNodeClient(mConn)
	pExpandNameSpaceReq := mp.ExpandNameSpaceReq{
		VolID:       in.UUID,
		BlockGroups: blockGroups,
	}
	ctx, _ = context.WithTimeout(context.Background(), 5*time.Second)
	pExpandNameSpaceAck, err := mc.ExpandNameSpace(ctx, &pExpandNameSpaceReq)
	if err != nil {
		logger.Error("ExpandNameSpace failed: %v", err)
		return &ack, err
	}
	if pExpandNameSpaceAck.Ret != 0 {
		if err != nil {
			logger.Error("ExpandNameSpace failed: %v", pExpandNameSpaceAck.Ret)
			return &ack, err
		}
	}
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
		if flag && vv.Status == 0 && vv.Tier == tier && vv.Free > utils.BlkSizeG {
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
	dataNode.Free = dataNode.Free - utils.BlkSizeG

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

func (s *VolMgrServer) updateBlockGroupStatus() {
	var blockGroup *vp.BlockGroup
	var err error
	for bgID, status := range s.BgStatusMap {
		if status > 0 {
			s.BgStatusMap[bgID] = 0
			status = 1
		}
		blockGroup, err = s.Cluster.RaftGroup.BlockGroupGet(bgID)
		if err != nil {
			logger.Error("updateBlockGroup BlockGroupGet failed: %v", err)
			continue
		}
		if blockGroup.Status != status {
			blockGroup.Status = status
			s.Cluster.RaftGroup.BlockGroupSet(bgID, blockGroup)
		}
	}
}

func (s *VolMgrServer) SnapShotCluster(ctx context.Context, in *vp.SnapShotClusterReq) (*vp.SnapShotClusterAck, error) {
	go raftopt.TakeClusterKvSnapShot(s.Cluster.RaftGroup, s.Cluster.RaftStorage, path.Join(va.Waldir, "Cluster", "wal", "snap"))
	return &vp.SnapShotClusterAck{Ret: 0}, nil
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
