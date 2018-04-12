package volmgr

import (
	"errors"

	"github.com/tiglabs/containerfs/logger"
	"github.com/tiglabs/containerfs/proto/dp"
	"github.com/tiglabs/containerfs/proto/mp"
	"github.com/tiglabs/containerfs/proto/vp"
	"github.com/tiglabs/containerfs/raftopt"
	"github.com/tiglabs/containerfs/utils"
	"golang.org/x/net/context"
)

//DataNodeRegistry deals with datanode's register request
func (s *VolMgrServer) DataNodeRegistry(ctx context.Context, in *vp.DataNode) (*vp.DataNodeRegistryAck, error) {
	ack := vp.DataNodeRegistryAck{}

	dataNode, err := s.Cluster.RaftGroup.DataNodeGet(1, in.Host)
	if err != nil && err != raftopt.ErrKeyNotFound {
		return &ack, err
	}
	if dataNode != nil {
		if dataNode.Status != in.Status {
			dataNode.Status = in.Status
			s.Cluster.RaftGroup.DataNodeSet(1, in.Host, dataNode)
			ack.Ret = 3
			return &ack, nil
		}
	}
	logger.Debug("datanode registry:%v", in)
	err = s.Cluster.RaftGroup.DataNodeSet(1, in.Host, in)
	if err != nil {
		logger.Error("DataNode(%v) Register to MetaNode failed:%v", in.Host, err)
		return &ack, err
	}

	logger.Debug("DataNode(%v) Register to MetaNode success", in.Host)
	return &ack, nil
}

//DetectDataNodes checks health state of all datanodes
func (s *VolMgrServer) DetectDataNodes() {

	vv, err := s.Cluster.RaftGroup.DataNodeGetAll(1)
	if err != nil {
		logger.Error("GetAllDataNode Info failed:%v for detectDataNodes", err)
		return
	}

	for _, v := range vv {
		go func() {
			s.wg.Add(1)
			defer s.wg.Done()
			s.detectDataNode(v)
		}()
	}
	s.wg.Wait()
	s.updateBlockGroupStatus()
}

//detectDataNode is an utility method to detect a single datanode
func (s *VolMgrServer) detectDataNode(v *vp.DataNode) {
	dnAddr := v.Host
	conn, err := utils.Dial(dnAddr)
	if err != nil {
		if v.Status == 0 {
			logger.Error("Detect DataNode:%v failed : Dial to DataNode failed !", dnAddr)
			v.Status = 1
			s.setDataNodeMap(v)
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
			s.setDataNodeMap(v)
			logger.Debug("Detect DataNode(%v) status from good to bad, set DataNode map success", dnAddr)
		}
		return
	}

	if v.Status != pDataNodeHealthCheckAck.Status {
		v.Status = pDataNodeHealthCheckAck.Status
		dataNodeBGs, err := s.Cluster.RaftGroup.DataNodeBGGet(v.Host)
		if err != nil && raftopt.ErrKeyNotFound != err {
			return
		}
		if dataNodeBGs != nil {
			for _, bgID := range dataNodeBGs.BGS {
				s.BgStatusMapSync.Lock()
				s.BgStatusMap[bgID] += v.Status
				s.BgStatusMapSync.Unlock()
			}
		}
		s.setDataNodeMap(v)
	}
	return
}

//GetDataNode lists all datanodes with detailed description
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

//DelDataNode removes specific datanode from cluster
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

//setDataNodeMap is an utility to update map from datanode to blockgroup
func (s *VolMgrServer) setDataNodeMap(v *vp.DataNode) int {
	key := v.Host
	err := s.Cluster.RaftGroup.DataNodeSet(1, key, v)
	if err != nil {
		logger.Error("Datanode  failed:%v", err)
		return -1
	}
	dataNodeBG, err := s.Cluster.RaftGroup.DataNodeBGGet(v.Host)
	if err == raftopt.ErrKeyNotFound {
		return 0
	}
	if err != nil {
		logger.Error("setDataNodeMap [%v %v] failed: %v", v.Host, v.Status, err)
		return -2
	}
	for _, bgId := range dataNodeBG.BGS {
		blockGroup, err := s.Cluster.RaftGroup.BlockGroupGet(bgId)
		if err != nil {
			logger.Error("setDataNodeMap failed: %v", err)
			return -3
		}
		blockGroup.Status = v.Status
		if err = s.Cluster.RaftGroup.BlockGroupSet(bgId, blockGroup); err != nil {
			logger.Error("setDataNodeMap failed: %v", err)
			return -4
		}
	}
	return 0
}

//delDataNode is not implemented yet
func (s *VolMgrServer) delDataNode(host string) error {
	return nil
}

//MetaNodeRegistry deals with metandoe's register request
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

//DetectMetaNodes checks health state of all datanodes
func (s *VolMgrServer) DetectMetaNodes() {

	vv, err := s.Cluster.RaftGroup.MetaNodeGetAll(1)
	if err != nil {
		logger.Error("GetAllDataNode Info failed:%v for detectDataNodes", err)
		return
	}

	for _, v := range vv {
		go s.detectMetaNode(v)
	}
}

//detectMetaNode is an utility method to detect a single datanode
func (s *VolMgrServer) detectMetaNode(v *vp.MetaNode) {
	conn, err := utils.Dial(v.Host + ":9901")
	if err != nil {
		if v.Status == 0 {
			v.Status = 1
			s.Cluster.RaftGroup.MetaNodeSet(1, v.Id, v)
		}
		logger.Error("Dial metanode host[%v] failed:%v", v.Host, err)
		return
	}

	defer conn.Close()
	mc := mp.NewMetaNodeClient(conn)
	pMetaNodeHealthCheckReq := mp.MetaNodeHealthCheckReq{}
	ack, err := mc.MetaNodeHealthCheck(context.Background(), &pMetaNodeHealthCheckReq)
	if err != nil {
		if v.Status == 0 {
			v.Status = 1
			s.Cluster.RaftGroup.MetaNodeSet(1, v.Id, v)
		}
		logger.Error("MetaNodeHealthCheck host[%v] failed:%v", v.Host, err)
		return
	}
	if ack.Ret != 0 {
		if v.Status == 0 {
			v.Status = 1
			s.Cluster.RaftGroup.MetaNodeSet(1, v.Id, v)
		}
		logger.Error("MetaNodeHealthCheck host[%v] failed:%v", v.Host, ack.Ret)
		return
	}
	if v.Status != ack.Status {
		v.Status = ack.Status
		s.Cluster.RaftGroup.MetaNodeSet(1, v.Id, v)
	}
	return
}

//delMetaNode is not implemented yet
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

//GetMetaNodeRG gets metanode raftgroup  for volume
func (s *VolMgrServer) GetMetaNodeRG(ctx context.Context, in *vp.GetMetaNodeRGReq) (*vp.GetMetaNodeRGAck, error) {
	ack := vp.GetMetaNodeRGAck{}
	vol, err := s.Cluster.RaftGroup.VolumeGet(1, in.UUID)
	if err != nil {
		logger.Error("get metanode raftgroup failed: %v", err)
		ack.Ret = -1
		return &ack, err
	}

	ack.Copies = vol.Copies

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

//GetMetaNodeRGPeers gets all metanode raftgroups in cluster
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

//getMetaNodesViaIds is an utility method to get metanode host by nodeid
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
