package com.jd.containerfs.service;

import com.jd.containerfs.dto.node.*;

/**
 * Created by lixiaoping3 on 17-11-20.
 */
public interface MetaNodeService {
    /**
     * 获取集群信息
     * @return
     */
    public ClusterInfoResultDto getClusterInfo();
    /**
     * 查询元数据LEADER节点
     * @return
     */
    public MetaLeaderResultDto getMetaLeader();
    /**
     * 查询数据节点
     * @param  paramDto
     * @return
     */
    public DataNodePageResultDto getDataNodes(DataNodePageParamDto paramDto);
    /**
     * 查询元数据节点
     * @param metaNodeParam
     * @return
     */
    public MetaNodeResultDto getMetaNodeInfo(MetaNodeParamDto metaNodeParam);
    /**
     * 查询VOLUME列表
     * @param  paramDto
     * @return
     */
    public VolumePageResultDto getVolumes(VolumePageParamDto paramDto);
    /**
     * 查询VOLUME详细信息
     * @param volumeParam
     * @return
     */
    public VolResultDto getVolInfo(VolumeParamDto volumeParam);
    /**
     * 监控数据节点
     * @param nodeInfoParam
     * @return
     */
    public NodeInfoResultDto metaNodeMonitor(NodeInfoParamDto nodeInfoParam);

    /**
     * 查询元数据节点
     * @param pageParamDto
     * @return
     */
    public MetaNodePageResultDto getMetaNodes(MetaNodePageParamDto pageParamDto);
}
