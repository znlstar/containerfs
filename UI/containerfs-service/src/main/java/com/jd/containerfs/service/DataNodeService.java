package com.jd.containerfs.service;

import com.jd.containerfs.dto.node.NodeInfoDto;
import com.jd.containerfs.dto.node.NodeInfoParamDto;
import com.jd.containerfs.dto.node.NodeInfoResultDto;

/**
 * Created by lixiaoping3 on 17-11-20.
 */
public interface DataNodeService {
    /**
     * 查询数据节点监控信息
     * @param nodeInfoParam
     * @return
     */
    public NodeInfoResultDto dataNodeMonitor(NodeInfoParamDto nodeInfoParam);
}
