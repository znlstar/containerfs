package com.jd.containerfs.service.impl;

import com.jd.containerfs.dto.node.NodeInfoDto;
import com.jd.containerfs.dto.node.NodeInfoParamDto;
import com.jd.containerfs.dto.node.NodeInfoResultDto;
import com.jd.containerfs.rpc.DataNodeClient;
import com.jd.containerfs.service.DataNodeService;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;

/**
 * Created by lixiaoping3 on 17-11-20.
 */
@Service
public class DataNodeServiceImpl implements DataNodeService {

    @Override
    public NodeInfoResultDto dataNodeMonitor(NodeInfoParamDto nodeInfoParam) {
        DataNodeClient client=DataNodeClient.buildDataNodeClient(nodeInfoParam.getIp(),nodeInfoParam.getPort());
        NodeInfoResultDto result= client.dataNodeMonitor(nodeInfoParam);
        return result;
    }
}
