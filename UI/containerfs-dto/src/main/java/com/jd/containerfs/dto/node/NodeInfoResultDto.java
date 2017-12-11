package com.jd.containerfs.dto.node;


import com.jd.containerfs.common.BaseResult;

/**
 * Created by lixiaoping3 on 17-11-22.
 */
public class NodeInfoResultDto extends BaseResult {

    private NodeInfoDto nodeInfo;

    public NodeInfoDto getNodeInfo() {
        return nodeInfo;
    }

    public void setNodeInfo(NodeInfoDto nodeInfo) {
        this.nodeInfo = nodeInfo;
    }


}
