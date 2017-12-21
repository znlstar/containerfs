package com.jd.containerfs.dto.node;


import com.jd.containerfs.common.BaseResult;

/**
 * Created by lixiaoping3 on 17-11-20.
 */
public class ClusterInfoResultDto extends BaseResult {

    private ClusterInfoDto clusterInfo;

    public ClusterInfoDto getClusterInfo() {
        return clusterInfo;
    }

    public void setClusterInfo(ClusterInfoDto clusterInfo) {
        this.clusterInfo = clusterInfo;
    }

}
