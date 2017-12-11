package com.jd.containerfs.vo;


import com.jd.containerfs.common.BaseResult;

/**
 * Created by lixiaoping3 on 17-11-22.
 */
public class MetaLeaderResultVO extends BaseResult {
    private MetaLeaderVO metaLeader;

    public MetaLeaderVO getMetaLeader() {
        return metaLeader;
    }

    public void setMetaLeader(MetaLeaderVO metaLeader) {
        this.metaLeader = metaLeader;
    }
}
