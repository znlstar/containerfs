package com.jd.containerfs.dto.node;


import com.jd.containerfs.common.BaseResult;

/**
 * Created by lixiaoping3 on 17-11-22.
 */
public class MetaLeaderResultDto extends BaseResult {
    private MetaLeaderDto metaLeader;

    public MetaLeaderDto getMetaLeader() {
        return metaLeader;
    }

    public void setMetaLeader(MetaLeaderDto metaLeader) {
        this.metaLeader = metaLeader;
    }
}
