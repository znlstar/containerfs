package com.jd.containerfs.vo;

import java.io.Serializable;

/**
 * Created by lixiaoping3 on 17-11-17.
 */
public class MetaNodeVO implements Serializable {

    private static final long serialVersionUID = 4386864594709330454L;
    private Long metaID;
    private Boolean isLeader;
    private Long appliedIndex;

    public Long getMetaID() {
        return metaID;
    }

    public void setMetaID(Long metaID) {
        this.metaID = metaID;
    }

    public Boolean getIsLeader() {
        return isLeader;
    }

    public void setIsLeader(Boolean isLeader) {
        this.isLeader = isLeader;
    }

    public Long getAppliedIndex() {
        return appliedIndex;
    }

    public void setAppliedIndex(Long appliedIndex) {
        this.appliedIndex = appliedIndex;
    }
}
