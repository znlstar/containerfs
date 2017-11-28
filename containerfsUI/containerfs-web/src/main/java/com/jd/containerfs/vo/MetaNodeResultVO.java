package com.jd.containerfs.vo;

import com.jd.containerfs.common.BaseResult;

/**
 * Created by lixiaoping3 on 17-11-22.
 */
public class MetaNodeResultVO extends BaseResult {

    private MetaNodeVO metaNode;

    public MetaNodeVO getMetaNode() {
        return metaNode;
    }

    public void setMetaNode(MetaNodeVO metaNode) {
        this.metaNode = metaNode;
    }

}
