package com.jd.containerfs.common;

/**
 * Created by lixiaoping3 on 17-11-22.
 * 元数据节点状态
 */
public enum MetaNodeStatusEnum {

    META_NODE_NORMAL("正常"),META_NODE_ABNORMAL("异常");
    private String statusDesc;

    MetaNodeStatusEnum(String statusDesc){
        this.statusDesc=statusDesc;
    }

    public String getStatusDesc() {
        return statusDesc;
    }

    public void setStatusDesc(String statusDesc) {
        this.statusDesc = statusDesc;
    }
}
