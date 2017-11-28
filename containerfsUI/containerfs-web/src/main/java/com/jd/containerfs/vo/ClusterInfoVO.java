package com.jd.containerfs.vo;

import java.io.Serializable;

/**
 * Created by lixiaoping3 on 17-11-17.
 */
public class ClusterInfoVO implements Serializable{

    private static final long serialVersionUID = -1594859140373264439L;
    private Integer metaNum;
    private Integer dataNum;
    private Integer volNum;
    private Integer clusterSpace;
    private Integer clusterFreeSpace;
    private Integer IO;
    private Integer IOPS;

    public Integer getMetaNum() {
        return metaNum;
    }

    public void setMetaNum(Integer metaNum) {
        this.metaNum = metaNum;
    }

    public Integer getDataNum() {
        return dataNum;
    }

    public void setDataNum(Integer dataNum) {
        this.dataNum = dataNum;
    }

    public Integer getVolNum() {
        return volNum;
    }

    public void setVolNum(Integer volNum) {
        this.volNum = volNum;
    }

    public Integer getClusterSpace() {
        return clusterSpace;
    }

    public void setClusterSpace(Integer clusterSpace) {
        this.clusterSpace = clusterSpace;
    }

    public Integer getClusterFreeSpace() {
        return clusterFreeSpace;
    }

    public void setClusterFreeSpace(Integer clusterFreeSpace) {
        this.clusterFreeSpace = clusterFreeSpace;
    }

    public Integer getIO() {
        return IO;
    }

    public void setIO(Integer IO) {
        this.IO = IO;
    }

    public Integer getIOPS() {
        return IOPS;
    }

    public void setIOPS(Integer IOPS) {
        this.IOPS = IOPS;
    }
}
