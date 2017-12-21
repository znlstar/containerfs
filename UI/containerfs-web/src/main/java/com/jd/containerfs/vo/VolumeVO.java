package com.jd.containerfs.vo;

import java.io.Serializable;

/**
 * Created by lixiaoping3 on 17-11-17.
 */
public class VolumeVO implements Serializable{

    private static final long serialVersionUID = -7771488927158531922L;
    private String uuid;
    private String name;
    private String tier;
    private Integer totalSize;
    private Integer allocatedSize;
    private Long rgid;

    public String getUuid() {
        return uuid;
    }

    public void setUuid(String uuid) {
        this.uuid = uuid;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getTier() {
        return tier;
    }

    public void setTier(String tier) {
        this.tier = tier;
    }

    public Integer getTotalSize() {
        return totalSize;
    }

    public void setTotalSize(Integer totalSize) {
        this.totalSize = totalSize;
    }

    public Integer getAllocatedSize() {
        return allocatedSize;
    }

    public void setAllocatedSize(Integer allocatedSize) {
        this.allocatedSize = allocatedSize;
    }

    public Long getRgid() {
        return rgid;
    }

    public void setRgid(Long rgid) {
        this.rgid = rgid;
    }
}
