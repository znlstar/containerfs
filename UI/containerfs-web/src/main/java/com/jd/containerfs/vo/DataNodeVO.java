package com.jd.containerfs.vo;

import com.jd.containerfs.common.DataNodeStatusEnum;

import java.io.Serializable;

/**
 * Created by lixiaoping3 on 17-11-23.
 */
public class DataNodeVO implements Serializable {
    private static final long serialVersionUID = 5541334353833734175L;
    private String ip;
    private Integer port;
    private String mountPoint;
    private Integer capacity;
    private Integer used;
    private Integer free;
    private Integer status;
    private String tier;

    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    public Integer getPort() {
        return port;
    }

    public void setPort(Integer port) {
        this.port = port;
    }

    public String getMountPoint() {
        return mountPoint;
    }

    public void setMountPoint(String mountPoint) {
        this.mountPoint = mountPoint;
    }

    public Integer getCapacity() {
        return capacity;
    }

    public void setCapacity(Integer capacity) {
        this.capacity = capacity;
    }

    public Integer getUsed() {
        return used;
    }

    public void setUsed(Integer used) {
        this.used = used;
    }

    public Integer getFree() {
        return free;
    }

    public void setFree(Integer free) {
        this.free = free;
    }

    public Integer getStatus() {
        return status;
    }

    public void setStatus(Integer status) {
        this.status = status;
    }

    public String getTier() {
        return tier;
    }

    public void setTier(String tier) {
        this.tier = tier;
    }

    public String getStatusDesc(){
        return DataNodeStatusEnum.getStatusDesc(status);
    }
}
