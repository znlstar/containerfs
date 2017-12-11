package com.jd.containerfs.vo;

import java.io.Serializable;

/**
 * Created by lixiaoping3 on 17-11-21.
 */
public class MetaNodeInfoVO implements Serializable {
    private String name;
    private String ip;
    private Integer port;
    private String statusDesc;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

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

    public String getStatusDesc() {
        return statusDesc;
    }

    public void setStatusDesc(String statusDesc) {
        this.statusDesc = statusDesc;
    }
}
