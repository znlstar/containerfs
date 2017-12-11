package com.jd.containerfs.dto.node;

import java.io.Serializable;

/**
 * Created by lixiaoping3 on 17-11-17.
 */
public class ClusterInfoParamDto implements Serializable {
    private static final long serialVersionUID = 6919594462032353913L;
    private String ip;
    private int port;

    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }
}
