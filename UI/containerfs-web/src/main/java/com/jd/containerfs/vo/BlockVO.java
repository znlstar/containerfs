package com.jd.containerfs.vo;

import java.io.Serializable;

/**
 * Created by lixiaoping3 on 17-11-20.
 */
public class BlockVO implements Serializable{

    private static final long serialVersionUID = 8404864403141888699L;

    private Long blkId;
    private String ip;
    private Integer port;
    private String path;
    private Integer status;
    private Long bgid;
    private String volId;

    public Long getBlkId() {
        return blkId;
    }

    public void setBlkId(Long blkId) {
        this.blkId = blkId;
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

    public String getPath() {
        return path;
    }

    public void setPath(String path) {
        this.path = path;
    }

    public Integer getStatus() {
        return status;
    }

    public void setStatus(Integer status) {
        this.status = status;
    }

    public Long getBgid() {
        return bgid;
    }

    public void setBgid(Long bgid) {
        this.bgid = bgid;
    }

    public String getVolId() {
        return volId;
    }

    public void setVolId(String volId) {
        this.volId = volId;
    }
}
