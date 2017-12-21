package com.jd.containerfs.dto.node;

import java.io.Serializable;

/**
 * Created by lixiaoping3 on 17-11-17.
 */
public class VolumePageParamDto implements Serializable{

    private static final long serialVersionUID = -2192288515753864096L;

    private String uuid;

    public String getUuid() {
        return uuid;
    }

    public void setUuid(String uuid) {
        this.uuid = uuid;
    }
}
