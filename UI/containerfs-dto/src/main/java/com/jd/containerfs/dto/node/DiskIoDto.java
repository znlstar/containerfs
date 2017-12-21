package com.jd.containerfs.dto.node;

import java.io.Serializable;

/**
 * Created by lixiaoping3 on 17-11-17.
 */
public class DiskIoDto implements Serializable{
    private static final long serialVersionUID = 5039711038942643551L;

    private String name;
    private Long readCount;
    private Long writeCount;
    private Long readBytes;
    private Long writeBytes;
    private Long iopsInProgress;
    private Long ioTime;
    private Long weightedIo;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Long getReadCount() {
        return readCount;
    }

    public void setReadCount(Long readCount) {
        this.readCount = readCount;
    }

    public Long getWriteCount() {
        return writeCount;
    }

    public void setWriteCount(Long writeCount) {
        this.writeCount = writeCount;
    }

    public Long getReadBytes() {
        return readBytes;
    }

    public void setReadBytes(Long readBytes) {
        this.readBytes = readBytes;
    }

    public Long getWriteBytes() {
        return writeBytes;
    }

    public void setWriteBytes(Long writeBytes) {
        this.writeBytes = writeBytes;
    }

    public Long getIopsInProgress() {
        return iopsInProgress;
    }

    public void setIopsInProgress(Long iopsInProgress) {
        this.iopsInProgress = iopsInProgress;
    }

    public Long getIoTime() {
        return ioTime;
    }

    public void setIoTime(Long ioTime) {
        this.ioTime = ioTime;
    }

    public Long getWeightedIo() {
        return weightedIo;
    }

    public void setWeightedIo(Long weightedIo) {
        this.weightedIo = weightedIo;
    }
}
