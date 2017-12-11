package com.jd.containerfs.dto.node;

import java.io.Serializable;
import java.util.List;

/**
 * Created by lixiaoping3 on 17-11-17.
 */
public class NodeInfoDto implements Serializable{

    private static final long serialVersionUID = -5993450516063060790L;

    private Double cpuUsage;
    private Double cpuLoad;
    private Long freeMem;
    private Long totalMem;
    private Double memUsedPercent;
    private Long pathTotal;
    private Long pathFree;
    private Double pathUsedPercent;
    private List<DiskIoDto> diskIosList;
    private List<NetIoDto> netIosList;

    public Double getCpuUsage() {
        return cpuUsage;
    }

    public void setCpuUsage(Double cpuUsage) {
        this.cpuUsage = cpuUsage;
    }

    public Double getCpuLoad() {
        return cpuLoad;
    }

    public void setCpuLoad(Double cpuLoad) {
        this.cpuLoad = cpuLoad;
    }

    public Long getFreeMem() {
        return freeMem;
    }

    public void setFreeMem(Long freeMem) {
        this.freeMem = freeMem;
    }

    public Long getTotalMem() {
        return totalMem;
    }

    public void setTotalMem(Long totalMem) {
        this.totalMem = totalMem;
    }

    public Double getMemUsedPercent() {
        return memUsedPercent;
    }

    public void setMemUsedPercent(Double memUsedPercent) {
        this.memUsedPercent = memUsedPercent;
    }

    public Long getPathTotal() {
        return pathTotal;
    }

    public void setPathTotal(Long pathTotal) {
        this.pathTotal = pathTotal;
    }

    public Long getPathFree() {
        return pathFree;
    }

    public void setPathFree(Long pathFree) {
        this.pathFree = pathFree;
    }

    public Double getPathUsedPercent() {
        return pathUsedPercent;
    }

    public void setPathUsedPercent(Double pathUsedPercent) {
        this.pathUsedPercent = pathUsedPercent;
    }

    public List<DiskIoDto> getDiskIosList() {
        return diskIosList;
    }

    public void setDiskIosList(List<DiskIoDto> diskIosList) {
        this.diskIosList = diskIosList;
    }

    public List<NetIoDto> getNetIosList() {
        return netIosList;
    }

    public void setNetIosList(List<NetIoDto> netIosList) {
        this.netIosList = netIosList;
    }
}
