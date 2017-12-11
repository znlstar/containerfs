package com.jd.containerfs.vo;

import java.io.Serializable;
import java.text.DecimalFormat;
import java.util.List;

/**
 * Created by wuzhengxuan on 2017/11/21.
 */
public class NodeInfoVO implements Serializable {

    private String date;
    private Double cpuUsage;
    private Double cpuLoad;
    private Long freeMem;
    private String usedMem;
    private Long totalMem;
    private Double memUsedPercent;
    private Long pathTotal;
    private Long pathFree;
    private Double pathUsedPercent;
    private List<DiskIoVO> diskIosList;
    private List<NetIoVO> netIosList;
    private DecimalFormat df = new DecimalFormat("0.00");

    public String getDate() {
        return date;
    }

    public void setDate(String date) {
        this.date = date;
    }

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

    public String getUsedMem() {
        return usedMem;
    }

    public void setUsedMem(String usedMem) {
        this.usedMem = usedMem;
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

    public String getCpuUsageStr(){
        return cpuUsage==null?"":df.format(cpuUsage);
    }

    public String getUsedMemStr(){
        return  String.valueOf((totalMem-freeMem)/1024/1024);
    }

    public String getFreeMemStr(){
        return freeMem==null?"":String.valueOf(freeMem/1024/1024/1024);
    }
    public String getTotalMemStr(){
        return totalMem==null?"":String.valueOf(totalMem/1024/1024);
    }

    public String getPathFreeStr(){
        return pathFree==null?"":String.valueOf(pathFree/1024/1024/1024);
    }

    public String getPathTotalStr(){
        return pathTotal==null?"":String.valueOf(pathTotal/1024/1024/1024);
    }

    public String getPathUsedPercentStr(){
        return pathUsedPercent==null?"":df.format(pathUsedPercent);
    }

    public String getMemUsedPercentStr(){
        return memUsedPercent==null?"":df.format(memUsedPercent);
    }

    public String getCpuLoadStr(){
        return cpuLoad==null?"":df.format(cpuLoad);
    }

    public List<DiskIoVO> getDiskIosList() {
        return diskIosList;
    }

    public void setDiskIosList(List<DiskIoVO> diskIosList) {
        this.diskIosList = diskIosList;
    }

    public List<NetIoVO> getNetIosList() {
        return netIosList;
    }

    public void setNetIosList(List<NetIoVO> netIosList) {
        this.netIosList = netIosList;
    }
}
