package com.jd.containerfs.vo;

import java.io.Serializable;
import java.text.DecimalFormat;

/**
 * Created by lixiaoping3 on 17-11-22.
 */
public class NetIoVO implements Serializable {

    private static final long serialVersionUID = -7713106066757295548L;
    private String name;
    private Long bytesSent;
    private Long bytesRecv;
    private Long packetsSent;
    private Long packetRecv;
    private Long errIn;
    private Long errOut;
    private Long dropIn;
    private Long dropOut;
    private DecimalFormat df = new DecimalFormat("0.00");

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Long getBytesSent() {
        return bytesSent;
    }

    public String getBytesSentStr() {
        if(bytesSent==0){
            return String.valueOf(bytesSent);
        }else{
            return  df.format(bytesSent/1024.0/1024.0);
        }
    }

    public void setBytesSent(Long bytesSent) {
        this.bytesSent = bytesSent;
    }

    public Long getBytesRecv() {
        return bytesRecv;
    }

    public String getBytesRecvStr() {
        if(bytesRecv==0){
            return String.valueOf(bytesRecv);
        }else{
            return  df.format(bytesRecv/1024.0/1024.0);
        }
    }

    public void setBytesRecv(Long bytesRecv) {
        this.bytesRecv = bytesRecv;
    }

    public Long getPacketsSent() {
        return packetsSent==null?0:packetsSent;
    }

    public void setPacketsSent(Long packetsSent) {
        this.packetsSent = packetsSent;
    }

    public Long getPacketRecv() {
        return packetRecv==null?0:packetRecv;
    }

    public void setPacketRecv(Long packetRecv) {
        this.packetRecv = packetRecv;
    }

    public Long getErrIn() {
        return errIn;
    }

    public void setErrIn(Long errIn) {
        this.errIn = errIn;
    }

    public Long getErrOut() {
        return errOut;
    }

    public void setErrOut(Long errOut) {
        this.errOut = errOut;
    }

    public Long getDropIn() {
        return dropIn;
    }

    public void setDropIn(Long dropIn) {
        this.dropIn = dropIn;
    }

    public Long getDropOut() {
        return dropOut;
    }

    public void setDropOut(Long dropOut) {
        this.dropOut = dropOut;
    }
}
