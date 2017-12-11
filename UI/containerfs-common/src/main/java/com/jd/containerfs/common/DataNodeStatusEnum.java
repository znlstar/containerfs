package com.jd.containerfs.common;

/**
 * Created by lixiaoping3 on 17-11-22.
 * 数据节点状态，正常为0，其他值均为异常
 */
public enum  DataNodeStatusEnum {
    STATUS_NORMAL(0,"正常");
    private Integer status;
    private String description;
    private static final String STATUS_ABNORMAL_DESC="异常";
    DataNodeStatusEnum(Integer status,String description){
        this.status=status;
        this.description=description;
    }

    public Integer getStatus() {
        return status;
    }

    public void setStatus(Integer status) {
        this.status = status;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }
    /**
     * 查询状态描述
     * @param status
     * @return
     */
    public static String getStatusDesc(Integer status){
        if(DataNodeStatusEnum.STATUS_NORMAL.getStatus().equals(status)){
            //0为正常，其它均为异常
            return  DataNodeStatusEnum.STATUS_NORMAL.getDescription();
        }
        //异常
        return DataNodeStatusEnum.STATUS_ABNORMAL_DESC;
    }
}
