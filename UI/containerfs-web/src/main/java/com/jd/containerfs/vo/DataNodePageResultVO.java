package com.jd.containerfs.vo;



import com.jd.containerfs.common.BaseResult;

import java.util.List;

/**
 * Created by lixiaoping3 on 17-11-17.
 */
public class DataNodePageResultVO extends BaseResult {

    private List<DataNodeVO> datanodesList;

    private Integer datanodesCount;

    public Integer getDatanodesCount() {
        return datanodesCount;
    }

    public void setDatanodesCount(Integer datanodesCount) {
        this.datanodesCount = datanodesCount;
    }

    public List<DataNodeVO> getDatanodesList() {
        return datanodesList;
    }

    public void setDatanodesList(List<DataNodeVO> datanodesList) {
        this.datanodesList = datanodesList;
    }
}
