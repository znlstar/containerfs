package com.jd.containerfs.dto.node;



import com.jd.containerfs.common.BaseResult;

import java.util.List;

/**
 * Created by lixiaoping3 on 17-11-17.
 */
public class DataNodePageResultDto extends BaseResult {

    private List<DataNodeDto> datanodesList;

    private Integer datanodesCount;

    public Integer getDatanodesCount() {
        return datanodesCount;
    }

    public void setDatanodesCount(Integer datanodesCount) {
        this.datanodesCount = datanodesCount;
    }

    public List<DataNodeDto> getDatanodesList() {
        return datanodesList;
    }

    public void setDatanodesList(List<DataNodeDto> datanodesList) {
        this.datanodesList = datanodesList;
    }
}
