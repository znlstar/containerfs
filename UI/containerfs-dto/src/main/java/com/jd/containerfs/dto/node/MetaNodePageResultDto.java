package com.jd.containerfs.dto.node;


import com.jd.containerfs.common.BaseResult;

import java.util.List;

/**
 * Created by lixiaoping3 on 17-11-21.
 */
public class MetaNodePageResultDto extends BaseResult {

    private List<MetaNodeInfoDto> list;

    private Integer count;

    public List<MetaNodeInfoDto> getList() {
        return list;
    }

    public void setList(List<MetaNodeInfoDto> list) {
        this.list = list;
    }

    public Integer getCount() {
        return count;
    }

    public void setCount(Integer count) {
        this.count = count;
    }
}
