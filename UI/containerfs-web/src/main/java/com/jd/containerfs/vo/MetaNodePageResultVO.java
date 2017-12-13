package com.jd.containerfs.vo;


import com.jd.containerfs.common.BaseResult;

import java.util.List;

/**
 * Created by lixiaoping3 on 17-11-21.
 */
public class MetaNodePageResultVO extends BaseResult {

    private List<MetaNodeInfoVO> list;

    private Integer count;

    public List<MetaNodeInfoVO> getList() {
        return list;
    }

    public void setList(List<MetaNodeInfoVO> list) {
        this.list = list;
    }

    public Integer getCount() {
        return count;
    }

    public void setCount(Integer count) {
        this.count = count;
    }
}
