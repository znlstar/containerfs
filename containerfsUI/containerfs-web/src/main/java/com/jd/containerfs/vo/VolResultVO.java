package com.jd.containerfs.vo;

import com.jd.containerfs.common.BaseResult;

import java.util.List;

/**
 * Created by lixiaoping3 on 17-11-20.
 */
public class VolResultVO extends BaseResult{

    private static final long serialVersionUID = -780973248737413129L;
    private List<BgpVO> bgpsList;
    private Integer bgpsCount;

    public Integer getBgpsCount() {
        return bgpsCount;
    }

    public void setBgpsCount(Integer bgpsCount) {
        this.bgpsCount = bgpsCount;
    }

    public List<BgpVO> getBgpsList() {
        return bgpsList;
    }

    public void setBgpsList(List<BgpVO> bgpsList) {
        this.bgpsList = bgpsList;
    }
}
