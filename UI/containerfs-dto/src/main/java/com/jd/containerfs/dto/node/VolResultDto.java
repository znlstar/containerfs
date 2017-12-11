package com.jd.containerfs.dto.node;

import com.jd.containerfs.common.BaseResult;

import java.io.Serializable;
import java.util.List;

/**
 * Created by lixiaoping3 on 17-11-20.
 */
public class VolResultDto extends BaseResult{

    private static final long serialVersionUID = -780973248737413129L;
    private List<BgpDto> bgpsList;
    private Integer bgpsCount;

    public Integer getBgpsCount() {
        return bgpsCount;
    }

    public void setBgpsCount(Integer bgpsCount) {
        this.bgpsCount = bgpsCount;
    }

    public List<BgpDto> getBgpsList() {
        return bgpsList;
    }

    public void setBgpsList(List<BgpDto> bgpsList) {
        this.bgpsList = bgpsList;
    }
}
