package com.jd.containerfs.vo;

import java.io.Serializable;
import java.util.List;

/**
 * Created by lixiaoping3 on 17-11-20.
 */
public class BgpVO implements Serializable{

    private static final long serialVersionUID = -6478039230308552318L;
    private List<BlockVO> blocksList;
    private Integer blocksCount;

    public List<BlockVO> getBlocksList() {
        return blocksList;
    }

    public void setBlocksList(List<BlockVO> blocksList) {
        this.blocksList = blocksList;
    }

    public Integer getBlocksCount() {
        return blocksCount;
    }

    public void setBlocksCount(Integer blocksCount) {
        this.blocksCount = blocksCount;
    }
}
