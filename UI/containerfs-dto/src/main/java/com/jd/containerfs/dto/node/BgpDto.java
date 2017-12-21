package com.jd.containerfs.dto.node;

import java.io.Serializable;
import java.util.List;

/**
 * Created by lixiaoping3 on 17-11-20.
 */
public class BgpDto implements Serializable{

    private static final long serialVersionUID = -6478039230308552318L;
    private List<BlockDto> blocksList;
    private Integer blocksCount;

    public List<BlockDto> getBlocksList() {
        return blocksList;
    }

    public void setBlocksList(List<BlockDto> blocksList) {
        this.blocksList = blocksList;
    }

    public Integer getBlocksCount() {
        return blocksCount;
    }

    public void setBlocksCount(Integer blocksCount) {
        this.blocksCount = blocksCount;
    }
}
