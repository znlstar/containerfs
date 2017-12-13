package com.jd.containerfs.vo;




import com.jd.containerfs.common.BaseResult;

import java.util.List;

/**
 * Created by lixiaoping3 on 17-11-17.
 */
public class VolumePageResultVO extends BaseResult {

    private Integer volumesCount;

    private List<VolumeVO> volumesList;

    public Integer getVolumesCount() {
        return volumesCount;
    }

    public void setVolumesCount(Integer volumesCount) {
        this.volumesCount = volumesCount;
    }

    public List<VolumeVO> getVolumesList() {
        return volumesList;
    }

    public void setVolumesList(List<VolumeVO> volumesList) {
        this.volumesList = volumesList;
    }
}
