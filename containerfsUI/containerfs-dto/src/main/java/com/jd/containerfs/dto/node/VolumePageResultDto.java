package com.jd.containerfs.dto.node;




import com.jd.containerfs.common.BaseResult;

import java.util.List;

/**
 * Created by lixiaoping3 on 17-11-17.
 */
public class VolumePageResultDto extends BaseResult {

    private Integer volumesCount;

    private List<VolumeDto> volumesList;

    public Integer getVolumesCount() {
        return volumesCount;
    }

    public void setVolumesCount(Integer volumesCount) {
        this.volumesCount = volumesCount;
    }

    public List<VolumeDto> getVolumesList() {
        return volumesList;
    }

    public void setVolumesList(List<VolumeDto> volumesList) {
        this.volumesList = volumesList;
    }
}
