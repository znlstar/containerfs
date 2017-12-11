package com.jd.containerfs.manager;

import com.jd.containerfs.common.Constants;
import com.jd.containerfs.common.util.DozerMapper;
import com.jd.containerfs.common.util.HttpUtils;
import com.jd.containerfs.common.util.JsonUtils;
import com.jd.containerfs.dto.node.NodeInfoDto;
import com.jd.containerfs.dto.node.NodeInfoResultDto;
import com.jd.containerfs.vo.NodeInfoVO;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.Map;

/**
 * Created by lixiaoping3 on 17-11-21.
 */
@Service
public class DataNodeManager {
    @Resource
    private String restServiceUrl;

    private static final String DATA_NODE_MONITOR="/datanode/monitor";

    /**
     * 数据节点监控
     * @param ip
     * @param port
     * @return
     */
    public NodeInfoVO dataNodeMonitor(String ip,Integer port){
        Map<String,Object> map= HttpUtils.request(restServiceUrl, DATA_NODE_MONITOR+"/"+ip+"/"+port, Constants.HTTP_REQUEST_GET, "", null);
        String body=(String)map.get("content");
        NodeInfoResultDto resultDto= JsonUtils.toJavaObject(body, NodeInfoResultDto.class);
        if(!resultDto.isSuccess()){
            throw new RuntimeException(resultDto.getResultMessage());
        }
        NodeInfoDto dto=resultDto.getNodeInfo();
        if(dto==null){
            return null;
        }
        NodeInfoVO vo= DozerMapper.map(dto,NodeInfoVO.class);
        return vo;
    }

}
