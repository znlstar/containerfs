package com.jd.containerfs.manager;

import com.jd.containerfs.common.Constants;
import com.jd.containerfs.common.util.DozerMapper;
import com.jd.containerfs.common.util.HttpUtils;
import com.jd.containerfs.common.util.JsonUtils;
import com.jd.containerfs.dto.node.*;
import com.jd.containerfs.vo.*;
import org.apache.commons.lang.StringUtils;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.Map;

/**
 * Created by lixiaoping3 on 17-11-21.
 */
@Service
public class MetaNodeManager {

    @Resource
    private String restServiceUrl;//REST服务部署URL
    //元数据节点列表REST
    private static final String METANODE_LIST="/metanode/list";
    //数据节点列表REST
    private static final String DATANODE_LIST="/metanode/datanodes";
    //VOLUME列表REST
    private static final String VOLUME_LIST="/metanode/volumes";
    //集群信息REST
    private static final String CLUSTER_INFO="/metanode/cluster";
    //数据节点监控REST
    private static final String META_NODE_MONITOR="/metanode/monitor";

    /**
     * 集群信息
     * @return
     */
    public ClusterInfoVO getCluster(){
        Map<String,Object> map=HttpUtils.request(restServiceUrl,CLUSTER_INFO, Constants.HTTP_REQUEST_GET,"",null);
        String body=(String)map.get("content");
        if(StringUtils.isEmpty(body)){
            return null;
        }
        ClusterInfoResultDto resultDto=JsonUtils.toJavaObject(body,ClusterInfoResultDto.class);
        if(!resultDto.isSuccess()){
            throw new RuntimeException(resultDto.getResultMessage());
        }
        ClusterInfoDto dto=resultDto.getClusterInfo();
        ClusterInfoVO vo=null;
        if(dto!=null){
            vo=DozerMapper.map(dto,ClusterInfoVO.class);
        }
        return  vo;
    }

    /**
     * 元数据节点列表查询
     * @return
     */
    public MetaNodePageResultVO getMetaNodeList(){
        Map<String,Object> map=HttpUtils.request(restServiceUrl,METANODE_LIST, Constants.HTTP_REQUEST_GET,"",null);
        String body=(String)map.get("content");
        MetaNodePageResultVO resultVo=new MetaNodePageResultVO();
        if(StringUtils.isEmpty(body)){
            resultVo.setSuccess(false);
            resultVo.setResultMessage("接口返回空");
            return resultVo;
        }
        MetaNodePageResultDto resultDto=JsonUtils.toJavaObject(body,MetaNodePageResultDto.class);
        if(!resultDto.isSuccess()){
            throw new RuntimeException(resultDto.getResultMessage());
        }
        resultVo=DozerMapper.map(resultDto,MetaNodePageResultVO.class);
        return resultVo;
    }

    /**
     * VOLUME列表查询
     * @param param
     * @return
     */
    public VolumePageResultVO getVolumeList(VolumePageParamVO param){
        Map<String,Object> map=HttpUtils.request(restServiceUrl,VOLUME_LIST, Constants.HTTP_REQUEST_GET,JsonUtils.toJsonString(param),null);
        String body=(String)map.get("content");
        VolumePageResultVO resultVo=new VolumePageResultVO();
        if(StringUtils.isEmpty(body)){
            resultVo.setSuccess(false);
            resultVo.setResultMessage("接口返回空");
            return resultVo;
        }
        VolumePageResultDto resultDto=JsonUtils.toJavaObject(body,VolumePageResultDto.class);
        if(!resultDto.isSuccess()){
            throw new RuntimeException(resultDto.getResultMessage());
        }
        resultVo=DozerMapper.map(resultDto,VolumePageResultVO.class);
        return resultVo;
    }

    /**
     * 数据节点列表查询
     * @param param
     * @return
     */
    public DataNodePageResultVO getDataNodeList(DataNodePageParamVO param){

        Map<String,Object> map=HttpUtils.request(restServiceUrl,DATANODE_LIST, Constants.HTTP_REQUEST_GET,JsonUtils.toJsonString(param),null);
        String body=(String)map.get("content");
        DataNodePageResultVO resultVo=new DataNodePageResultVO();
        if(StringUtils.isEmpty(body)){
            resultVo.setSuccess(false);
            resultVo.setResultMessage("接口返回空");
            return resultVo;
        }
        DataNodePageResultDto resultDto=JsonUtils.toJavaObject(body,DataNodePageResultDto.class);
        if(!resultDto.isSuccess()){
            throw new RuntimeException(resultDto.getResultMessage());
        }
        resultVo=DozerMapper.map(resultDto,DataNodePageResultVO.class);
        return resultVo;
    }

    /**
     * 元数据节点监控
     * @param ip
     * @param port
     * @return
     */
    public NodeInfoVO metaNodeMonitor(String ip,Integer port){
        Map<String,Object> map= HttpUtils.request(restServiceUrl, META_NODE_MONITOR+"/"+ip+"/"+port, Constants.HTTP_REQUEST_GET, "", null);
        String body=(String)map.get("content");
        NodeInfoResultDto resultDto=JsonUtils.toJavaObject(body, NodeInfoResultDto.class);
        if(!resultDto.isSuccess()){
            throw new RuntimeException(resultDto.getResultMessage());
        }
        NodeInfoDto dto = resultDto.getNodeInfo();
        if(dto==null){
            return null;
        }
        NodeInfoVO vo=DozerMapper.map(dto,NodeInfoVO.class);
        return vo;
    }

}
