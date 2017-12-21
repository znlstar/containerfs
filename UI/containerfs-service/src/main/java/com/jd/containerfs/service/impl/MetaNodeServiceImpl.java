package com.jd.containerfs.service.impl;

import com.jd.containerfs.common.MetaNodeStatusEnum;
import com.jd.containerfs.dto.node.*;
import com.jd.containerfs.rpc.MetaNodeClient;
import com.jd.containerfs.service.MetaNodeService;
import mp.Metanode;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by lixiaoping3 on 17-11-20.
 */
@Service
public class MetaNodeServiceImpl implements MetaNodeService {

    @Resource
    private String metaNodeStr;//配置的元数据节点

    @Resource
    private String leaderVolId;//集群LEADER ID

    @Override
    public ClusterInfoResultDto getClusterInfo() {
        String leaderTarget=getLeaderTarget();
        if(StringUtils.isEmpty(leaderTarget)){
            return null;
        }
        MetaNodeClient client=MetaNodeClient.buildMetaNodeClient(leaderTarget);
        ClusterInfoResultDto cluster=client.getClusterInfo();
        return cluster;
    }


    @Override
    public MetaLeaderResultDto getMetaLeader() {
        MetaLeaderResultDto leaderResultDto=null;
        MetaLeaderParamDto param=new MetaLeaderParamDto();
        param.setVolId(leaderVolId);
        List<MetaNodeInfoDto> metaNodes=getAllMetaNodes();
        for(MetaNodeInfoDto node:metaNodes){
            MetaNodeClient client=MetaNodeClient.buildMetaNodeClient(node.getIp(),node.getPort());
            leaderResultDto=client.getMetaLeader(param);
            if(!leaderResultDto.isSuccess()){
                continue;
            }
            return leaderResultDto;
        }
        leaderResultDto.setSuccess(false);
        leaderResultDto.setResultMessage("没有获取到LEADER节点信息");
        return leaderResultDto;
    }

    @Override
    public DataNodePageResultDto getDataNodes(DataNodePageParamDto paramDto) {
        String leaderTarget=getLeaderTarget();
        if(StringUtils.isEmpty(leaderTarget)){
            return null;
        }
        MetaNodeClient client=MetaNodeClient.buildMetaNodeClient(leaderTarget);
        DataNodePageResultDto dataNodeResult=client.getDataNodes();
        if(!dataNodeResult.isSuccess()){
            return dataNodeResult;
        }
        String ip=paramDto.getIp();
        List<DataNodeDto> allList=dataNodeResult.getDatanodesList();
        if(StringUtils.isNotEmpty(ip)&&
                CollectionUtils.isNotEmpty(allList)){
            List<DataNodeDto> filterList=new ArrayList<>();
            ip=ip.trim();
            for(DataNodeDto node:allList){
                if(ip.equals(node.getIp())){
                    filterList.add(node);
                }
            }
            dataNodeResult.setDatanodesList(filterList);
            dataNodeResult.setDatanodesCount(filterList.size());
        }
        return dataNodeResult;
    }

    /**
     * 查询LEADER连接信息
     * @return
     */
    private String getLeaderTarget(){
        MetaLeaderResultDto resultDto=getMetaLeader();
        if(!resultDto.isSuccess()){
            return null;
        }
        MetaLeaderDto dto=resultDto.getMetaLeader();
        String leaderTarget=dto.getLeader();
        return leaderTarget;
    }

    @Override
    public MetaNodeResultDto getMetaNodeInfo(MetaNodeParamDto metaNodeParam) {
        MetaNodeClient client=MetaNodeClient.buildMetaNodeClient(metaNodeParam.getIp(),metaNodeParam.getPort());
        MetaNodeResultDto resultDto=client.getMetaNodeInfo();
        return resultDto;
    }

    @Override
    public VolumePageResultDto getVolumes(VolumePageParamDto paramDto) {
        String leaderTarget=getLeaderTarget();
        if(StringUtils.isEmpty(leaderTarget)){
            VolumePageResultDto resultDto=new VolumePageResultDto();
            resultDto.setSuccess(false);
            resultDto.setResultMessage("当前没有LEADER节点");
            return resultDto;
        }
        MetaNodeClient client=MetaNodeClient.buildMetaNodeClient(leaderTarget);
        VolumePageResultDto resultDto=client.getVolumes();
        String uuid=paramDto.getUuid();
        List<VolumeDto> allList=resultDto.getVolumesList();
        if(StringUtils.isNotEmpty(uuid)&&
                CollectionUtils.isNotEmpty(allList)){
            List<VolumeDto> filterList=new ArrayList<>();
            uuid=uuid.trim();
            for(VolumeDto vol:allList){
                if(uuid.equals(vol.getUuid())){
                    filterList.add(vol);
                }
            }
            resultDto.setVolumesList(filterList);
            resultDto.setVolumesCount(filterList.size());
        }
        return resultDto;
    }

    @Override
    public VolResultDto getVolInfo(VolumeParamDto volumeParam) {
        MetaNodeClient client=MetaNodeClient.buildMetaNodeClient(volumeParam.getIp(),volumeParam.getPort());
        MetaLeaderParamDto leaderParam=new MetaLeaderParamDto();
        leaderParam.setVolId(volumeParam.getUuid());
        MetaLeaderResultDto leaderResult=client.getMetaLeader(leaderParam);
        VolResultDto resultDto=new VolResultDto();
        if(!leaderResult.isSuccess()){
            resultDto.setSuccess(false);
            resultDto.setResultMessage(leaderResult.getResultMessage());
            return resultDto;
        }
        MetaLeaderDto leaderDto=leaderResult.getMetaLeader();
        String leaderTarget=leaderDto.getLeader();
        if(StringUtils.isEmpty(leaderTarget)){
            resultDto.setSuccess(false);
            resultDto.setResultMessage("没有获取到LEADER节点信息");
            return resultDto;
        }
        MetaNodeClient leader=MetaNodeClient.buildMetaNodeClient(leaderTarget);
        resultDto=leader.getVolInfo(volumeParam);
        return resultDto;
    }

    @Override
    public NodeInfoResultDto metaNodeMonitor(NodeInfoParamDto nodeInfoParam) {
        MetaNodeClient client=MetaNodeClient.buildMetaNodeClient(nodeInfoParam.getIp(),nodeInfoParam.getPort());
        NodeInfoResultDto resultDto=client.metaNodeMonitor();
        return resultDto;
    }

    private List<MetaNodeInfoDto> getAllMetaNodes(){
        List<MetaNodeInfoDto> list=new ArrayList<MetaNodeInfoDto>();
        if(StringUtils.isEmpty(metaNodeStr)){
            return null;
        }
        String [] nodeArr=metaNodeStr.split(",");
        if(nodeArr.length!=3){
            return null;
        }
        for(String nodeStr:nodeArr){
            String [] arr=nodeStr.split(":");
            String ip=arr[0];
            Integer port=Integer.parseInt(arr[1]);
            String name=arr[2];
            MetaNodeInfoDto node=new MetaNodeInfoDto();
            node.setIp(ip);
            node.setPort(port);
            node.setName(name);
            boolean isNormal=getMetaNodeStatus(ip,port);
            node.setStatusDesc(isNormal?MetaNodeStatusEnum.META_NODE_NORMAL.getStatusDesc():MetaNodeStatusEnum.META_NODE_ABNORMAL.getStatusDesc());
            list.add(node);
        }
        return list;
    }

    private boolean getMetaNodeStatus(String ip,Integer port){//查看元数据节点是否正常
        MetaNodeParamDto param=new MetaNodeParamDto();
        param.setIp(ip);
        param.setPort(port);
        int reTryCount=3;
        boolean normal=false;
        while((reTryCount--)>0){
            MetaNodeResultDto info=null;
            try{
                info=getMetaNodeInfo(param);
            }catch(Exception e){//异常，则重试3次
                e.printStackTrace();
            }
            if(info.isSuccess()){
                normal=true;
                return normal;
            }
        }
        return normal;
    }

    @Override
    public MetaNodePageResultDto getMetaNodes(MetaNodePageParamDto pageParamDto) {
        MetaNodePageResultDto resultDto=new MetaNodePageResultDto();
        List<MetaNodeInfoDto> list=getAllMetaNodes();
        resultDto.setList(list);
        resultDto.setCount(list.size());
        resultDto.setSuccess(true);
        return resultDto;
    }


}
