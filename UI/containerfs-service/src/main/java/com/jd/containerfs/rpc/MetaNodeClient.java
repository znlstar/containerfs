package com.jd.containerfs.rpc;

import com.jd.containerfs.common.BaseResult;
import com.jd.containerfs.common.util.DozerMapper;
import com.jd.containerfs.dto.node.*;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import mp.MetaNodeGrpc;
import mp.Metanode.*;
import org.apache.commons.lang.StringUtils;

/**
* Created by lixiaoping3 on 17-11-16.
*/
public class MetaNodeClient {

    private ManagedChannel channel;
    private MetaNodeGrpc.MetaNodeBlockingStub blockingStub;

    private MetaNodeClient(String host, int port){
        channel= ManagedChannelBuilder.forAddress(host, port).
                usePlaintext(true).build();
        blockingStub= MetaNodeGrpc.newBlockingStub(channel);
    }

    private MetaNodeClient(String target){
        channel=ManagedChannelBuilder.forTarget(target).usePlaintext(true).build();
        blockingStub=MetaNodeGrpc.newBlockingStub(channel);
    }

    public static MetaNodeClient buildMetaNodeClient(String host,int port){
        MetaNodeClient client=new MetaNodeClient(host,port);
        return client;
    }

    public static MetaNodeClient buildMetaNodeClient(String target){//target ---> "host:port"
        MetaNodeClient client=new MetaNodeClient(target);
        return client;
    }

    private void shutdown(){
        try{
            if(!channel.isShutdown()){
                channel.shutdown();
            }
        }catch(Exception e){
            e.printStackTrace();
        }
    }

    /**
     * 获取集群信息
     * @return
     */
    public ClusterInfoResultDto getClusterInfo() {
        ClusterInfoResultDto resultDto=new ClusterInfoResultDto();
        ClusterInfoReq request= ClusterInfoReq.newBuilder().build();
        ClusterInfoAck ack=null;
        try{
            ack=blockingStub.clusterInfo(request);
        }catch(Exception e){
            e.printStackTrace();
            setErrorResult(resultDto,"rpc->clusterInfo异常");
            return resultDto;
        }finally {
            shutdown();
        }
        if(ack!=null){
            ClusterInfoDto dto= DozerMapper.map(ack, ClusterInfoDto.class);
            resultDto.setClusterInfo(dto);
        }
        resultDto.setSuccess(true);
        return resultDto;
    }

    /**
     * 获取LEADER
     * @param metaLeaderParam
     * @return
     */
    public MetaLeaderResultDto getMetaLeader(MetaLeaderParamDto metaLeaderParam){
        MetaLeaderResultDto resultDto=new MetaLeaderResultDto();
        GetMetaLeaderReq request=GetMetaLeaderReq.newBuilder().setVolID(metaLeaderParam.getVolId()).build();
        GetMetaLeaderAck ack=null;
        try{
            ack=blockingStub.getMetaLeader(request);
        }catch(Exception e){
            e.printStackTrace();
            setErrorResult(resultDto,"rpc->getMetaLeader异常");
            return resultDto;
        }finally {
            shutdown();
        }
        String leader=null;
        if(ack!=null){
            MetaLeaderDto dto = DozerMapper.map(ack, MetaLeaderDto.class);
            leader=dto.getLeader();
            if(StringUtils.isNotEmpty(leader)){
                resultDto.setMetaLeader(dto);
            }
        }
        if(!StringUtils.isNotEmpty(leader)){
            setErrorResult(resultDto,"LEADER节点为空");
            return resultDto;
        }
        resultDto.setSuccess(true);
        return resultDto;
    }

    /**
     * 查询数据节点
     * @return
     */
    public DataNodePageResultDto getDataNodes()  {
        DataNodePageResultDto resultDto=new DataNodePageResultDto();
        GetAllDatanodeReq request= GetAllDatanodeReq.newBuilder().build();
        GetAllDatanodeAck ack=null;
        try{
            ack=blockingStub.getAllDatanode(request);
        }catch(Exception e){
            e.printStackTrace();
            setErrorResult(resultDto,"rpc->getAllDatanode异常");
            return resultDto;
        }finally {
            shutdown();
        }
        if(ack!=null){
            resultDto=DozerMapper.map(ack, DataNodePageResultDto.class);
        }
        resultDto.setSuccess(true);
        return resultDto;
    }

    /**
     * 查询VOLUMES
     * @return
     */
    public VolumePageResultDto getVolumes() {
        VolumePageResultDto resultDto=new VolumePageResultDto();
        VolumeInfosReq req=VolumeInfosReq.newBuilder().build();
        VolumeInfosAck ack=null;
        try{
            ack=blockingStub.volumeInfos(req);
        }catch(Exception e){
            e.printStackTrace();
            setErrorResult(resultDto,"rpc->volumeInfos异常");
            return resultDto;
        }finally {
            shutdown();
        }
        if(ack!=null){
            resultDto=DozerMapper.map(ack,VolumePageResultDto.class);
        }
        resultDto.setSuccess(true);
        return resultDto;
    }

    /**
     * 查询VOLUME信息
     * @param volumeParam
     * @return
     */
    public VolResultDto getVolInfo(VolumeParamDto volumeParam) {
        VolResultDto resultDto=new VolResultDto();
        GetVolInfoReq request=GetVolInfoReq.newBuilder().setUUID(volumeParam.getUuid()).build();
        GetVolInfoAck ack=null;
        try{
            ack=blockingStub.getVolInfo(request);
        }catch(Exception e){
            e.printStackTrace();
            setErrorResult(resultDto,"rpc->getVolInfo异常");
            return resultDto;
        }finally {
            shutdown();
        }
        if(ack!=null){
            resultDto=DozerMapper.map(ack,VolResultDto.class);
        }
        resultDto.setSuccess(true);
        return resultDto;
    }

    public MetaNodeResultDto getMetaNodeInfo() {
        MetaNodeResultDto resultDto=new MetaNodeResultDto();
        MetaNodeInfoReq request=MetaNodeInfoReq.newBuilder().build();
        MetaNodeInfoAck ack=null;
        try{
            ack=blockingStub.metaNodeInfo(request);
        }catch(Exception e){
            e.printStackTrace();
            setErrorResult(resultDto,"rpc->metaNodeInfo异常");
            return resultDto;
        }finally {
            shutdown();
        }
        if(ack!=null){
            MetaNodeDto dto=DozerMapper.map(ack,MetaNodeDto.class);
            resultDto.setMetaNode(dto);
        }
       resultDto.setSuccess(true);
        return resultDto;
    }

    public NodeInfoResultDto metaNodeMonitor(){
        NodeInfoResultDto resultDto=new NodeInfoResultDto();
        NodeMonitorReq request=NodeMonitorReq.newBuilder().build();
        NodeMonitorAck ack=null;
        try{
            ack=blockingStub.nodeMonitor(request);
        }catch(Exception e){
            e.printStackTrace();
            setErrorResult(resultDto,"rpc->nodeMonitor异常");
            return resultDto;
        }finally {
            shutdown();
        }
        if(ack!=null){
            NodeInfo nodeInfo=ack.getNodeInfo();
            if(nodeInfo!=null){
                NodeInfoDto monitorInfo=DozerMapper.map(nodeInfo,NodeInfoDto.class);
                resultDto.setNodeInfo(monitorInfo);
            }
        }
        resultDto.setSuccess(true);
        return resultDto;
    }

    private void setErrorResult(BaseResult result,String errorMessage){
        result.setSuccess(false);
        result.setResultMessage(errorMessage);
    }

}
