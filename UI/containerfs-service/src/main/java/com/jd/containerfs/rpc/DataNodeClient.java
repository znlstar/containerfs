package com.jd.containerfs.rpc;

import com.jd.containerfs.common.BaseResult;
import com.jd.containerfs.common.util.DozerMapper;
import com.jd.containerfs.dto.node.NodeInfoDto;
import com.jd.containerfs.dto.node.NodeInfoParamDto;
import com.jd.containerfs.dto.node.NodeInfoResultDto;
import dp.DataNodeGrpc;
import dp.DataNodeGrpc.*;
import dp.Datanode;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

/**
 * Created by lixiaoping3 on 17-11-17.
 */
public class DataNodeClient {

    private ManagedChannel channel;
    private DataNodeBlockingStub blockingStub;
//    private DataNodeFutureStub  asyncStub;

    private DataNodeClient(String host, int port){
        channel= ManagedChannelBuilder.forAddress(host, port).
                usePlaintext(true).build();
        blockingStub=DataNodeGrpc.newBlockingStub(channel);
//        asyncStub=DataNodeGrpc.newFutureStub(channel);
    }

    public DataNodeClient(String target){
        channel=ManagedChannelBuilder.forTarget(target).usePlaintext(true).build();
        blockingStub=DataNodeGrpc.newBlockingStub(channel);
//        asyncStub=DataNodeGrpc.newFutureStub(channel);
    }

    public static DataNodeClient buildDataNodeClient(String host,int port){
        DataNodeClient client=new DataNodeClient(host,port);
        return client;
    }

    public static DataNodeClient buildDataNodeClient(String target){//target ---> "host:port"
        DataNodeClient client=new DataNodeClient(target);
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
     * 数据节点监控
     * @param paramDto
     * @return
     */
    public NodeInfoResultDto dataNodeMonitor(NodeInfoParamDto paramDto){
        Datanode.NodeMonitorReq request= Datanode.NodeMonitorReq.newBuilder().build();
        Datanode.NodeMonitorAck ack=null;
        NodeInfoResultDto resultDto=new NodeInfoResultDto();
        try{
            ack=blockingStub.nodeMonitor(request);
        }catch(Exception e){
            setErrorResult(resultDto,"调用nodeMonitor异常");
            return resultDto;
        }finally {
            shutdown();
        }
        if(ack==null){
            setErrorResult(resultDto,"调用nodeMonitor返回空");
        }
        Datanode.NodeInfo nodeInfo=ack.getNodeInfo();
        if(nodeInfo!=null){
            NodeInfoDto monitorInfo= DozerMapper.map(nodeInfo, NodeInfoDto.class);
            resultDto.setNodeInfo(monitorInfo);
        }
        resultDto.setSuccess(true);
        return resultDto;
    }

    private void setErrorResult(BaseResult result,String errorMessage){
        result.setSuccess(false);
        result.setResultMessage(errorMessage);
    }

}
