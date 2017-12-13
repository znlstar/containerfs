package com.jd.containerfs.manager;

import com.jd.containerfs.common.Constants;
import com.jd.containerfs.common.util.DozerMapper;
import com.jd.containerfs.common.util.HttpUtils;
import com.jd.containerfs.common.util.JsonUtils;
import com.jd.containerfs.dto.node.*;
import com.jd.containerfs.util.NodeUtil;
import com.jd.containerfs.vo.*;
import org.apache.commons.lang.StringUtils;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.*;

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

    //key是IP
    private static Map<String,List<NodeInfoVO>> nodeInfoMap = new HashMap<>();

    //key是IP
    private static Map<String,List<Date>> nodeInfoTimeMap = new HashMap<>();

    /**
     * 集群信息
     * @return
     */
    public ClusterInfoVO getCluster(){
        Map<String,Object> map=HttpUtils.request(restServiceUrl,CLUSTER_INFO, Constants.HTTP_REQUEST_GET,"",null);
        String body=(String)map.get("content");
        System.out.println("getCluster:===================>");
        System.out.println(body);
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
        NodeUtil.filterDiskIos(dto);
        NodeUtil.filterNetIos(dto);
        NodeInfoVO vo=DozerMapper.map(dto,NodeInfoVO.class);

        //从历史数据中取出数据
        List<NodeInfoVO> ipNodeInfoVOList = nodeInfoMap.get(ip);
        List<Date> ipNodeInfoVOTimeList = nodeInfoTimeMap.get(ip);
        if(ipNodeInfoVOList==null){
            ipNodeInfoVOList = new ArrayList<NodeInfoVO>();
            ipNodeInfoVOTimeList = new ArrayList<Date>();

            //将所有的数据项设置为0
            NodeInfoVO keepVO =  vo;
            vo = this.getInitDate(vo);

            ipNodeInfoVOList.add(keepVO);//存数据不能存全是0的，要存有数据的
            ipNodeInfoVOTimeList.add(new Date());
        }else{

            //获取当次数据和上一次采集数据的平均值
            NodeInfoVO oldVO = ipNodeInfoVOList.get(ipNodeInfoVOList.size()-1);
            Date oldDate = ipNodeInfoVOTimeList.get(ipNodeInfoVOTimeList.size()-1);
            Date now = new Date();
            NodeInfoVO keepVO =  vo;
            vo = this.getBetweenDate(oldVO,vo,oldDate,now);

            ipNodeInfoVOList.add(keepVO);
            ipNodeInfoVOTimeList.add(now);
            if(ipNodeInfoVOList.size()>10){
                ipNodeInfoVOList.remove(0);
                ipNodeInfoVOTimeList.remove(0);
            }
        }

        //把LIST放回map中
        nodeInfoMap.put(ip,ipNodeInfoVOList);
        nodeInfoTimeMap.put(ip,ipNodeInfoVOTimeList);

        return vo;
    }


    /**
     * 一次请求时初始化数据
     * @param nodeInfoVO
     * @return
     */
    private NodeInfoVO getInitDate(NodeInfoVO nodeInfoVO){
        List<DiskIoVO> diskIoVOList = nodeInfoVO.getDiskIosList();
        for(DiskIoVO diskIoVO : diskIoVOList){
            diskIoVO.setReadCount(0L);
            diskIoVO.setWriteCount(0L);
            diskIoVO.setReadBytes(0L);
            diskIoVO.setWriteBytes(0L);
        }

        List<NetIoVO> netIosList = nodeInfoVO.getNetIosList();
        for(NetIoVO netIoVO : netIosList){
            netIoVO.setBytesSent(0L);
            netIoVO.setBytesRecv(0L);
            netIoVO.setPacketsSent(0L);
            netIoVO.setPacketRecv(0L);
            netIoVO.setErrIn(0L);
            netIoVO.setErrOut(0L);
            netIoVO.setDropIn(0L);
            netIoVO.setDropOut(0L);
        }

        return nodeInfoVO;
    }

    /**
     * 两次数据取平均值
     * @param oldVO
     * @param newVO
     * @param oldDate
     * @param newDate
     * @return
     */
    private NodeInfoVO getBetweenDate(NodeInfoVO oldVO, NodeInfoVO newVO, Date oldDate, Date newDate){

        //获取时间相隔秒数
        Long second = (newDate.getTime()-oldDate.getTime())/1000;

        NodeInfoVO vo = new NodeInfoVO();
        vo.setCpuUsage(newVO.getCpuUsage());
        vo.setCpuLoad(newVO.getCpuLoad());
        vo.setFreeMem(newVO.getFreeMem());
        vo.setUsedMem(newVO.getUsedMem());
        vo.setTotalMem(newVO.getTotalMem());
        vo.setMemUsedPercent(newVO.getMemUsedPercent());
        vo.setPathTotal(newVO.getPathTotal());
        vo.setPathFree(newVO.getPathFree());
        vo.setPathUsedPercent(newVO.getPathUsedPercent());

        //设置磁盘
        List<DiskIoVO> oldDiskIoVOList = oldVO.getDiskIosList();
        List<DiskIoVO> newDiskIoVOList = newVO.getDiskIosList();
        List<DiskIoVO> diskIoVOList = new ArrayList<>();
        DiskIoVO diskIoVO = null;
        for(int i=0;i<oldDiskIoVOList.size();i++){
            diskIoVO = new DiskIoVO();
            diskIoVO.setName(newDiskIoVOList.get(i).getName());
            diskIoVO.setReadCount((newDiskIoVOList.get(i).getReadCount()-oldDiskIoVOList.get(i).getReadCount())/second);
            diskIoVO.setWriteCount((newDiskIoVOList.get(i).getWriteCount()-oldDiskIoVOList.get(i).getWriteCount())/second);
            diskIoVO.setReadBytes((newDiskIoVOList.get(i).getReadBytes()-oldDiskIoVOList.get(i).getReadBytes())/second);
            diskIoVO.setWriteBytes((newDiskIoVOList.get(i).getWriteBytes()-oldDiskIoVOList.get(i).getWriteBytes())/second);
            diskIoVOList.add(diskIoVO);
        }
        vo.setDiskIosList(diskIoVOList);

        //设置网络
        List<NetIoVO> oldNetIosList = oldVO.getNetIosList();
        List<NetIoVO> newNetIosList = newVO.getNetIosList();
        List<NetIoVO> netIosList = new ArrayList<>();
        NetIoVO netIoVO = null;
        for(int i=0;i<newNetIosList.size();i++){
            netIoVO = new NetIoVO();
            netIoVO.setName(newNetIosList.get(i).getName());
            netIoVO.setBytesSent((newNetIosList.get(i).getBytesSent()-oldNetIosList.get(i).getBytesSent())/second);
            netIoVO.setBytesRecv((newNetIosList.get(i).getBytesRecv()-oldNetIosList.get(i).getBytesRecv())/second);
            netIoVO.setPacketsSent((newNetIosList.get(i).getPacketsSent()-oldNetIosList.get(i).getPacketsSent())/second);
            netIoVO.setPacketRecv((newNetIosList.get(i).getPacketRecv()-oldNetIosList.get(i).getPacketRecv())/second);
            netIoVO.setErrIn((newNetIosList.get(i).getErrIn()-oldNetIosList.get(i).getErrIn())/second);
            netIoVO.setErrOut((newNetIosList.get(i).getErrOut()-oldNetIosList.get(i).getErrOut())/second);
            netIoVO.setDropIn((newNetIosList.get(i).getDropIn()-oldNetIosList.get(i).getDropIn())/second);
            netIoVO.setDropOut((newNetIosList.get(i).getDropOut()-oldNetIosList.get(i).getDropOut())/second);
            netIosList.add(netIoVO);
        }
        vo.setNetIosList(netIosList);

        return vo;
    }


}
