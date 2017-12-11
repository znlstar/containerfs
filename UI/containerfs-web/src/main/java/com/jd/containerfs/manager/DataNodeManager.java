package com.jd.containerfs.manager;

import com.jd.containerfs.common.Constants;
import com.jd.containerfs.common.util.DozerMapper;
import com.jd.containerfs.common.util.HttpUtils;
import com.jd.containerfs.common.util.JsonUtils;
import com.jd.containerfs.dto.node.NodeInfoDto;
import com.jd.containerfs.dto.node.NodeInfoResultDto;
import com.jd.containerfs.util.NodeUtil;
import com.jd.containerfs.vo.DiskIoVO;
import com.jd.containerfs.vo.NetIoVO;
import com.jd.containerfs.vo.NodeInfoVO;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.*;

/**
 * Created by lixiaoping3 on 17-11-21.
 */
@Service
public class DataNodeManager {
    @Resource
    private String restServiceUrl;

    private static final String DATA_NODE_MONITOR="/datanode/monitor";

    //key是IP
    private static Map<String,List<NodeInfoVO>> nodeInfoMap = new HashMap<>();

    //key是IP
    private static Map<String,List<Date>> nodeInfoTimeMap = new HashMap<>();

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
        NodeUtil.filterDiskIos(dto);
        NodeUtil.filterNetIos(dto);
        NodeInfoVO vo= DozerMapper.map(dto,NodeInfoVO.class);

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
    private NodeInfoVO getBetweenDate(NodeInfoVO oldVO,NodeInfoVO newVO,Date oldDate,Date newDate){

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
