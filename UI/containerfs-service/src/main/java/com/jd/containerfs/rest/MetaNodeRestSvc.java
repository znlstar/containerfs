package com.jd.containerfs.rest;


import com.jd.containerfs.dto.node.*;
import com.jd.containerfs.service.MetaNodeService;
import org.springframework.web.bind.annotation.*;

import javax.annotation.Resource;
import java.util.List;

/**
 * Created by lixiaoping3 on 17-11-20.
 */
@RestController
@RequestMapping("metanode")
public class MetaNodeRestSvc {

    @Resource
    private MetaNodeService metaNodeService;

    /**
     * 集群信息查询REST
     * @return
     */
    @RequestMapping(value="/cluster",method=RequestMethod.GET,produces = "application/json")
    public ClusterInfoResultDto getClusterInfo(){
        ClusterInfoResultDto resultDto= metaNodeService.getClusterInfo();
        return resultDto;
    }

    /**
     * 元数据节点查询REST
     * @return
     */
    @RequestMapping(value="/list",method=RequestMethod.GET,produces = "application/json")
    public MetaNodePageResultDto getMetaNodeList(){
        MetaNodePageResultDto resultDto=metaNodeService.getMetaNodes(null);
        return resultDto;
    }

    /**
     * 获取LEADER节点REST
     * @return
     */
    @RequestMapping(value="/leader",method = RequestMethod.GET,produces = "application/json")
    public MetaLeaderResultDto getMetaLeader(){
        MetaLeaderResultDto resultDto=metaNodeService.getMetaLeader();
        return resultDto;
    }

    /**
     * 数据节点查询REST
     * @return
     */
    @RequestMapping(value="/datanodes",produces = "application/json")
    public DataNodePageResultDto getDataNodes(@RequestBody DataNodePageParamDto param){
        DataNodePageResultDto resultDto= metaNodeService.getDataNodes(param);
        return resultDto;
    }

    /**
     * volume查询REST
     * @return
     */
    @RequestMapping(value="/volumes",produces = "application/json")
    public VolumePageResultDto getVolumes(@RequestBody VolumePageParamDto param){
        VolumePageResultDto resultDto= metaNodeService.getVolumes(param);
        return resultDto;
    }

    /**
     * 元数据节点探活REST
     * @param ip
     * @param port
     * @return
     */
    @RequestMapping(value="/info/{ip}/{port}",method = RequestMethod.GET,produces = "application/json")
    public MetaNodeResultDto getMetaNodeInfo(@PathVariable("ip") String ip,@PathVariable("port") Integer port){
        MetaNodeParamDto param=new MetaNodeParamDto();
        param.setIp(ip);
        param.setPort(port);
        MetaNodeResultDto resultDto=metaNodeService.getMetaNodeInfo(param);
        return resultDto;
    }

    /**
     * Volume的监控信息
     * @param ip
     * @param port
     * @param uuid
     * @return
     */
    @RequestMapping(value="/volume/{ip}/{port}/{uuid}",method = RequestMethod.GET,produces = "application/json")
    public VolResultDto getVolInfo(@PathVariable("ip") String ip,@PathVariable("port") Integer port,@PathVariable("uuid") String uuid){
        VolumeParamDto paramDto=new VolumeParamDto();
        paramDto.setIp(ip);
        paramDto.setPort(port);
        paramDto.setUuid(uuid);
        VolResultDto resultDto=metaNodeService.getVolInfo(paramDto);
        return resultDto;
    }

    /**
     * MetaNode的监控信息
     * @param ip
     * @param port
     * @return
     */
    @RequestMapping(value="/monitor/{ip}/{port}",method = RequestMethod.GET,produces = "application/json")
    public NodeInfoResultDto metaNodeMonitor(@PathVariable("ip") String ip,@PathVariable("port") Integer port){
        NodeInfoParamDto param=new NodeInfoParamDto();
        param.setIp(ip);
        param.setPort(port);
        NodeInfoResultDto resultDto=metaNodeService.metaNodeMonitor(param);
        return resultDto;
    }


}
