package com.jd.containerfs.rest;

import com.jd.containerfs.dto.node.NodeInfoParamDto;
import com.jd.containerfs.dto.node.NodeInfoResultDto;
import com.jd.containerfs.service.DataNodeService;
import org.springframework.web.bind.annotation.*;

import javax.annotation.Resource;

/**
 * Created by lixiaoping3 on 17-11-20.
 * 数据节点REST接口
 */
@RestController
@RequestMapping("datanode")
public class DataNodeRestSvc {

    @Resource
    private DataNodeService dataNodeService;

    /**
     * DataNode的监控信息
     * @param ip
     * @param port
     * @return
     */
    @RequestMapping(value="/monitor/{ip}/{port}",method = RequestMethod.GET,produces = "application/json")
    public NodeInfoResultDto dataNodeMonitor(@PathVariable("ip") String ip,@PathVariable("port") Integer port){
        NodeInfoParamDto param=new NodeInfoParamDto();
        param.setIp(ip);
        param.setPort(port);
        NodeInfoResultDto resultDto=dataNodeService.dataNodeMonitor(param);
        return resultDto;
    }

}
