package com.jd.containerfs.controller;

import com.jd.containerfs.common.util.JSONHelper;
import com.jd.containerfs.manager.MetaNodeManager;
import com.jd.containerfs.vo.*;
import org.apache.commons.collections.CollectionUtils;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;

import javax.annotation.Resource;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

/**
 * Created by lixiaoping3 on 17-11-21.
 * 元数据节点CONTROLLER
 */
@Controller
@RequestMapping("/metanode")
public class MetaNodeController extends BaseController{

    @Resource
    private MetaNodeManager metaNodeManager;

    /**
     * 元数据节点列表查询
     * @param model
     * @return
     */
    @RequestMapping(value="/list",method = RequestMethod.GET)
    public String listMetaNodes(Model model){
        MetaNodePageResultVO resultVo=metaNodeManager.getMetaNodeList();
        List<MetaNodeInfoVO> list=resultVo.getList();
        model.addAttribute("metanodes",list);
        return "/metanode/metanodes_list";
    }

    /**
     *
     * 数据节点列表查询
     * @param param
     * @param model
     * @return
     */
    @RequestMapping(value="/dataNodeList",method = RequestMethod.GET)
    public String listDataNodes(DataNodePageParamVO param,Model model){
        DataNodePageResultVO resultVo=metaNodeManager.getDataNodeList(param);
        List<DataNodeVO> list=resultVo.getDatanodesList();
        int total=0;
        int used=0;
        if(CollectionUtils.isNotEmpty(list)){
            for(DataNodeVO node:list){
//                total+=(node.getFree()+node.getUsed());
                total+=node.getCapacity();
                used+=node.getUsed();
            }
        }
        model.addAttribute("dataNodeParam",param);
        model.addAttribute("datanodes",list);
        model.addAttribute("usedSpace",used);
        model.addAttribute("totalSpace",total);
        return "/metanode/datanodes_list";
    }

    @RequestMapping(value="/volumeList",method=RequestMethod.GET)
    public String volumeList(VolumePageParamVO paramVO,Model model){
        VolumePageResultVO resultVo=metaNodeManager.getVolumeList(paramVO);
        List<VolumeVO> list=resultVo.getVolumesList();
        model.addAttribute("volumes",list);
        model.addAttribute("volParam",paramVO);
        return "/metanode/volumes_list";
    }

    /**
     * 跳转到MetaNode监控页
     * @param ip
     * @param port
     * @param model
     * @return
     */
    @RequestMapping(value="/monitor/{ip}/{port}",method = RequestMethod.GET)
    public String metaNodeMonitor(@PathVariable("ip")String ip, @PathVariable("port") Integer port, Model model){

        NodeInfoVO vo = metaNodeManager.metaNodeMonitor(ip,port);
        System.out.println("metanode netIo=====");
        System.out.println(JSONHelper.toJSON(vo.getNetIosList()));
        SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Date now = new Date();
        String dateStr = formatter.format(now);
        vo.setDate(dateStr);
        model.addAttribute("monitor",vo);
        model.addAttribute("ip",ip);
        model.addAttribute("port",port);
        return "metanode/monitor";
    }

    /**
     * ajax动态刷新MetaNode信息
     * @param ip
     * @param port
     * @return
     */
    @ResponseBody
    @RequestMapping(value="/data/{ip}/{port}",method = RequestMethod.GET)
    public NodeInfoVO metaNodeData(@PathVariable("ip")String ip, @PathVariable("port") Integer port){
        NodeInfoVO nodeInfoVO = metaNodeManager.metaNodeMonitor(ip,port);
        SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Date now = new Date();
        String dateStr = formatter.format(now);
        nodeInfoVO.setDate(dateStr);
        return nodeInfoVO;
    }
}
