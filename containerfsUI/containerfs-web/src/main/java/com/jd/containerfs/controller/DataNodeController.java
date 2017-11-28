package com.jd.containerfs.controller;

import com.jd.containerfs.manager.DataNodeManager;
import com.jd.containerfs.vo.NodeInfoVO;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;

import javax.annotation.Resource;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Created by wuzhengxuan on 2017/11/21.
 * 数据节点CONTROLLER
 */
@Controller
@RequestMapping("/datanode")
public class DataNodeController extends BaseController{

    @Resource
    private DataNodeManager dataNodeManager;

    /**
     * 跳转到DataNode监控页
     * @param ip
     * @param port
     * @param model
     * @return
     */
    @RequestMapping(value="/monitor/{ip}/{port}",method = RequestMethod.GET)
    public String dataNodeMonitor(@PathVariable("ip")String ip, @PathVariable("port") Integer port, Model model){
        NodeInfoVO vo = dataNodeManager.dataNodeMonitor(ip,port);
        SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Date now = new Date();
        String dateStr = formatter.format(now);
        vo.setDate(dateStr);
        model.addAttribute("monitor",vo);
        model.addAttribute("ip",ip);
        model.addAttribute("port",port);

        return "datanode/monitor";
    }

    /**
     * ajax动态刷新DataNode信息
     * @param ip
     * @param port
     * @return
     */
    @ResponseBody
    @RequestMapping(value="/data/{ip}/{port}",method = RequestMethod.GET)
    public NodeInfoVO dataNodeData(@PathVariable("ip")String ip, @PathVariable("port") Integer port){
        NodeInfoVO vo = dataNodeManager.dataNodeMonitor(ip,port);
        SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Date now = new Date();
        String dateStr = formatter.format(now);
        DecimalFormat df = new DecimalFormat("#.00");
        vo.setDate(dateStr);
        return vo;
    }
}
