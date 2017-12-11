package com.jd.containerfs.controller;

import com.jd.containerfs.manager.MetaNodeManager;
import com.jd.containerfs.vo.ClusterInfoVO;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;

import javax.annotation.Resource;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Created by wuzhengxuan on 2017/7/26.
 * 首页相关CONTROLLER
 */
@Controller
public class IndexController {

    @Resource
    private MetaNodeManager metaNodeManager;

    /**
     * 首页
     * @param model
     * @return
     */
    @RequestMapping(value = "/", method = RequestMethod.GET)
    public String index(Model model) {
        return "/index";
    }

    /**
     * 页面HEADER
     * @param model
     * @return
     */
    @RequestMapping(value = "/topbar", method = RequestMethod.GET)
    public String topbar(Model model) {
        return "/topbar";
    }

    /**
     * 左侧菜单栏
     * @param model
     * @return
     */
    @RequestMapping(value = "/side_menu", method = RequestMethod.GET)
    public String sideMenu(Model model) {
        return "/side_menu";
    }

    /**
     * 集群信息
     * @param engineRoom
     * @param model
     * @return
     */
    @RequestMapping(value = "/main", method = RequestMethod.GET)
    public String main(String engineRoom,Model model) {
        ClusterInfoVO cluster=metaNodeManager.getCluster();
        SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Date now = new Date();
        String dateStr = formatter.format(now);
        model.addAttribute("serverNum",cluster.getMetaNum()+cluster.getDataNum());
        model.addAttribute("MetaNum",cluster.getMetaNum());
        model.addAttribute("DataNum",cluster.getDataNum());
        model.addAttribute("VolNum",cluster.getVolNum());
        model.addAttribute("ClusterSpace",cluster.getClusterSpace());
        model.addAttribute("ClusterFreeSpace",cluster.getClusterFreeSpace());
        model.addAttribute("dateStr",dateStr);
        model.addAttribute("io",cluster.getIO());
        model.addAttribute("iops",cluster.getIOPS());
        return "/main";
    }

    /**
     * 集群信息动态刷新
     * @param engineRoom
     * @return
     */
    @ResponseBody
    @RequestMapping(value = "/clusterData", method = RequestMethod.GET)
    public String main(String engineRoom){

        SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Date now = new Date();
        String dateStr = formatter.format(now);
        ClusterInfoVO cluster=metaNodeManager.getCluster();
        int io=cluster.getIO();
        int iops=cluster.getIOPS();
        String returnJson = "{\"dateStr\":\""+dateStr+"\",\"io\":\""+io+"\",\"iops\":\""+iops+"\"}";
        return returnJson;
    }
}
