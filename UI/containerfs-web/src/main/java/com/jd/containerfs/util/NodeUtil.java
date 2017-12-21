package com.jd.containerfs.util;

import com.jd.containerfs.dto.node.DiskIoDto;
import com.jd.containerfs.dto.node.NetIoDto;
import com.jd.containerfs.dto.node.NodeInfoDto;
import org.apache.commons.collections.CollectionUtils;

import java.util.Iterator;
import java.util.List;

/**
 * Created by lixiaoping3 on 17-12-7.
 */
public class NodeUtil {

    public static void filterNetIos(NodeInfoDto node){
        if(node==null){
            return;
        }
        List<NetIoDto> list=node.getNetIosList();
        if(CollectionUtils.isEmpty(list)){
            return;
        }
        Iterator<NetIoDto> it=list.iterator();
        while(it.hasNext()){
            NetIoDto io=it.next();
            if(io==null){
                continue;
            }
            if(io.getName()!=null&&io.getName().startsWith("cali")){
                it.remove();
            }
        }

    }

    public static void filterDiskIos(NodeInfoDto node){
        if(node==null){
            return;
        }
        List<DiskIoDto> list = node.getDiskIosList();
        if(CollectionUtils.isEmpty(list)){
            return;
        }
        Iterator<DiskIoDto> it=list.iterator();
        while(it.hasNext()){
            DiskIoDto diskIo = it.next();
            if(diskIo==null){
                continue;
            }
            if(diskIo.getName()!=null && diskIo.getName().startsWith("dm-")){
                it.remove();
            }else if(diskIo.getName()!=null && diskIo.getName().startsWith("nvme")){
                diskIo.setName(diskIo.getName().replace("nvme","sas"));
            }
        }
    }
}
