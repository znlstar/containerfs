package com.jd.containerfs.dto.node;

import java.io.Serializable;

/**
 * Created by lixiaoping3 on 17-11-17.
 */
public class MetaLeaderDto  implements Serializable{

    private static final long serialVersionUID = 1381842751399267749L;

    private String leader;

    public String getLeader() {
        return leader;
    }

    public void setLeader(String leader) {
        this.leader = leader;
    }
}
