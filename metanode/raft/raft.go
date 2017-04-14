package mRaft

import (
	"github.com/lostz/graft"
)

type Raft struct {
	Me     string
	MePort int
	Peer   []string
	R      *graft.Raft
}

var RaftInfo Raft

func StartRaftService() *graft.Raft {
	var err error
	chanState := make(chan bool, 100)
	RaftInfo.R, err = graft.New(RaftInfo.Peer, RaftInfo.Me, RaftInfo.MePort, chanState)
	if err != nil {
		return nil
	} else {
		return RaftInfo.R
	}
}
