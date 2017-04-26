package mRaft

import (
	"github.com/lostz/graft"
)

// raft struct
type Raft struct {
	Me     string
	MePort int
	Peer   []string
	R      *graft.Raft
}

// raft info
var RaftInfo Raft

// start service
func StartRaftService() *graft.Raft {
	var err error
	chanState := make(chan bool, 100)
	RaftInfo.R, err = graft.New(RaftInfo.Peer, RaftInfo.Me, RaftInfo.MePort, chanState)
	if err != nil {
		return nil
	}
	return RaftInfo.R
}
