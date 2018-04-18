package metanode

import (
	"encoding/json"
	"github.com/tiglabs/containerfs/proto"
	"github.com/tiglabs/containerfs/util"
	"github.com/tiglabs/containerfs/util/log"
	"net/http"
	"github.com/tiglabs/containerfs/util/config"
)

const (
	HandleMasterUrl  = "/cmd/master"
	HandleSdkUrl     = "/cmd/sdk"
	ReplyToMasterUrl = "/node/response"
)

var (
	gLog = log.GetLog()
)

type MetaNode struct {
	ip         string
	HttpAddr   string
	logDir     string
	toAdminCh  chan *proto.AdminTask
	masterAddr string
}

func NewServer() (m *MetaNode) {
	return &MetaNode{}
}

func (m *MetaNode) HandleMaster(w http.ResponseWriter, r *http.Request) {

}

func (m *MetaNode) HandleSdk(w http.ResponseWriter, r *http.Request) {

}

func (m *MetaNode) Start(config *config.Config) (err error) {
	m.toAdminCh = make(chan *proto.AdminTask, 1024)
	log.NewLog(m.logDir, "metanode", log.DebugLevel)
	go m.replyCmdToAdmin()
	http.HandleFunc(HandleMasterUrl, m.HandleMaster)
	http.HandleFunc(HandleSdkUrl, m.HandleSdk)

	return
}

func (m *MetaNode) replyCmdToAdmin() {
	for {
		select {
		case t := <-m.toAdminCh:
			tasks := make([]*proto.AdminTask, 0)
			tasks = append(tasks, t)
			data, err := json.Marshal(tasks)
			if err != nil {
				continue
			}
			util.PostToDataNode(data, ReplyToMasterUrl)
		}
	}

}
