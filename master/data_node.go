package master

import (
	"github.com/tiglabs/action_dev/uti"
	"math/rand"
	"sync"
	"time"
)

const (
	ReservedVolCount = 1
)

type DataNode struct {
	TcpAddr            string `json:"TcpAddr"`
	MaxDiskAvailWeight uint64 `json:"MaxDiskAvailWeight"`
	Total              uint64 `json:"TotalWeight"`
	Used               uint64 `json:"UsedWeight"`
	ZoneName           string `json:"Zone"`
	HttpAddr           string

	reportTime time.Time
	isActive   bool
	sync.Mutex
	ratio       float64
	selectCount uint64
	carry       float64
	sender      *AdminTaskSender
}

func NewDataNode() (dnode *DataNode) {
	dnode = new(DataNode)
	dnode.carry = rand.Float64()
	dnode.Total = 1
	dnode.sender = NewAdminTaskSender(dnode.HttpAddr)

	return
}

/*check node heartbeat if reportTime > DataNodeTimeOut,then isActive is false*/
func (dnode *DataNode) checkHeartBeat() {
	dnode.Lock()
	defer dnode.Unlock()
	if time.Since(dnode.reportTime) > time.Second*(time.Duration(gConfig.NodeTimeOutSec)) {
		dnode.isActive = false
	}

	return
}

/*set node is online*/
func (dnode *DataNode) setNodeAlive() {
	dnode.Lock()
	defer dnode.Unlock()
	dnode.reportTime = time.Now()
	dnode.isActive = true
}

/*check not is offline*/
func (dnode *DataNode) checkIsActive() bool {
	dnode.Lock()
	defer dnode.Unlock()
	return dnode.isActive

}

func (dnode *DataNode) UpdateNodeMetric(sourceNode *DataNode) {
	dnode.Lock()
	defer dnode.Unlock()
	dnode.MaxDiskAvailWeight = sourceNode.MaxDiskAvailWeight
	dnode.Total = sourceNode.Total
	dnode.Used = sourceNode.Used
	dnode.ZoneName = sourceNode.ZoneName
	dnode.ratio = (float64)(dnode.Used) / (float64)(dnode.Total)
}

func (dnode *DataNode) IsWriteAble() (ok bool) {
	dnode.Lock()
	defer dnode.Unlock()

	if dnode.isActive == true && dnode.MaxDiskAvailWeight > (uint64)(util.DefaultVolSize) &&
		dnode.Total-dnode.Used > (uint64)(util.DefaultVolSize)*ReservedVolCount {
		ok = true
	}

	return
}

func (dnode *DataNode) IsAvailCarryNode() (ok bool) {
	dnode.Lock()
	defer dnode.Unlock()

	return dnode.carry >= 1
}

func (dnode *DataNode) SetCarry(carry float64) {
	dnode.Lock()
	defer dnode.Unlock()
	dnode.carry = carry
}

func (dnode *DataNode) SelectNodeForWrite() {
	dnode.Lock()
	defer dnode.Unlock()
	dnode.ratio = float64(dnode.Used) / float64(dnode.Total)
	dnode.selectCount++
	dnode.Used += (uint64)(util.DefaultVolSize)
	dnode.carry = dnode.carry - 1.0
}
