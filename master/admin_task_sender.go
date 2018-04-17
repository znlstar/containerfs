package master

import (
	"encoding/json"
	"github.com/tiglabs/action_dev/prot"
	"github.com/tiglabs/action_dev/uti"
	"sync"
	"time"
)

const (
	TaskWaitResponseTimeOut = 2
	TaskSendCount           = 5
	TaskSendUrl             = "/node/task"
)

/*this is only master send admin command to metanode or datanode sender
because this command is cost very long time,so the sender send command
and do anything..then the metanode or  datanode send command response
to master use another http handler

*/
type AdminTaskSender struct {
	targetAddr string
	taskMap    map[string]*proto.AdminTask
	sync.Mutex
}

func NewAdminTaskSender(targetAddr string) (sender *AdminTaskSender) {
	sender = &AdminTaskSender{targetAddr: targetAddr, taskMap: make(map[string]*proto.AdminTask)}
	go sender.send()

	return
}

func (sender *AdminTaskSender) send() {
	ticker := time.Tick(time.Second)
	for {
		select {
		case <-ticker:
			time.Sleep(time.Millisecond * 100)
		default:
			tasks := sender.getNeedDealTask()
			if len(tasks) == 0 {
				time.Sleep(time.Millisecond * 100)
				continue
			}
			sender.sendTasks(tasks)

		}
	}

}

func (sender *AdminTaskSender) sendTasks(tasks []*proto.AdminTask) error {
	requestBody, err := json.Marshal(tasks)
	if err != nil {
		return err
	}
	addr := sender.targetAddr
	_, err = util.PostToDataNode(requestBody, addr+TaskSendUrl)
	return err
}

func (sender *AdminTaskSender) DelTask(t *proto.AdminTask) {
	sender.Lock()
	defer sender.Unlock()
	_, ok := sender.taskMap[t.ID]
	if !ok {
		return
	}
	delete(sender.taskMap, t.ID)
}

func (sender *AdminTaskSender) PutTask(t *proto.AdminTask) {
	sender.Lock()
	defer sender.Unlock()
	_, ok := sender.taskMap[t.ID]
	if !ok {
		sender.taskMap[t.ID] = t
	}
}

func (sender *AdminTaskSender) getNeedDealTask() (tasks []*proto.AdminTask) {
	tasks = make([]*proto.AdminTask, 0)
	delTasks := make([]*proto.AdminTask, 0)
	sender.Lock()
	defer sender.Unlock()
	for _, task := range sender.taskMap {
		if task.Status == proto.TaskFail || task.Status == proto.TaskSuccess || task.CheckTaskTimeOut() {
			delTasks = append(delTasks, task)
		}
		if !task.CheckTaskNeedRetrySend() {
			continue
		}
		tasks = append(tasks, task)
		if len(tasks) == TaskSendCount {
			break
		}
	}

	for _, delTask := range delTasks {
		delete(sender.taskMap, delTask.ID)
	}

	return
}
