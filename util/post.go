package util

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"net/http"
	"strconv"
	"github.com/tiglabs/containerfs/util/log"
)

const (
	TaskWaitResponseTimeOut = 2
)

func PostToDataNode(data []byte, url string) (msg []byte, err error) {
	log.GetLog().LogDebug(fmt.Sprintf("action[PostToDataNode],url:%v,send data:%v", url, string(data)))
	client := &http.Client{Timeout: TaskWaitResponseTimeOut}
	buff := bytes.NewBuffer(data)
	req, err := http.NewRequest("POST", url, buff)
	if err != nil {
		log.GetLog().LogError(fmt.Sprintf("action[PostToDataNode],url:%v,err:%v", url, err.Error()))
		return
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Connection", "close")
	resp, err := client.Do(req)

	if err != nil {
		log.GetLog().LogError(fmt.Sprintf("action[PostToDataNode],url:%v, err:%v", url, err.Error()))
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		err = fmt.Errorf(" action[PostToDataNode] Data send failed,url:%v, status code:%v ", url, strconv.Itoa(resp.StatusCode))
		return nil, err
	}
	msg, _ = ioutil.ReadAll(resp.Body)
	resp.Body.Close()

	return msg, nil
}
