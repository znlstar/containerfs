package utils

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"strings"
)

type ReqHadesBody struct {
	Type string   `json:"type"`
	Ips  []string `json:"ips"`
	//Ports []int    `json:"ports"`
}

func PostHades(url string, ip string, port int) {
	var r ReqHadesBody
	client := &http.Client{}
	r.Type = "A"
	r.Ips = append(r.Ips, ip)
	//r.Ports = append(r.Ports, port)

	rbody, err := json.Marshal(r)

	req, err := http.NewRequest("POST", url, strings.NewReader(string(rbody)))
	if err != nil {
		// handle error
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("charset", "UTF-8")

	resp, err := client.Do(req)

	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		fmt.Printf("=== Post hades new domain respbody:%v = error:%v ===\n", body, err)
	}

	fmt.Printf("=== Post hades new domain(%v:%v) respbody:%v ===\n", ip, port, string(body))
}

func DelHades(url string) {
	client := &http.Client{}

	req, err := http.NewRequest("DELETE", url, nil)
	if err != nil {
		// handle error
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("charset", "UTF-8")
	resp, err := client.Do(req)
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		fmt.Printf("=== Del hades new domain respbody:%v = error:%v ===\n", body, err)
	}

	fmt.Printf("=== Del hades domain respbody:%v ===\n", string(body))
}
