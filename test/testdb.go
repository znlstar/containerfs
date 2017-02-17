package main

import (
	"fmt"
	"strconv"
	//"time"
	"database/sql"
	"sort"
	"strings"
	//"os"
	"ipd.org/containerfs/logger"
	_ "ipd.org/containerfs/volmgr/mysql"
)

func init() {
	logger.SetConsole(true)
	logger.SetRollingFile("/tmp/", "test.log", 10, 5, logger.MB)
	logger.SetLevel(logger.DEBUG)

}

func insert(db *sql.DB) {
	//_,err := db.Exec("lock tables info write")

	stmt, err := db.Prepare("INSERT INTO info(name, age) VALUES(?, ?)")
	defer stmt.Close()

	if err != nil {
		//log.Println(err)
		return
	}

	for i := 0; i < 100; i++ {
		if i < 25 {
			stmt.Exec("jls", strconv.Itoa(i))
		} else if i < 50 {
			stmt.Exec("zxc", strconv.Itoa(i))
		} else if i < 75 {
			stmt.Exec("hjf", strconv.Itoa(i))
		} else {
			stmt.Exec("wxg", strconv.Itoa(i))
		}
	}
	//db.Exec("unlock tables")
}

func main() {
	//var ss string
	//for i :=10; i < 25; i++{

	//ss += strconv.Itoa(i)+","
	//log.Printf("====== str:%s ====", ss)
	//}
	//uuid, err := utils.GenUUID()
	//log.Printf("====== uuid:%s ====", uuid)
	var ss string
	ss = "5,6,7,"
	ll := strings.Split(ss, ",")
	logger.Debug("=====xxx test log == before str:%s == end string:%v", ss, ll)

	j1, err := strconv.Atoi(ll[0])
	j2, err := strconv.Atoi(ll[1])
	j3, err := strconv.Atoi(ll[2])
	logger.Debug("= j1:%d == j2:%d == j3:%d ==", j1, j2, j3)
	intList := []int{2, 4, 3, 5, 7, 6, 9, 8, 1, 0}
	sort.Ints(intList)
	fmt.Printf("%v %d %d\n", intList, intList[0], intList[len(intList)-1])
	return
	db, err := sql.Open("mysql", "root:root@tcp(127.0.0.1:3306)/jls?charset=utf8")

	if err != nil {
		panic(err)
	}
	defer db.Close()

	err = db.Ping()
	if err != nil {
		panic(err)
	}

	insert(db)
	count := 1000 / 10
	logger.Debug("xxxx count:", count)
}
