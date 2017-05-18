package main

//import "github.com/chasex/redis-go-cluster"
import "time"
import "fmt"
import "math/rand"

//import "strconv"

func main() {

	/*
		cluster1, _ := redis.NewCluster(
			&redis.Options{
				StartNodes:   []string{"127.0.0.1:7000", "127.0.0.1:7001", "127.0.0.1:7002", "127.0.0.1:7003", "127.0.0.1:7004", "127.0.0.1:7005"},
				ConnTimeout:  1000 * time.Millisecond,
				ReadTimeout:  1000 * time.Millisecond,
				WriteTimeout: 1000 * time.Millisecond,
				KeepAlive:    16,
				AliveTime:    60 * time.Second,
			})
	*/
	/*
		cluster2, _ := redis.NewCluster(
			&redis.Options{
				StartNodes:   []string{"127.0.0.1:7000", "127.0.0.1:7001", "127.0.0.1:7002", "127.0.0.1:7003", "127.0.0.1:7004", "127.0.0.1:7005"},
				ConnTimeout:  1000 * time.Millisecond,
				ReadTimeout:  1000 * time.Millisecond,
				WriteTimeout: 1000 * time.Millisecond,
				KeepAlive:    16,
				AliveTime:    60 * time.Second,
			})
	*/
	var cnt int64
	for cnt < 10000 {
		//for true {

		/*
			reply1, err1 := redis.Int(cluster1.Do("INCR", "mycount"))
			//reply2, err2 := redis.Int(cluster2.Do("INCR", "mycount"))

			fmt.Printf("get reply1:%v,err1:%v\n", reply1, err1)
			//fmt.Printf("get reply2:%v,err2:%v\n", reply2, err2)

			time.Sleep(time.Second)
			cnt++
		*/

		rand.Seed(int64(time.Now().Nanosecond()))
		num := rand.Intn(10)

		fmt.Println(num)

		/*
			val := "bar-" + cnt_str
			_, err := cluster.Do("SET", key, val)
			if err != nil {
				time.Sleep(time.Second * 2)
				_, err = cluster.Do("SET", key, val)
				if err != nil {
					time.Sleep(time.Second * 2)
					_, err = cluster.Do("SET", key, val)
					if err != nil {
						fmt.Printf("set key:%v,val:%v\n failed ", key, val)
						break
					}
				}
			}
			//fmt.Printf("set key:%v,val:%v\n", key, val)
			a, err := redis.String(cluster.Do("GET", "foo"))
			if err != nil {
				time.Sleep(time.Second * 2)
				a, err = redis.String(cluster.Do("GET", "foo"))
				if err != nil {
					time.Sleep(time.Second * 2)
					a, err = redis.String(cluster.Do("GET", "foo"))
					if err != nil {
						fmt.Printf("get failed ... %v\n", err)
						break
					}
				}

			}
			a = a
			//fmt.Printf("get key:%v,val:%v\n", key, a)
			cnt++

		*/

	}
}
