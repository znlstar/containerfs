package main

//import "github.com/chasex/redis-go-cluster"
import "github.com/go-redis/redis"
import "time"
import "fmt"
import "sync"
import (
	"os"
)

//import "strconv"

var RedisClient *redis.ClusterClient

var wg sync.WaitGroup

const (
	str256 = "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"
)

func put() {
	RedisClient.Set("key", str256, 0).Err()
	wg.Done()

}

func get() {
	RedisClient.Get("key").Err()
	wg.Done()

}

func main() {

	RedisClient = redis.NewClusterClient(&redis.ClusterOptions{
		Addrs: []string{"172.22.189.48:7000", "172.22.189.48:7001", "172.22.189.49:7002", "172.22.189.49:7003", "172.22.189.50:7004", "172.22.189.50:7005"},
	})

	if RedisClient == nil {
		fmt.Println("connect redis failed...")
		os.Exit(1)
	}

	t1 := time.Now()

	for i := 0; i < 20000; i++ {
		wg.Add(1)
		go put()
	}
	t2 := time.Now()

	for i := 0; i < 20000; i++ {
		wg.Add(1)
		go get()
	}
	t3 := time.Now()

	result1 := fmt.Sprintf("d1 : %v\n", t2.Sub(t1))
	result2 := fmt.Sprintf("d2 : %v\n", t3.Sub(t2))

	print(result1)
	print(result2)

}
