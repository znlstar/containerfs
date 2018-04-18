package pool

import (
	"net"
	"strings"
	"sync"
	"time"
)

type ConnPool struct {
	sync.Mutex
	pools map[string]*ChannelPool
}

func NewConnPool() (connP *ConnPool) {
	return &ConnPool{pools: make(map[string]*ChannelPool)}
}

func (connP *ConnPool) Get(addr string) (c net.Conn, err error) {
	var obj interface{}
	connP.Lock()
	pool, ok := connP.pools[addr]
	connP.Unlock()
	factory := func(addr interface{}) (interface{}, error) {
		return net.DialTimeout("tcp", addr.(string), time.Second)
	}
	close := func(v interface{}) error { return v.(net.Conn).Close() }
	if !ok {
		poolConfig := &PoolConfig{
			InitialCap:  5,
			MaxCap:      300,
			Factory:     factory,
			Close:       close,
			IdleTimeout: time.Minute * 30,
		}
		pool, err = NewChannelPool(poolConfig)
		if err != nil {
			return net.DialTimeout("tcp", addr, time.Second)
		}
	}

	if obj, err = pool.Get(); err != nil {
		return net.DialTimeout("tcp", addr, time.Second)
	}
	c = obj.(net.Conn)

	return
}

func (connP *ConnPool) Put(c net.Conn) {
	addr := strings.Split(c.LocalAddr().String(), ":")[0]
	connP.Lock()
	pool, ok := connP.pools[addr]
	connP.Unlock()
	if !ok {
		c.Close()
		return
	}
	pool.Put(c)

	return
}
