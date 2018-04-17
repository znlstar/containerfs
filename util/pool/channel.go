package pool

import (
	"fmt"
	"sync"
	"time"
)

type PoolConfig struct {
	Para        interface{}
	InitialCap  int
	MaxCap      int
	Factory     func(para interface{}) (interface{}, error)
	Close       func(interface{}) error
	IdleTimeout time.Duration
}

type ChannelPool struct {
	para        interface{}
	mu          sync.Mutex
	conns       chan *IdleConn
	factory     func(para interface{}) (interface{}, error)
	close       func(interface{}) error
	idleTimeout time.Duration
}

type IdleConn struct {
	conn interface{}
	t    time.Time
}

func NewChannelPool(poolConfig *PoolConfig) (*ChannelPool, error) {
	c := &ChannelPool{
		conns:       make(chan *IdleConn, poolConfig.MaxCap),
		factory:     poolConfig.Factory,
		close:       poolConfig.Close,
		idleTimeout: poolConfig.IdleTimeout,
		para:        poolConfig.Para,
	}

	for i := 0; i < poolConfig.InitialCap; i++ {
		conn, err := c.factory(poolConfig.Para)
		if err != nil {
			c.Release()
			return nil, fmt.Errorf("factory is not able to fill the pool: %s", err)
		}
		c.conns <- &IdleConn{conn: conn, t: time.Now()}
	}

	return c, nil
}

func (c *ChannelPool) getConns() chan *IdleConn {
	c.mu.Lock()
	conns := c.conns
	c.mu.Unlock()
	return conns
}

func (c *ChannelPool) Get() (interface{}, error) {
	conns := c.getConns()
	if conns == nil {
		return nil, ErrClosed
	}
	for {
		select {
		case wrapConn := <-conns:
			if wrapConn == nil {
				return c.factory(c.para)
			}
			if timeout := c.idleTimeout; timeout > 0 {
				if wrapConn.t.Add(timeout).Before(time.Now()) {
					c.Close(wrapConn.conn)
					continue
				}
			}
			return wrapConn.conn, nil
		default:
			conn, err := c.factory(c.para)
			if err != nil {
				return nil, err
			}

			return conn, nil
		}
	}
}

func (c *ChannelPool) Put(conn interface{}) error {
	if conn == nil {
		return nil
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	if c.conns == nil {
		return c.Close(conn)
	}

	select {
	case c.conns <- &IdleConn{conn: conn, t: time.Now()}:
		return nil
	default:
		return c.Close(conn)
	}
}

func (c *ChannelPool) Close(conn interface{}) error {
	if conn == nil {
		return nil
	}
	return c.close(conn)
}

func (c *ChannelPool) Release() {
	c.mu.Lock()
	conns := c.conns
	c.conns = nil
	c.factory = nil
	closeFun := c.close
	c.close = nil
	c.mu.Unlock()

	if conns == nil {
		return
	}

	close(conns)
	for wrapConn := range conns {
		closeFun(wrapConn.conn)
	}
}

func (c *ChannelPool) Len() int {
	return len(c.getConns())
}
