package storage

import (
	"container/list"
	"os"
	"sync"
)

type Extent struct {
	file      *os.File
	name      string
	key       uint32
	lock      sync.RWMutex
	blocksCrc []byte
	element   *list.Element
}

func NewExtentInCore(name string, keyId uint32) (e *Extent) {
	e = new(Extent)
	e.key = keyId
	e.name = name
	e.blocksCrc = make([]byte, BlockCrcHeaderSize)

	return
}

func (e *Extent) readlock() {
	e.lock.RLock()
}

func (e *Extent) readUnlock() {
	e.lock.RUnlock()
}

func (e *Extent) writelock() {
	e.lock.Lock()
}

func (e *Extent) writeUnlock() {
	e.lock.Unlock()
}

func (e *Extent) closeExtent() (err error) {
	e.writelock()
	_, err = e.file.WriteAt(e.blocksCrc, 0)
	err = e.file.Close()
	e.writeUnlock()

	return
}

func (e *Extent) deleteExtent() (err error) {
	e.writelock()
	err = os.Remove(e.name)
	e.writeUnlock()

	return
}
