package storage

import (
	"encoding/binary"
	"github.com/tiglabs/action_dev/uti"
	"hash/crc32"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
)

type Chunk struct {
	file        *os.File
	tree        *ObjectTree
	lastOid     uint64
	syncLastOid uint64
	commitLock  sync.RWMutex
	compactLock util.TryMutex
}

type ChunkInfo struct {
	ChunkId int
	LastOid uint64
}

func NewChunk(dataDir string, chunkId int) (c *Chunk, err error) {
	c = new(Chunk)
	name := dataDir + "/" + strconv.Itoa(chunkId)
	maxOid, err := c.loadTree(name)
	if err != nil {
		return nil, err
	}

	c.storeLastOid(maxOid)
	return c, nil
}

func (c *Chunk) applyDelObjects(objects []uint64) (err error) {
	for _, needle := range objects {
		c.tree.delete(needle)
	}

	c.storeSyncLastOid(c.loadLastOid())
	return
}

func (c *Chunk) loadTree(name string) (maxOid uint64, err error) {
	if c.file, err = os.OpenFile(name, ChunkOpenOpt, 0666); err != nil {
		return
	}

	var idxFile *os.File
	idxName := name + ".idx"
	if idxFile, err = os.OpenFile(idxName, ChunkOpenOpt, 0666); err != nil {
		c.file.Close()
		return
	}

	tree := NewObjectTree(idxFile)
	if maxOid, err = tree.Load(); err == nil {
		c.tree = tree
	} else {
		idxFile.Close()
		c.file.Close()
	}

	return
}

// returns count of valid objects calculated for CRC
func (c *Chunk) getCheckSum() (fullCRC uint32, syncLastOid uint64, count int) {
	syncLastOid = c.loadSyncLastOid()
	if syncLastOid == 0 {
		syncLastOid = c.loadLastOid()
	}

	c.tree.idxFile.Sync()
	crcBuffer := make([]byte, 0)
	buf := make([]byte, 4)
	c.commitLock.RLock()
	WalkIndexFile(c.tree.idxFile, func(oid uint64, offset, size, crc uint32) error {
		if oid > syncLastOid {
			return nil
		}
		o, ok := c.tree.get(oid)
		if !ok {
			return nil
		}
		if !o.Check(offset, size, crc) {
			return nil
		}
		binary.BigEndian.PutUint32(buf, o.Crc)
		crcBuffer = append(crcBuffer, buf...)
		count++
		return nil
	})
	c.commitLock.RUnlock()

	fullCRC = crc32.ChecksumIEEE(crcBuffer)
	return
}

func (c *Chunk) loadLastOid() uint64 {
	return atomic.LoadUint64(&c.lastOid)
}

func (c *Chunk) storeLastOid(val uint64) {
	atomic.StoreUint64(&c.lastOid, val)
	return
}

func (c *Chunk) incLastOid() uint64 {
	return atomic.AddUint64(&c.lastOid, uint64(1))
}

func (c *Chunk) loadSyncLastOid() uint64 {
	return atomic.LoadUint64(&c.syncLastOid)
}

func (c *Chunk) storeSyncLastOid(val uint64) {
	atomic.StoreUint64(&c.syncLastOid, val)
	return
}

func (c *Chunk) doCompact() (err error) {
	var (
		newIdxFile, newDatFile *os.File
		tree                   *ObjectTree
	)

	name := c.file.Name()
	newIdxName := name + ".cpx"
	newDatName := name + ".cpd"

	if newIdxFile, err = os.OpenFile(newIdxName, ChunkOpenOpt|os.O_TRUNC, 0644); err != nil {
		return err
	}
	defer newIdxFile.Close()

	if newDatFile, err = os.OpenFile(newDatName, ChunkOpenOpt|os.O_TRUNC, 0644); err != nil {
		return err
	}
	defer newDatFile.Close()

	tree = NewObjectTree(newIdxFile)

	if err = c.copyValidData(tree, newDatFile); err != nil {
		return err
	}

	return nil
}

func (c *Chunk) copyValidData(dstNm *ObjectTree, dstDatFile *os.File) (err error) {
	srcNm := c.tree
	srcDatFile := c.file
	srcIdxFile := srcNm.idxFile
	deletedSet := make(map[uint64]struct{})
	_, err = WalkIndexFile(srcIdxFile, func(oid uint64, offset, size, crc uint32) error {
		var (
			o *Object
			e error

			newOffset int64
		)

		_, ok := deletedSet[oid]
		if size == TombstoneFileSize && !ok {
			o = &Object{Oid: oid, Offset: offset, Size: size, Crc: crc}
			if e = dstNm.appendToIdxFile(o); e != nil {
				return e
			}
			deletedSet[oid] = struct{}{}
			return nil
		}

		o, ok = srcNm.get(oid)
		if !ok {
			return nil
		}

		if !o.Check(offset, size, crc) {
			return nil
		}

		realsize := o.Size
		if newOffset, e = dstDatFile.Seek(0, 2); e != nil {
			return e
		}

		dataInFile := make([]byte, realsize)
		if _, e = srcDatFile.ReadAt(dataInFile, int64(o.Offset)); e != nil {
			return e
		}

		if _, e = dstDatFile.Write(dataInFile); e != nil {
			return e
		}

		o.Offset = uint32(newOffset)
		if e = dstNm.appendToIdxFile(o); e != nil {
			return e
		}

		return nil
	})

	return err
}

func (c *Chunk) doCommit() (err error) {
	name := c.file.Name()
	c.tree.idxFile.Close()
	c.file.Close()

	err = os.Rename(name+".cpd", name)
	if err != nil {
		return
	}
	err = os.Rename(name+".cpx", name+".idx")
	if err != nil {
		return
	}

	maxOid, err := c.loadTree(name)
	if err == nil && maxOid > c.loadLastOid() {
		c.storeLastOid(maxOid)
	}
	return err
}
