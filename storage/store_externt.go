package storage

import (
	"container/list"
	"encoding/binary"
	"fmt"
	"github.com/juju/errors"
	"hash/crc32"
	"io"
	"io/ioutil"
	"os"
	"strconv"
	"strings"
	"sync"
	"syscall"
)

var (
	ExtentOpenOpt = os.O_CREATE | os.O_RDWR | os.O_EXCL
)

const (
	BlockCrcHeaderSize = 4097
	BlockCount         = 1024
	MarkDelete         = 'D'
	UnMarkDelete       = 'U'
	MarkDeleteIndex    = 4096
	BlockSize          = 65536
	PerBlockCrcSize    = 4
)

type ExtentStore struct {
	dataDir    string
	lock       sync.Mutex
	extents    map[uint32]*Extent
	fdlist     *list.List
	baseFileId uint32
}

func NewExtentStore(dataDir string, newMode bool) (s *ExtentStore, err error) {
	s = new(ExtentStore)
	s.dataDir = dataDir
	if err = CheckAndCreateSubdir(dataDir, newMode); err != nil {
		return nil, fmt.Errorf("NewExtentStore [%v] err[%v]", dataDir, err)
	}

	s.extents = make(map[uint32]*Extent)
	s.fdlist = list.New()
	if err = s.initBaseFileId(); err != nil {
		return nil, fmt.Errorf("NewExtentStore [%v] err[%v]", dataDir, err)
	}

	return
}

func (s *ExtentStore) DeleteStore() {
	s.ClearAllCache()
	os.RemoveAll(s.dataDir)

	return
}

func (s *ExtentStore) Create(fileId uint32) (err error) {
	var e *Extent
	emptyCrc := crc32.ChecksumIEEE(make([]byte, BlockSize))
	if e, err = s.createExtent(fileId); err != nil {
		return
	}

	for blockNo := 0; blockNo < BlockCount; blockNo++ {
		binary.BigEndian.PutUint32(e.blocksCrc[blockNo*PerBlockCrcSize:(blockNo+1)*PerBlockCrcSize], emptyCrc)
	}

	if _, err = e.file.WriteAt(e.blocksCrc, 0); err != nil {
		return
	}
	if err = e.file.Sync(); err != nil {
		return
	}
	s.addExtentToCache(e)

	return
}

func (s *ExtentStore) createExtent(fileId uint32) (e *Extent, err error) {
	name := s.dataDir + "/" + strconv.Itoa((int)(fileId))

	e = NewExtentInCore(name, fileId)
	if e.file, err = os.OpenFile(e.name, ExtentOpenOpt, 0666); err != nil {
		return nil, err
	}
	if err = os.Truncate(name, BlockCrcHeaderSize); err != nil {
		return nil, err
	}

	return
}

func (s *ExtentStore) getExtent(fileId uint32) (e *Extent, err error) {
	var ok bool
	if e, ok = s.getExtentFromCache(fileId); !ok {
		e, err = s.loadExtentFromDisk(fileId)
	}

	return e, err
}

func (s *ExtentStore) loadExtentFromDisk(fileId uint32) (e *Extent, err error) {
	name := s.dataDir + "/" + strconv.Itoa((int)(fileId))
	e = NewExtentInCore(name, fileId)
	if err = s.openExtentFromDisk(e); err == nil {
		s.addExtentToCache(e)
	}

	return
}

func (s *ExtentStore) initBaseFileId() error {
	var maxFileId int
	files, err := ioutil.ReadDir(s.dataDir)
	if err != nil {
		return err
	}

	for _, f := range files {
		fileId, err := strconv.Atoi(f.Name())
		if err != nil {
			continue
		}
		if fileId >= maxFileId {
			maxFileId = fileId
		}
	}
	s.baseFileId = (uint32)(maxFileId)

	return nil
}

func (s *ExtentStore) openExtentFromDisk(e *Extent) (err error) {
	e.writelock()
	defer e.writeUnlock()

	if e.file, err = os.OpenFile(e.name, os.O_RDWR|os.O_EXCL, 0666); err != nil {
		if strings.Contains(err.Error(), syscall.ENOENT.Error()) {
			err = ErrorChunkNotFound
		}
		return err
	}
	if _, err = e.file.ReadAt(e.blocksCrc, 0); err != nil {
		return
	}

	return
}

func (s *ExtentStore) Write(fileId uint32, offset, size int64, data []byte, crc uint32) (err error) {
	var e *Extent
	if e, err = s.getExtent(fileId); err != nil {
		return
	}
	if err = s.checkOffsetAndSize(offset, size); err != nil {
		return
	}
	if e.blocksCrc[MarkDeleteIndex] == MarkDelete {
		err = ErrorHasDelete
		return
	}

	e.readlock()
	defer e.readUnlock()
	if _, err = e.file.WriteAt(data[:size], offset+BlockCrcHeaderSize); err != nil {
		return
	}

	blockNo := offset / BlockSize
	binary.BigEndian.PutUint32(e.blocksCrc[blockNo*PerBlockCrcSize:(blockNo+1)*PerBlockCrcSize], crc)
	if _, err = e.file.WriteAt(e.blocksCrc[blockNo*PerBlockCrcSize:(blockNo+1)*PerBlockCrcSize], blockNo*PerBlockCrcSize); err != nil {
		return
	}

	return
}

func (s *ExtentStore) checkOffsetAndSize(offset, size int64) error {
	if offset+size > BlockSize*BlockCount {
		return ErrorUnmatchPara
	}
	if offset >= BlockCount*BlockSize || size == 0 {
		return ErrorUnmatchPara
	}

	offsetInBlock := offset % BlockSize
	if offsetInBlock+size > BlockSize || offsetInBlock != 0 {
		return ErrorUnmatchPara
	}

	return nil
}

func (s *ExtentStore) Read(fileId uint32, offset, size int64, nbuf []byte) (crc uint32, err error) {
	var e *Extent
	if e, err = s.getExtent(fileId); err != nil {
		return
	}
	if err = s.checkOffsetAndSize(offset, size); err != nil {
		return
	}
	if e.blocksCrc[MarkDeleteIndex] == MarkDelete {
		err = ErrorHasDelete
		return
	}

	e.readlock()
	defer e.readUnlock()
	if _, err = e.file.ReadAt(nbuf[:size], offset+BlockCrcHeaderSize); err != nil {
		return
	}
	blockNo := offset / BlockSize
	crc = binary.BigEndian.Uint32(e.blocksCrc[blockNo*4 : (blockNo+1)*4])

	return
}

func (s *ExtentStore) MarkDelete(fileId uint32, offset, size int64) (err error) {
	var e *Extent
	if e, err = s.getExtent(fileId); err != nil {
		return
	}

	e.readlock()
	defer e.readUnlock()
	e.blocksCrc[MarkDeleteIndex] = MarkDelete
	if _, err = e.file.WriteAt(e.blocksCrc, 0); err != nil {
		return
	}

	return
}

func (s *ExtentStore) Delete(fileId uint32) (err error) {
	var e *Extent
	if e, err = s.getExtent(fileId); err != nil {
		return nil
	}

	s.delExtentFromCache(e)
	if err = e.deleteExtent(); err != nil {
		return nil
	}

	return
}

func (s *ExtentStore) IsMarkDelete(fileId uint32) (isMarkDelete bool, err error) {
	var e *Extent
	if e, err = s.getExtent(fileId); err != nil {
		return
	}
	isMarkDelete = e.blocksCrc[MarkDeleteIndex] == MarkDelete

	return
}

func (s *ExtentStore) Sync(fileId uint32) (err error) {
	var e *Extent
	if e, err = s.getExtent(fileId); err != nil {
		return
	}
	e.readlock()
	defer e.readUnlock()

	return e.file.Sync()
}

func (s *ExtentStore) SyncAll() { /*notici this function must called on program exit or kill */
	s.lock.Lock()
	defer s.lock.Unlock()
	for _, v := range s.extents {
		v.readlock()
		v.file.Sync()
		v.readUnlock()
	}
}

func (s *ExtentStore) GetBlockCrcBuffer(fileId uint32, headerBuff []byte) (err error) {
	var e *Extent
	if e, err = s.getExtent(fileId); err != nil {
		return
	}

	if len(headerBuff) != BlockCrcHeaderSize {
		return errors.New("header buff is not BlockCrcHeaderSize")
	}

	e.readlock()
	_, err = e.file.ReadAt(headerBuff, 0)
	e.readUnlock()

	return
}

func (s *ExtentStore) GetWatermark(fileId uint32) (size int64, err error) {
	var (
		e     *Extent
		finfo os.FileInfo
	)
	if e, err = s.getExtent(fileId); err != nil {
		return
	}
	e.readlock()
	defer e.readUnlock()

	finfo, err = e.file.Stat()
	if err != nil {
		return
	}
	size = finfo.Size() - BlockCrcHeaderSize

	return
}

func (s *ExtentStore) extentExist(fileId uint32) (exist bool) {
	name := s.dataDir + "/" + strconv.Itoa((int)(fileId))
	if _, err := os.Stat(name); err == nil {
		exist = true
		warterMark, err := s.GetWatermark(fileId)
		if err == io.EOF || warterMark < BlockCrcHeaderSize {
			err = s.fillBlockCrcHeader(name, BlockSize)
		}
	}

	return
}

func (s *ExtentStore) fillBlockCrcHeader(name string, blockSize int64) (err error) {
	if fp, err := os.OpenFile(name, os.O_RDWR|os.O_EXCL, 0666); err == nil {
		emptyCrc := crc32.ChecksumIEEE(make([]byte, blockSize))
		extentCrc := make([]byte, BlockCrcHeaderSize)
		for blockNo := 0; blockNo < BlockCount; blockNo++ {
			binary.BigEndian.PutUint32(extentCrc[blockNo*4:(blockNo+1)*4], emptyCrc)
		}
		_, err = fp.WriteAt(extentCrc, 0)
		fp.Close()
	}

	return
}

func (s *ExtentStore) GetStoreFileCount() (files int, err error) {
	var finfos []os.FileInfo

	if finfos, err = ioutil.ReadDir(s.dataDir); err == nil {
		files = len(finfos)
	}

	return
}

func (s *ExtentStore) GetStoreUsedSize() (size int64) {
	if finfoArray, err := ioutil.ReadDir(s.dataDir); err == nil {
		for _, finfo := range finfoArray {
			size += finfo.Size()
		}
	}

	return
}
