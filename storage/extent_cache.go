package storage

import "container/list"

func (s *ExtentStore) addExtentToCache(ec *Extent) {
	s.lock.Lock()
	s.extents[ec.key] = ec
	ec.element = s.fdlist.PushBack(ec)
	s.lock.Unlock()
}

func (s *ExtentStore) delExtentFromCache(ec *Extent) {
	s.lock.Lock()
	delete(s.extents, ec.key)
	s.fdlist.Remove(ec.element)
	ec.closeExtent()
	s.lock.Unlock()
}

func (s *ExtentStore) getExtentFromCache(key uint32) (ec *Extent, ok bool) {
	s.lock.Lock()
	if ec, ok = s.extents[key]; ok {
		s.fdlist.MoveToBack(ec.element)
	}
	s.lock.Unlock()

	return
}

func (s *ExtentStore) ClearAllCache() {
	s.lock.Lock()
	defer s.lock.Unlock()
	for e := s.fdlist.Front(); e != nil; {
		curr := e
		e = e.Next()
		ec := curr.Value.(*Extent)
		delete(s.extents, ec.key)
		ec.closeExtent()
		s.fdlist.Remove(curr)
	}
	s.fdlist = list.New()
	s.extents = make(map[uint32]*Extent)
}

func (s *ExtentStore) GetStoreActiveFiles() (activeFiles int) {
	s.lock.Lock()
	activeFiles = s.fdlist.Len()
	s.lock.Unlock()

	return
}

func (s *ExtentStore) CloseStoreActiveFiles() {
	s.lock.Lock()
	defer s.lock.Unlock()
	needClose := s.fdlist.Len() / 2
	for i := 0; i < needClose; i++ {
		if e := s.fdlist.Front(); e != nil {
			front := e.Value.(*Extent)
			delete(s.extents, front.key)
			s.fdlist.Remove(front.element)
			front.closeExtent()
		}
	}
}
