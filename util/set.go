package util

import (
	"sync"
)

type Set struct {
	sync.RWMutex
	m map[int]struct{}
}

func NewSet() *Set {
	return &Set{
		m: make(map[int]struct{}),
	}
}

func (s *Set) Add(key int) {
	s.Lock()
	defer s.Unlock()
	s.m[key] = struct{}{}
}

func (s *Set) Remove(key int) {
	s.Lock()
	defer s.Unlock()
	delete(s.m, key)
}

func (s *Set) Has(key int) bool {
	s.RLock()
	defer s.RUnlock()
	_, ok := s.m[key]
	return ok
}

func (s *Set) List() []int {
	list := make([]int, 0)
	s.RLock()
	defer s.RUnlock()
	for key := range s.m {
		list = append(list, key)
	}
	return list
}

func (s *Set) Len() int {
	s.RLock()
	defer s.RUnlock()
	return len(s.m)
}
