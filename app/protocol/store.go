package protocol

import (
	"sync"
	"time"
)

type Store struct {
	m    map[string]string
	lock sync.RWMutex
}

func NewStore() *Store {
	return &Store{
		m:    make(map[string]string),
		lock: sync.RWMutex{},
	}
}

var store *Store = NewStore()

func (store *Store) Set(key, val string) {
	store.lock.Lock()
	defer store.lock.Unlock()
	store.m[key] = val
}

func (store *Store) Get(key string) (string, bool) {
	store.lock.RLock()
	defer store.lock.RUnlock()
	val, ok := store.m[key]
	return val, ok
}

func (store *Store) SetWithTTL(key string, val string, ttl time.Duration) {
	store.Set(key, val)
	go func() {
		time.Sleep(ttl)
		store.lock.Lock()
		defer store.lock.Unlock()
		delete(store.m, key)
	}()
}
