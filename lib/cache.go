package lib

import (
	"gametaverse-data-service/schema"
	"log"
	"sync"
)

type Cache struct {
	mux *sync.Mutex
	UA  map[string]map[string][]schema.UserAction
}

func NewCache() *Cache {
	mux := &sync.Mutex{}
	return &Cache{
		UA:  make(map[string]map[string][]schema.UserAction),
		mux: mux,
	}
}

func (c *Cache) Add(key string, ua map[string][]schema.UserAction) {
	c.mux.Lock()
	c.UA[key] = ua
	c.mux.Unlock()
}

func (c *Cache) Get(key string) (map[string][]schema.UserAction, bool) {
	c.mux.Lock()
	res, exists := c.UA[key]
	c.mux.Unlock()
	if exists {
		log.Print("cache hit!! key: " + key)
	}
	return res, exists
}
