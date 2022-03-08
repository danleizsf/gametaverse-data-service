package lib

import (
	"gametaverse-data-service/schema"
	"log"
	"sync"
)

type Cache struct {
	mux      *sync.Mutex
	UA       map[string]map[string][]schema.UserAction
	UAbyDate map[string][]map[string][]schema.UserAction

	Summary map[string][]schema.Summary
}

func NewCache() *Cache {
	mux := &sync.Mutex{}
	return &Cache{
		UA:       make(map[string]map[string][]schema.UserAction),
		UAbyDate: make(map[string][]map[string][]schema.UserAction),
		Summary:  make(map[string][]schema.Summary),
		mux:      mux,
	}
}

func (c *Cache) AddUA(key string, ua map[string][]schema.UserAction) {
	c.mux.Lock()
	c.UA[key] = ua
	c.mux.Unlock()
}

func (c *Cache) GetUA(key string) (map[string][]schema.UserAction, bool) {
	c.mux.Lock()
	res, exists := c.UA[key]
	c.mux.Unlock()
	if exists {
		log.Print("cache hit!! key: " + key)
	}
	return res, exists
}

func (c *Cache) AddUAByDate(key string, ua []map[string][]schema.UserAction) {
	c.mux.Lock()
	c.UAbyDate[key] = ua
	c.mux.Unlock()
}

func (c *Cache) GetUAByDate(key string) ([]map[string][]schema.UserAction, bool) {
	c.mux.Lock()
	res, exists := c.UAbyDate[key]
	c.mux.Unlock()
	if exists {
		log.Print("cache hit!! key: " + key)
	}
	return res, exists
}

func (c *Cache) AddSummary(key string, s []schema.Summary) {
	c.mux.Lock()
	c.Summary[key] = s
	c.mux.Unlock()
}

func (c *Cache) GetSummary(key string) ([]schema.Summary, bool) {
	c.mux.Lock()
	res, exists := c.Summary[key]
	c.mux.Unlock()
	if exists {
		log.Print("cache hit!! key: " + key)
	}
	return res, exists
}
