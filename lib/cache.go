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
	log.Print("Add UA cache key: " + key)
	c.mux.Unlock()
}

func (c *Cache) GetUA(key string) (map[string][]schema.UserAction, bool) {
	c.mux.Lock()
	res, exists := c.UA[key]
	c.mux.Unlock()
	if exists {
		log.Print("UA cache hit! key: " + key)
	} else {
		log.Print("UA cache miss! key: " + key)
	}
	return res, exists
}

func (c *Cache) AddUAByDate(key string, ua []map[string][]schema.UserAction) {
	c.mux.Lock()
	c.UAbyDate[key] = ua
	log.Print("Add UAByDate cache key: " + key)
	c.mux.Unlock()
}

func (c *Cache) GetUAByDate(key string) ([]map[string][]schema.UserAction, bool) {
	c.mux.Lock()
	res, exists := c.UAbyDate[key]
	c.mux.Unlock()
	if exists {
		log.Print("UAByDate cache hit! key: " + key)
	} else {
		log.Print("UAByDate cache miss! key: " + key)
	}
	return res, exists
}

func (c *Cache) AddSummary(key string, s []schema.Summary) {
	c.mux.Lock()
	c.Summary[key] = s
	log.Print("Add Summary cache key: " + key)
	c.mux.Unlock()
}

func (c *Cache) GetSummary(key string) ([]schema.Summary, bool) {
	c.mux.Lock()
	res, exists := c.Summary[key]
	c.mux.Unlock()
	if exists {
		log.Print("Summary cache hit! key: " + key)
	} else {
		log.Print("Summary cache miss! key: " + key)
	}
	return res, exists
}
