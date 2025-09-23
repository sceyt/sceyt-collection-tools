package cache

import (
	gocache "github.com/patrickmn/go-cache"
	"time"
)

// EventType is a string identifier for event kinds
type EventType string

// Event is a generic event
type Event struct {
	Type EventType
	Data interface{}
}

// Cache is a thread-safe map with event-driven update capability
type Cache interface {
	RegisterHandler(eventType EventType, handler Handler)
	HandleEvent(e *Event)
	Get(key string) (interface{}, bool)
	Set(key string, value interface{}, ttl time.Duration)
	Delete(key string)
}

// Handler is a function that processes an event and updates the cache
type Handler func(c Cache, e *Event)

type cache struct {
	data     *gocache.Cache
	handlers map[EventType]Handler
}

// NewCache creates a new cache
func NewCache() Cache {
	return &cache{
		data:     gocache.New(gocache.NoExpiration, 10*time.Minute),
		handlers: make(map[EventType]Handler),
	}
}

// RegisterHandler binds a handler to an event type
func (c *cache) RegisterHandler(eventType EventType, handler Handler) {
	c.handlers[eventType] = handler
}

// HandleEvent processes an incoming event
func (c *cache) HandleEvent(e *Event) {
	if handler, ok := c.handlers[e.Type]; ok {
		handler(c, e)
	}
}

// Get retrieves a cached value
func (c *cache) Get(key string) (interface{}, bool) {
	v, ok := c.data.Get(key)
	return v, ok
}

// Set stores a value in the cache
func (c *cache) Set(key string, value interface{}, ttl time.Duration) {
	c.data.Set(key, value, ttl)
}

// Delete removes a value from the cache
func (c *cache) Delete(key string) {
	c.data.Delete(key)
}
