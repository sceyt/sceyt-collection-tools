package eventcache

import (
	"encoding/json"
	"time"

	"github.com/bluele/gcache"
	"github.com/google/uuid"
	messageQueue "github.com/sceyt/sceyt-amqp"
	"github.com/sirupsen/logrus"
)

// LoaderFunc used for gcache LoaderFunc through a thin adapter.
type LoaderFunc func(key string) (interface{}, error)

// KeyDeriver maps a message (route/body/headers) to a list of cache keys to invalidate.
type KeyDeriver func(route string, body []byte, headers map[string]interface{}) []string

// Cache is a minimal wrapper around gcache with Rabbit-driven invalidation and reconnection handling.
type Cache struct {
	gc         gcache.Cache
	rabbit     *messageQueue.RabbitQueue
	exchange   string
	bindKeys   []string
	deriveKeys KeyDeriver
}

// New creates a new cache.
// count is the max size of gcache ARC, loader is optional.
// If rabbit is not nil, the cache will subscribe and invalidate on updates.
func New(count int, loader LoaderFunc, rabbit *messageQueue.RabbitQueue, exchange string, bindKeys []string, deriveKeys KeyDeriver) *Cache {
	c := &Cache{
		rabbit:     rabbit,
		exchange:   exchange,
		bindKeys:   bindKeys,
		deriveKeys: deriveKeys,
	}

	gc := gcache.New(count).
		ARC().
		Expiration(10 * time.Minute)
	if loader != nil {
		gc = gc.LoaderFunc(func(key interface{}) (interface{}, error) {
			return loader(key.(string))
		})
	}
	c.gc = gc.Build()

	if c.rabbit != nil && c.exchange != "" && len(c.bindKeys) > 0 && c.deriveKeys != nil {
		go c.subscribe()
		go c.watchRabbitMQConnection()
	}

	return c
}

// Get returns value from cache (using LoaderFunc on miss if provided)
func (c *Cache) Get(key string) (interface{}, error) {
	return c.gc.Get(key)
}

// Set sets a value manually (bypassing Loader).
func (c *Cache) Set(key string, value interface{}) error {
	return c.gc.Set(key, value)
}

// Remove deletes a key.
func (c *Cache) Remove(key string) {
	c.gc.Remove(key)
}

// Purge clears the cache.
func (c *Cache) Purge() {
	c.gc.Purge()
}

func (c *Cache) subscribe() {
	channel, err := c.rabbit.NewChannel()
	if err != nil {
		logrus.WithError(err).Fatal("eventcache: failed to open channel")
	}
	defer channel.Close()

	qname := "eventcache-" + uuid.New().String()
	queue, err := channel.QueueDeclare(qname, false, true, false, false, nil)
	if err != nil {
		logrus.WithError(err).Fatal("eventcache: failed to declare queue")
	}

	for _, rk := range c.bindKeys {
		if err := channel.QueueBind(queue.Name, rk, c.exchange, false, nil); err != nil {
			logrus.WithError(err).Fatal("eventcache: failed to bind queue")
		}
	}

	msgs, err := channel.Consume(queue.Name, "", false, false, false, false, nil)
	if err != nil {
		logrus.WithError(err).Fatal("eventcache: failed to consume")
	}

	logrus.Infof("[*] eventcache subscribed to exchange=%s keys=%v", c.exchange, c.bindKeys)
	for d := range msgs {
		var headers map[string]interface{}
		if d.Headers != nil {
			headers = d.Headers
		} else {
			headers = map[string]interface{}{}
		}
		keys := c.deriveKeys(d.RoutingKey, d.Body, headers)
		for _, k := range keys {
			c.gc.Remove(k)
		}
		d.Ack(false)
	}
}

func (c *Cache) watchRabbitMQConnection() {
	receiver := make(chan string)
	c.rabbit.NotifyState(receiver)
	for {
		state := <-receiver
		if state == "closed" {
			logrus.Error("eventcache, RabbitMQ connection closed")
		} else if state == "open" {
			logrus.Info("eventcache, RabbitMQ connection opened - purging cache and resubscribing")
			c.gc.Purge()
			go c.subscribe()
		}
	}
}

// Common helper to derive keys from JSON body by field name.
func DeriveKeysFromJSONField(field string) KeyDeriver {
	return func(route string, body []byte, headers map[string]interface{}) []string {
		m := map[string]string{}
		if err := json.Unmarshal(body, &m); err == nil {
			if v, ok := m[field]; ok && v != "" {
				return []string{v}
			}
		}
		return nil
	}
}
