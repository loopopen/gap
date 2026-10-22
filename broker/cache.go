package broker

import "sync"

// ClientCache keeps one successful producer and one successful consumer per
// configuration key. A failed open is not cached, so the next call can try again.
type ClientCache struct {
	mu        sync.Mutex
	producers map[string]Producer
	consumers map[string]Consumer
}

func NewClientCache() *ClientCache {
	return &ClientCache{
		producers: make(map[string]Producer),
		consumers: make(map[string]Consumer),
	}
}

func (c *ClientCache) Producer(key string, create func() (Producer, error)) (Producer, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if producer, ok := c.producers[key]; ok {
		return producer, nil
	}
	producer, err := create()
	if err != nil {
		return nil, err
	}
	c.producers[key] = producer
	return producer, nil
}

func (c *ClientCache) Consumer(key string, create func() (Consumer, error)) (Consumer, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if consumer, ok := c.consumers[key]; ok {
		return consumer, nil
	}
	consumer, err := create()
	if err != nil {
		return nil, err
	}
	c.consumers[key] = consumer
	return consumer, nil
}
