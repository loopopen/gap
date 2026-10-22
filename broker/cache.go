package broker

import "sync"

// ClientCache keeps the first successful producer and one consumer per group.
// A failed open is not cached, so the next call can try again.
type ClientCache struct {
	mu        sync.Mutex
	producer  Producer
	consumers map[string]Consumer
}

func NewClientCache() *ClientCache {
	return &ClientCache{consumers: make(map[string]Consumer)}
}

func (c *ClientCache) Producer(create func() (Producer, error)) (Producer, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.producer == nil {
		producer, err := create()
		if err != nil {
			return nil, err
		}
		c.producer = producer
	}
	return c.producer, nil
}

func (c *ClientCache) Consumer(group string, create func() (Consumer, error)) (Consumer, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	consumer, ok := c.consumers[group]
	if !ok {
		var err error
		consumer, err = create()
		if err != nil {
			return nil, err
		}
		c.consumers[group] = consumer
	}
	return consumer, nil
}
