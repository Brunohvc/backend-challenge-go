package sqsin

import "time"

func (c *Consumer) SetRetryTiming(ping, receive time.Duration) {
	c.pingEvery, c.receiveRetry = ping, receive
}

func (c *Consumer) HeldMessages() int {
	c.held.mu.Lock()
	defer c.held.mu.Unlock()
	return len(c.held.msgs)
}
