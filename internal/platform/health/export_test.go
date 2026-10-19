package health

import "time"

func (c *Checker) SetCheckTimeout(d time.Duration) {
	c.timeout = d
}
