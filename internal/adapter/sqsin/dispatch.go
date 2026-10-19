package sqsin

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
)

type group struct {
	id   string
	msgs []*heldMessage
}

func groupByMessageGroup(msgs []*heldMessage) []group {
	var groups []group
	index := make(map[string]int)
	for _, h := range msgs {
		id := h.msg.Attributes[string(types.MessageSystemAttributeNameMessageGroupId)]
		i, ok := index[id]
		if !ok {
			i = len(groups)
			index[id] = i
			groups = append(groups, group{id: id})
		}
		groups[i].msgs = append(groups[i].msgs, h)
	}
	return groups
}

func (c *Consumer) dispatch(stopping, work context.Context, r *run, msgs []types.Message) {
	groups := groupByMessageGroup(c.held.hold(msgs))
	for i, g := range groups {
		if i > 0 && !c.acquireSlot(stopping) {
			for _, rest := range groups[i:] {
				c.giveBack(work, rest.msgs)
			}
			return
		}
		r.groups.Add(1)
		go func() {
			defer r.groups.Done()
			defer c.releaseSlot()
			c.runGroup(stopping, work, g)
		}()
	}
}

func (c *Consumer) runGroup(stopping, work context.Context, g group) {
	for i, h := range g.msgs {
		if stopping.Err() != nil || c.unavailable.Load() {
			c.giveBack(work, g.msgs[i:])
			return
		}
		if !c.process(work, g.id, h) {
			c.giveBack(work, g.msgs[i+1:])
			return
		}
	}
}

func (c *Consumer) giveBack(ctx context.Context, msgs []*heldMessage) {
	for _, h := range msgs {
		c.held.settle(h)
		if err := callSQS(ctx, func(ctx context.Context) error { return c.changeVisibility(ctx, h.msg, 0) }); err != nil {
			c.log.WarnContext(ctx, "could not give a message back; it returns when its visibility runs out",
				"sqsMessageId", aws.ToString(h.msg.MessageId), "error", err)
			continue
		}
		c.log.DebugContext(ctx, "message given back unprocessed", "sqsMessageId", aws.ToString(h.msg.MessageId))
	}
}

func (c *Consumer) acquireSlot(ctx context.Context) bool {
	if ctx.Err() != nil {
		return false
	}
	select {
	case c.slots <- struct{}{}:
		return true
	case <-ctx.Done():
		return false
	}
}

func (c *Consumer) releaseSlot() { <-c.slots }
