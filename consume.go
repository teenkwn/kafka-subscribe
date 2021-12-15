package kafka

import (
	"context"
	"fmt"

	"github.com/Shopify/sarama"
	log "github.com/sirupsen/logrus"
)

type (
	consumerGroup struct {
		ctx     context.Context
		topic   Topic
		groupId string
		group   sarama.ConsumerGroup
		handler *consumerGroupHandler
		closed  bool
	}

	consumerGroupHandler struct {
		groupId string
		claimer func([]byte)
	}
)

// ConsumerGroup implementation
// Consumer: start consumer message for configured topics
func (csg *consumerGroup) Consume(claimer func([]byte)) {
	csg.handler.groupId = csg.groupId
	csg.handler.claimer = claimer

	go func() {
		for {
			if csg.closed {
				break
			}

			select {
			case <-csg.ctx.Done():
				err := csg.Close()
				if err != nil {
					log.Error("closing consumer group", err)
				}

			default:
				err := csg.group.Consume(csg.ctx, []string{csg.topic.Name()}, csg.handler)
				if err == sarama.ErrClusterAuthorizationFailed {
					err := csg.Close()
					if err != nil {
						log.Error("closing consumer group", err)
					}
				}
			}
		}
	}()
}

func (csg *consumerGroup) Close() error {
	csg.closed = true
	return csg.group.Close()
}

func (csg *consumerGroup) String() string {
	return fmt.Sprintf(
		"groupId %v: listenting for: %v",
		csg.groupId,
		csg.topic.Name(),
	)
}

// ConsumerGroupHandler implementation
func (consumerGroupHandler) Setup(_ sarama.ConsumerGroupSession) error {
	return nil
}

func (consumerGroupHandler) Cleanup(sess sarama.ConsumerGroupSession) error {
	return nil
}

func (h consumerGroupHandler) ConsumeClaim(sess sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for msg := range claim.Messages() {
		log.Info("---> Message topic:%q partition:%d offset:%d\n", msg.Topic, msg.Partition, msg.Offset)

		h.claimer(msg.Value)
		sess.MarkMessage(msg, "")
	}
	return nil
}
