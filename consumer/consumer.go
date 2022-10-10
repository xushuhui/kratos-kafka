package consumer

import (
	"context"

	"strings"
	"sync"

	kafka2 "github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/go-kratos/kratos/v2/log"
	"github.com/pkg/errors"
	"github.com/xushuhui/kratos-kafka/transport/kafka"
)

type GroupConsumer struct {
	// config setting
	brokers  []string
	topics   []string
	group    string
	ready    chan struct{}
	consumer *kafka2.Consumer
	handlers map[string]kafka.Handler
	logger   *log.Helper
}

// ConsumerOption is a GroupConsumer option.
type GroupConsumerOption func(*GroupConsumer)

// Logger with the specify logger
func Logger(logger log.Logger) GroupConsumerOption {
	return func(c *GroupConsumer) {
		c.logger = log.NewHelper(logger)
	}
}

// NewGroupConsumer inits a consumer group consumer
func NewGroupConsumer(brokers string, topics []string, group string, opts ...GroupConsumerOption) (*GroupConsumer, error) {
	// parse config setting
	result := &GroupConsumer{
		brokers:  strings.Split(brokers, ","),
		topics:   topics,
		group:    group,
		ready:    make(chan struct{}),
		handlers: make(map[string]kafka.Handler),
		logger:   log.NewHelper(log.DefaultLogger),
	}

	kafkaconf := &kafka2.ConfigMap{
		"api.version.request":       "true",
		"auto.offset.reset":         "latest",
		"heartbeat.interval.ms":     3000,
		"session.timeout.ms":        30000,
		"max.poll.interval.ms":      120000,
		"fetch.max.bytes":           1024000,
		"max.partition.fetch.bytes": 256000}
	kafkaconf.SetKey("bootstrap.servers", brokers)
	kafkaconf.SetKey("group.id", group)
	kafkaconf.SetKey("enable.auto.commit", "false")
	kafkaconf.SetKey("security.protocol", "plaintext")
	consumer, err := kafka2.NewConsumer(kafkaconf)
	if err != nil {
		return nil, errors.Wrap(err, "init sarama kafka client error")
	}

	result.consumer = consumer

	return result, nil
}

// Topics returns all the topics this consumer subscribes
func (c *GroupConsumer) Topics() []string {
	return c.topics
}

// RegisterHandler registers a handler to handle the messages of a specific topic
func (c *GroupConsumer) RegisterHandler(handler kafka.Handler) {
	c.handlers[handler.Topic()] = handler
}

// RegisterHandler checks whether this consumer has a handler for the specific topic
func (c *GroupConsumer) HasHandler(topic string) bool {
	_, ok := c.handlers[topic]
	return ok
}

// Consume starts the consumer to receive and handle the messages
func (c *GroupConsumer) Consume(ctx context.Context) error {
	// check handlers before consuming
	for _, topic := range c.topics {
		if _, ok := c.handlers[topic]; !ok {
			return errors.Errorf("no handler for topic %s", topic)
		}
		if err := c.consumer.Subscribe(topic, nil); err != nil {
			c.logger.Errorf("consumer %+v consumes error %+v", c, err)
			return err
		}
		c.handle(ctx, topic)
	}

	if err := c.consumer.Close(); err != nil {
		return errors.Errorf("close kafka consumer %+v error %+v", c.consumer, err)
	}

	return errors.Errorf("consumer %+v exited", c.consumer)
}

func (c *GroupConsumer) Close() error {
	return c.consumer.Close()
}
func (c *GroupConsumer) handle(ctx context.Context, topic string) {
	wg := &sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {

			message, err := c.consumer.ReadMessage(-1)
			if err != nil {
				c.logger.Errorf("consumer %+v consumes error %+v", c, err)
				return
			}

			// check if context was cancelled, signaling that the consumer should stop
			if err := ctx.Err(); err != nil {
				c.logger.Errorf("consumer %+v exits due to context is canceled %+v", c, err)
				return
			}
			if err := c.handlers[topic].Handle(message); err != nil {
				// make sure you have a way to record or retry the error message
				c.logger.Errorf("consume message %s of topic %s partition %d error %+v", string(message.Value), topic, message.TopicPartition.Partition, err)
				continue
			}

			c.ready = make(chan struct{})
		}
	}()

	<-c.ready // Await till the consumer has been set up

	wg.Wait()
}
