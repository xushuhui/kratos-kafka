package kafka

import (
	"context"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/go-kratos/kratos/v2/log"
	"github.com/go-kratos/kratos/v2/transport"
	"github.com/pkg/errors"
	"golang.org/x/sync/errgroup"
)

var _ transport.Server = (*Server)(nil)

// Consumer is the Kafka Consumer interface
type Consumer interface {
	Topics() []string
	RegisterHandler(handler Handler)
	HasHandler(topic string) bool
	Consume(ctx context.Context) error
	Close() error
}
type Handler interface {
	Topic() string
	Handle(message *kafka.Message) error
}

// Server is a Kafka server wrapper
type Server struct {
	consumers []Consumer
	handlers  map[string]Handler
}

// ServerOption is a Kafka server option.
type ServerOption func(server *Server)

// Consumers registers a set of consumers to the Server.
func Consumers(consumers []Consumer) ServerOption {
	return func(server *Server) {
		server.consumers = consumers
	}
}

// Handlers registers a set of handlers to the Server.
func Handlers(handlers ...Handler) ServerOption {
	return func(server *Server) {
		for _, handler := range handlers {
			server.handlers[handler.Topic()] = handler
		}
	}
}

// NewServer creates a Kafka server by options.
func NewServer(opts ...ServerOption) (*Server, error) {
	server := &Server{handlers: make(map[string]Handler)}

	for _, o := range opts {
		o(server)
	}

	if len(server.consumers) == 0 {
		return nil, errors.Errorf("no consumers")
	}
	if len(server.handlers) == 0 {
		return nil, errors.Errorf("no handlers")
	}

	for _, srvConsumer := range server.consumers {
		for _, topic := range srvConsumer.Topics() {
			if srvConsumer.HasHandler(topic) {
				return nil, errors.Errorf("duplicated handler for topic %s", topic)
			}
			handler, ok := server.handlers[topic]
			if !ok {
				return nil, errors.Errorf("no available handler for topic %s", topic)
			}
			srvConsumer.RegisterHandler(handler)
		}
	}
	return server, nil
}

// Start starts the Kafka server
func (s *Server) Start(ctx context.Context) error {
	eg, ctx := errgroup.WithContext(ctx)

	for _, serverConsumer := range s.consumers {
		srvConsumer := serverConsumer
		eg.Go(func() error {
			return srvConsumer.Consume(ctx)
		})
	}
	log.Info("[Kakfa] server start")
	return eg.Wait()
}

// Stop stops the Kafka server
func (s *Server) Stop(ctx context.Context) error {
	var result error
	for _, consumer := range s.consumers {
		if err := consumer.Close(); err != nil {
			log.Errorf("close consumer error: %v", err)
			result = err
		}
	}
	log.Info("[Kakfa] server stopping")
	return result
}
