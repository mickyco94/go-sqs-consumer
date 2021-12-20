package sqs

import "time"

type QueueConfiguration struct {
	channelSize     int
	handlers        map[string]HandlerFunc
	deadLetterQueue string
	middlewares     []func(Handler) Handler
	retryConfig     []time.Duration
}

func NewQueueConfiguration() *QueueConfiguration {
	return &QueueConfiguration{
		handlers:    map[string]HandlerFunc{},
		middlewares: make([]func(Handler) Handler, 0),
	}
}

func (cfg *QueueConfiguration) WithChannelSize(size int) {
	cfg.channelSize = size
}

func (cfg *QueueConfiguration) WithHandler(messageType string, handler HandlerFunc) {
	cfg.handlers[messageType] = handler
}

func (cfg *QueueConfiguration) WithDeadLetterQueue(queueName string) {
	cfg.deadLetterQueue = queueName
}

func (cfg *QueueConfiguration) Use(middleware func(Handler) Handler) {
	cfg.middlewares = append(cfg.middlewares, middleware)
}

func (cfg *QueueConfiguration) WithRetryPolicy(policy ...time.Duration) {
	cfg.retryConfig = policy
}
