package sqs

import "time"

func NewQueueConfiguration() *QueueConfiguration {
	return &QueueConfiguration{}
}

func (cfg *QueueConfiguration) WithChannelSize(size int) {
	cfg.channelSize = size
}

func (cfg *QueueConfiguration) WithDeadLetterQueue(queueName string) {
	cfg.deadLetterQueue = queueName
}

func (cfg *QueueConfiguration) WithRetryPolicy(policy ...time.Duration) {
	cfg.retryConfig = policy
}
