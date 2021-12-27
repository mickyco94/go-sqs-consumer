package sqs

import (
	"context"
	"time"

	awssqs "github.com/aws/aws-sdk-go/service/sqs"
)

// Features to add:
// [?] Error handling and piping
//   [] Define some error types
// [] Move different processes to different channels, e.g. deadlettering
// [] Message retry handlers, on a separate channel. Batch publish
// [] Transient retries, only push to err channel if exhausted
// [x] Middleware, create a handler pipeline. Let users define their own
//   [] Create some basic, like a timer. Logger etc.
// [] Extend HandlerContext
// [] Extend app to have viper config, take a config struct in for AWS settings at least
// [] Make a decision on where the queue resolution should live
// [] Some element of lazy loading
// [] Graceful shutdown, block for requests in[]flight. Needs to be implemented for HTTP too
//   [] Close all channels and attempt to cancel in-flight messages
// [] Add docs, i.e. comments
// [x] Move to using a result writer + request context pattern that is more in-line with idiomatic go
// [] Arbritrary context thing
// [] Unit tests!!

type ResponseReceiver interface {
	DeadLetter() error
	Retry() error
	Handled() error
}

type Body string

type Request struct {
	context         context.Context
	MessageId       string
	MessageType     string
	Attempt         int
	MaxAttempts     int
	Body            Body
	originalMessage awssqs.Message
}

type Consumer interface {
	Consume(queueName string, queueCfg func(queueConfig *QueueConfiguration))
	Listen() (chan *Result, error)
}

type QueueConfiguration struct {
	channelSize     int
	deadLetterQueue string
	retryConfig     []time.Duration
}

type SqsConsumer struct {
	queues []*queue
	sqs    *awssqs.SQS
}

type receiptHandle *string
