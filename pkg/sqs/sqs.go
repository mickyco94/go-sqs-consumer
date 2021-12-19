package sqs

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	awssqs "github.com/aws/aws-sdk-go/service/sqs"
)

// Features to add:
// [?] Error handling and piping
//   [] Define some error types
// [] Move different processes to different channels, e.g. deadlettering
// [] Message retry handlers, on a separate channel. Batch publish
// [] Transient retries, only push to err channel if exhausted
// [] Middleware, create a handler pipeline. Let users define their own
//   [] Create some basic, like a timer. Logger etc.
// [] Extend HandlerContext
// [] Extend app to have viper config, take a config struct in for AWS settings at least
// [] Make a decision on where the queue resolution should live
// [] Some element of lazy loading
// [] Graceful shutdown, block for requests in[]flight. Needs to be implemented for HTTP too
//   [] Close all channels and attempt to cancel in-flight messages
// [] Add docs, i.e. comments

type HandlerFunc func(ctx *HandlerContext, rawMsg string) HandlerResult

// ServeHTTP calls f(w, r).
func (f HandlerFunc) Handle(ctx *HandlerContext, r string) HandlerResult {
	return f(ctx, r)
}

type Handler interface {
	Handle(*HandlerContext, string) HandlerResult
}

type queue struct {
	name                string
	deadLetterQueueName string
	url                 string
	deadLetterQueueUrl  string
	sqs                 *SqsConsumer
	handlerRegistration map[string]Handler
	incomingChannel     chan *awssqs.Message
	deleteChannel       chan *string
	retryChannel        chan *awssqs.Message
}

// HandlerResult is an enum that indicates the action to take upon completion
type HandlerResult int

const (
	//Handled indicates the message has been successfuly handled and will therefore be deleted from the queue
	Handled = 0
	//Retry will prompt the message to be re-enqueued with the specified delay
	Retry      = 1
	DeadLetter = 2
)

type SqsConsumer struct {
	queues  []queue
	sqs     *awssqs.SQS
	errChan chan error
}

type QueueConfiguration struct {
	channelSize     int
	handlers        map[string]HandlerFunc
	deadLetterQueue string
	middlewares     []func(Handler) Handler
}

type HandlerContext struct {
	context.Context
	MessageId   string
	MessageType string
}

func NewConsumer() *SqsConsumer {

	sess := session.Must(session.NewSessionWithOptions(session.Options{
		Config: aws.Config{
			Region:   aws.String("eu-west-1"),
			Endpoint: aws.String("http://localhost:4566/"),
			Credentials: credentials.NewCredentials(&credentials.StaticProvider{
				Value: credentials.Value{
					AccessKeyID:     "XX",
					SecretAccessKey: "XX",
				},
			}),
		},
	}))

	s := &SqsConsumer{
		sqs:     awssqs.New(sess),
		errChan: make(chan error),
		queues:  make([]queue, 0),
	}

	return s
}

func (s *SqsConsumer) Consume(queueName string, queueCfg func(queueConfig *QueueConfiguration)) {
	cfg := &QueueConfiguration{
		handlers:    map[string]HandlerFunc{},
		middlewares: make([]func(Handler) Handler, 0),
	}

	queueCfg(cfg)

	chained := map[string]Handler{}
	for k, v := range cfg.handlers {
		chained[k] = Chain(cfg.middlewares...).Handler(v)
	}

	queue := queue{
		name:                queueName,
		handlerRegistration: chained,
		incomingChannel:     make(chan *awssqs.Message, cfg.channelSize),
		deleteChannel:       make(chan *string),
		retryChannel:        make(chan *awssqs.Message),
		sqs:                 s,
	}
	s.queues = append(s.queues, queue)
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

func (s *SqsConsumer) Run() chan error {

	for _, q := range s.queues {
		q.listenForMessages()
	}

	return s.errChan
}

func (s *SqsConsumer) getQueueUrl(queueName string) (string, error) {
	res, err := s.sqs.GetQueueUrl(&awssqs.GetQueueUrlInput{
		QueueName: aws.String(queueName),
	})

	if err != nil {
		return "", nil
	}

	return aws.StringValue(res.QueueUrl), nil
}

func (q *queue) deleteMessage(receiptHandle *string) {
	q.sqs.sqs.DeleteMessage(&awssqs.DeleteMessageInput{
		QueueUrl:      aws.String(q.url),
		ReceiptHandle: receiptHandle,
	})
}

func (q *queue) listenForMessages() {
	queueUrl, err := q.sqs.getQueueUrl(q.deadLetterQueueName)

	if err != nil {
		q.sqs.errChan <- err
		return
	} else {
		q.deadLetterQueueUrl = queueUrl
	}

	queueUrl, err = q.sqs.getQueueUrl(q.name)

	if err != nil {
		q.sqs.errChan <- err
		return
	} else {
		q.url = queueUrl
	}

	if err != nil {
		q.sqs.errChan <- err
	}

	go func() {
		for message := range q.incomingChannel {
			go q.handleInternal(message)
		}
	}()
	go q.pollMessages(q.incomingChannel)
}

func (q *queue) handleInternal(message *awssqs.Message) {

	messageTypeAttribute := message.MessageAttributes["MessageType"]

	if messageTypeAttribute == nil {
		q.deleteMessage(message.ReceiptHandle)
		return
	}

	messageType := aws.StringValue(messageTypeAttribute.StringValue)

	handler := q.handlerRegistration[messageType]

	if handler == nil {
		fmt.Println("Couldn't find a handler")
		q.deleteMessage(message.ReceiptHandle)
		return
	}

	ctx := &HandlerContext{
		MessageId:   aws.StringValue(message.MessageId),
		MessageType: messageType,
		Context:     context.TODO(),
	}

	result := handler.Handle(ctx, aws.StringValue(message.Body))

	switch result {
	case Handled:
		q.deleteMessage(message.ReceiptHandle)
	case DeadLetter:
		q.deadLetterMessage(message)
	case Retry:
		panic("Not yet implemented!")
	}
}

func (q *queue) deadLetterMessage(msg *awssqs.Message) {
	_, err := q.sqs.sqs.SendMessage(&awssqs.SendMessageInput{
		MessageBody:       msg.Body,
		MessageAttributes: msg.MessageAttributes,
		QueueUrl:          &q.deadLetterQueueUrl,
	})

	if err != nil {
		q.sqs.errChan <- err
		return
	}
}

func (q *queue) pollMessages(chn chan<- *awssqs.Message) {

	for {

		output, err := q.sqs.sqs.ReceiveMessage(&awssqs.ReceiveMessageInput{
			QueueUrl:              aws.String(q.url),
			WaitTimeSeconds:       aws.Int64(15),
			MessageAttributeNames: []*string{aws.String("All")},
			AttributeNames:        []*string{aws.String("All")},
		})

		if err != nil {
			//This error could be transient, maybe we want them to be able to have an event listener for this err?
			//Error channel
			q.sqs.errChan <- err
			continue
		}

		for _, message := range output.Messages {
			chn <- message
		}
	}
}

// Chain returns a Middlewares type from a slice of middleware handlers.
func Chain(middlewares ...func(Handler) Handler) Middlewares {
	return Middlewares(middlewares)
}

// Handler builds and returns a Handler from the chain of middlewares,
// with `h Handler` as the final handler.
func (mws Middlewares) Handler(h Handler) Handler {
	return &ChainHandler{h, chain(mws, h), mws}
}

// HandlerFunc builds and returns a Handler from the chain of middlewares,
// with `h Handler` as the final handler.
func (mws Middlewares) HandlerFunc(h HandlerFunc) Handler {
	return &ChainHandler{h, chain(mws, h), mws}
}

// ChainHandler is a sqs.Handler with support for handler composition and
// execution.
type ChainHandler struct {
	Endpoint    Handler
	chain       Handler
	Middlewares Middlewares
}

func (c *ChainHandler) Handle(ctx *HandlerContext, rawMsg string) HandlerResult {
	return c.chain.Handle(ctx, rawMsg)
}

// chain builds a sqs.Handler composed of an inline middleware stack and endpoint
// handler in the order they are passed.
func chain(middlewares []func(Handler) Handler, endpoint Handler) Handler {
	// Return ahead of time if there aren't any middlewares for the chain
	if len(middlewares) == 0 {
		return endpoint
	}

	// Wrap the end handler with the middleware chain
	h := middlewares[len(middlewares)-1](endpoint)
	for i := len(middlewares) - 2; i >= 0; i-- {
		h = middlewares[i](h)
	}

	return h
}

// Middlewares type is a slice of standard middleware handlers with methods
// to compose middleware chains and sqs.Handler's.
type Middlewares []func(Handler) Handler
