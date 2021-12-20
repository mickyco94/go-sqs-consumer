package sqs

import (
	"context"
	"errors"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
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

type ResponseReceiver interface {
	DeadLetter() error
	Retry() error
	Handled() error
	GetResult() HandlerResult
}

type Body string

type Request struct {
	context         context.Context
	MessageId       string
	MessageType     string
	Body            Body
	originalMessage awssqs.Message
}

type requestHandler struct {
	request Request
	q       *queue
	result  HandlerResult
}

//DeadLetter directly does this, instead we should write to a channel that is consumed within this file
//That dead-letters the queue itself
func (r *requestHandler) DeadLetter() error {

	if r.result != Unhandled {
		return errors.New("message has already been handled")
	}

	r.result = DeadLetter
	func() {
		r.q.deadLetterChannel <- &r.request.originalMessage
	}()

	return nil
}

func (r *requestHandler) Retry() error {

	if r.result != Unhandled {
		return errors.New("message has already been handled")
	}

	r.result = Retry
	go func() {
		r.q.retryChannel <- &r.request.originalMessage
	}()

	return nil
}

func (r *requestHandler) Handled() error {

	if r.result != Unhandled {
		return errors.New("message has already been handled")
	}

	r.result = Handled

	go func() {
		r.q.handleChannel <- receiptHandle(r.request.originalMessage.ReceiptHandle)
	}()

	return nil
}

func (r *requestHandler) GetResult() HandlerResult {
	return r.result
}

type HandlerFunc func(w ResponseReceiver, r Request)

func (f HandlerFunc) Handle(w ResponseReceiver, r Request) {
	f(w, r)
}

type Handler interface {
	Handle(ResponseReceiver, Request)
}

type queue struct {
	name                string
	deadLetterQueueName string
	url                 string
	deadLetterQueueUrl  string
	sqs                 *SqsConsumer
	handlerRegistration map[string]Handler
	incomingChannel     chan *awssqs.Message
	deadLetterChannel   chan *awssqs.Message
	retryChannel        chan *awssqs.Message
	handleChannel       chan receiptHandle
}

// HandlerResult is an enum that indicates the action to take upon completion
type HandlerResult int

const (
	//Unhandled indicates the message has yet to be processed by the event pipeline
	Unhandled = 0
	//Handled indicates the message has been successfuly handled and will therefore be deleted from the queue
	Handled = 1
	//Retry will prompt the message to be re-enqueued with the specified delay
	Retry = 2
	//DeadLetter will send the message to the configured dead-letter queue
	DeadLetter = 3
)

type SqsConsumer struct {
	queues  []queue
	sqs     *awssqs.SQS
	errChan chan error
}

type receiptHandle *string

type QueueConfiguration struct {
	channelSize     int
	handlers        map[string]HandlerFunc
	deadLetterQueue string
	middlewares     []func(Handler) Handler
}

func NewConsumer(cfg aws.Config) *SqsConsumer {

	sess := session.Must(session.NewSessionWithOptions(session.Options{
		Config: cfg,
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
		deadLetterQueueName: cfg.deadLetterQueue,
		handlerRegistration: chained,
		incomingChannel:     make(chan *awssqs.Message, cfg.channelSize),
		deadLetterChannel:   make(chan *awssqs.Message),
		handleChannel:       make(chan receiptHandle),
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

func (s *SqsConsumer) Listen() chan error {

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

func (q *queue) deleteMessage(receiptHandle receiptHandle) {
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

	go func() {
		for receiptHandle := range q.handleChannel {
			go q.deleteMessage(receiptHandle)
		}
	}()

	go func() {
		for message := range q.deadLetterChannel {
			go q.deadLetterMessage(message)
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
		q.deleteMessage(message.ReceiptHandle)
		return
	}

	request := Request{
		Body:            Body(aws.StringValue(message.Body)),
		originalMessage: *message,
		MessageType:     messageType,
		context:         context.Background(),
		MessageId:       aws.StringValue(message.MessageId),
	}

	internalHandler := &requestHandler{
		request: request,
		q:       q,
		result:  0,
	}

	handler.Handle(internalHandler, request)
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

func (c *ChainHandler) Handle(w ResponseReceiver, r Request) {
	c.chain.Handle(w, r)
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
