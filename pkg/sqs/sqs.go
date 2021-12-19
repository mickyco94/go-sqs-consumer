package sqs

import (
	"context"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	awssqs "github.com/aws/aws-sdk-go/service/sqs"
)

type HandlerFunc func(ctx *HandlerContext, rawMsg string) HandlerResult

type SqsConsumer struct {
	queues  []queue
	sqs     *awssqs.SQS
	errChan chan error
}

type QueueConfiguration struct {
	channelSize     int
	handlers        map[string]HandlerFunc
	deadLetterQueue string
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
		handlers: map[string]HandlerFunc{},
	}
	queueCfg(cfg)
	queue := queue{
		name:                queueName,
		handlerRegistration: cfg.handlers,
		incomingChannel:     make(chan *awssqs.Message, cfg.channelSize),
		deleteChannel:       make(chan *string),
		retryChannel:        make(chan *awssqs.Message),
		sqs:                 s,
	}
	s.queues = append(s.queues, queue)
}

func (cfg *QueueConfiguration) WithChannelSize(size int) *QueueConfiguration {
	cfg.channelSize = size
	return cfg
}

func (cfg *QueueConfiguration) WithHandler(messageType string, handler HandlerFunc) *QueueConfiguration {
	cfg.handlers[messageType] = handler
	return cfg
}

func (cfg *QueueConfiguration) WithDeadLetterQueue(queueName string) *QueueConfiguration {
	cfg.deadLetterQueue = queueName
	return cfg
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
		q.deleteMessage(message.ReceiptHandle)
		return
	}

	ctx := &HandlerContext{
		MessageId:   aws.StringValue(message.MessageId),
		MessageType: messageType,
		Context:     context.TODO(),
	}

	result := handler(ctx, aws.StringValue(message.Body))

	switch result {
	case Handled:
		q.deleteMessage(message.ReceiptHandle)
	case DeadLetter:
		q.deadLetterMessage(message)
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

type queue struct {
	name                string
	deadLetterQueueName string
	url                 string
	deadLetterQueueUrl  string
	sqs                 *SqsConsumer
	handlerRegistration map[string]HandlerFunc
	incomingChannel     chan *awssqs.Message
	deleteChannel       chan *string
	retryChannel        chan *awssqs.Message
}

type HandlerResult int

const (
	Handled    = 0
	Retry      = 1
	DeadLetter = 2
)
