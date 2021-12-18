package sqs

import (
	"context"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	awssqs "github.com/aws/aws-sdk-go/service/sqs"
	"github.com/sirupsen/logrus"
)

type SqsConsumer struct {
	queue  Queue
	logger logrus.FieldLogger
	sqs    *awssqs.SQS
}

type QueueConfiguration struct {
	Name                string
	ChannelSize         int
	HandlerRegistration map[string]func(ctx *HandlerContext, json string) HandlerResult
}

type HandlerContext struct {
	context.Context
	logContext map[string]interface{}
	MessageId  string
}

func (ctx HandlerContext) toLogrusFields() logrus.Fields {
	fields := logrus.Fields{}
	for key, element := range ctx.logContext {
		fields[key] = element
	}
	return fields
}

func (ctx *HandlerContext) SetLogProperty(key string, value interface{}) {
	ctx.logContext[key] = value
}

func New(logger logrus.FieldLogger, queueConfig *QueueConfiguration) *SqsConsumer {

	sess := session.Must(session.NewSessionWithOptions(session.Options{
		Config: aws.Config{
			Region:   aws.String("eu-west-1"),
			Endpoint: aws.String("http://localhost:4566/"), //Move to config
			Credentials: credentials.NewCredentials(&credentials.StaticProvider{
				Value: credentials.Value{
					AccessKeyID:     "XX",
					SecretAccessKey: "XX",
				},
			}),
		},
	}))

	s := &SqsConsumer{
		logger: logger,
		sqs:    awssqs.New(sess),
		queue: Queue{
			name:                queueConfig.Name,
			handlerRegistration: queueConfig.HandlerRegistration,
			incomingChannel:     make(chan *awssqs.Message, queueConfig.ChannelSize),
			deleteChannel:       make(chan *string),
			retryChannel:        make(chan *awssqs.Message),
		},
	}

	s.queue.sqs = s

	return s
}

func (s *SqsConsumer) Run() error {
	return s.queue.listenForMessages()
}

func (q *Queue) setQueueUrl() error {
	res, err := q.sqs.sqs.GetQueueUrl(&awssqs.GetQueueUrlInput{
		QueueName: &q.name,
	})

	if err != nil {
		q.sqs.logger.Error("Error getting queue url")
		return err
	}

	q.url = aws.StringValue(res.QueueUrl)

	return nil
}

func (q *Queue) deleteMessage(receiptHandle *string) {
	q.sqs.sqs.DeleteMessage(&awssqs.DeleteMessageInput{
		QueueUrl:      aws.String(q.url),
		ReceiptHandle: receiptHandle,
	})
}

func (q *Queue) listenForMessages() error {
	err := q.setQueueUrl()

	if err != nil {
		return err
	}

	go func() {
		for message := range q.incomingChannel {

			start := time.Now()

			messageTypeAttribute := message.MessageAttributes["MessageType"]

			if messageTypeAttribute == nil {
				q.sqs.logger.Debug("Received message without message type attribute, deleting")
				q.deleteMessage(message.ReceiptHandle)
				continue
			}

			messageType := aws.StringValue(messageTypeAttribute.StringValue)

			handler := q.handlerRegistration[messageType]

			if handler == nil {
				q.deleteMessage(message.ReceiptHandle)
				q.sqs.logger.WithFields(logrus.Fields{
					"MessageType": messageType,
					"Attributes":  message.Attributes,
				}).Debug("No handler registered for message type")
				continue
			}

			ctx := &HandlerContext{
				logContext: map[string]interface{}{},
				MessageId:  aws.StringValue(message.MessageId),
				Context:    context.TODO(),
			}

			result := handler(ctx, aws.StringValue(message.Body))

			elapsed := time.Since(start)

			q.sqs.logger.WithFields(ctx.toLogrusFields()).WithFields(logrus.Fields{
				"MessageId": aws.StringValue(message.MessageId),
				"Body":      aws.StringValue(message.Body),
				"Result":    result,
				"Duration":  elapsed,
			}).Info("Handled message")

			switch result {
			case Handled:
				q.deleteMessage(message.ReceiptHandle)
			case DeadLetter:

			}

		}
	}()
	go q.pollMessages(q.incomingChannel)

	return nil
}

func (q *Queue) deadLetterMessage() {
	//Need to define a dlq in config
}

func (q *Queue) pollMessages(chn chan<- *awssqs.Message) {

	for {

		q.sqs.logger.Debug("Polling queue")

		output, err := q.sqs.sqs.ReceiveMessage(&awssqs.ReceiveMessageInput{
			QueueUrl:              aws.String(q.url),
			MaxNumberOfMessages:   aws.Int64(2),
			WaitTimeSeconds:       aws.Int64(15),
			MessageAttributeNames: []*string{aws.String("All")},
			AttributeNames:        []*string{aws.String("All")},
		})

		if err != nil {
			q.sqs.logger.WithField("ErrorMessage", err).Error("Error receiving message")
		}

		for _, message := range output.Messages {
			chn <- message
		}
	}
}

type Queue struct {
	name                string
	handlerRegistration map[string]func(ctx *HandlerContext, json string) HandlerResult
	url                 string
	sqs                 *SqsConsumer
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
