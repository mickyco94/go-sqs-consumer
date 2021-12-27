package sqs

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/sqs"
	awssqs "github.com/aws/aws-sdk-go/service/sqs"
)

//Move to using channels that return Request struct
//Have a separate error channel that they can also listen to
//Put all of these in the same struct and return from listen
//Move handler stuff into a separate package
//sqs/handler/pipeline
//handler can be purpose built for reading from these channels
//pipeline can be an extension that allows you to add middleware and defines some basic middlewares

type queue struct {
	name                string
	deadLetterQueueName string
	url                 string
	deadLetterQueueUrl  string
	retryPolicy         []time.Duration
	resultChannel       chan *Result
	client              sqs.SQS
}

func (q *queue) deleteMessage(rh receiptHandle) error {
	_, err := q.client.DeleteMessage(&awssqs.DeleteMessageInput{
		QueueUrl:      aws.String(q.url),
		ReceiptHandle: rh,
	})

	if err != nil {
		return err
	}

	return nil
}

func (q *queue) listenForMessages() (chan *Result, error) {
	queueUrl, err := getQueueUrl(q.client, q.deadLetterQueueName)

	if err != nil {
		return nil, err
	} else {
		q.deadLetterQueueUrl = queueUrl
	}

	queueUrl, err = getQueueUrl(q.client, q.name)

	if err != nil {
		return nil, err
	} else {
		q.url = queueUrl
	}

	go q.pollMessages()

	return q.resultChannel, nil
}

// func (q *queue) handleInternal(message *awssqs.Message) {

// 	messageTypeAttribute := message.MessageAttributes["MessageType"]

// 	if messageTypeAttribute == nil {
// 		q.deleteMessage(message.ReceiptHandle)
// 		return
// 	}

// 	messageType := aws.StringValue(messageTypeAttribute.StringValue)

// 	handler := q.handlerRegistration[messageType]

// 	if handler == nil {
// 		q.deleteMessage(message.ReceiptHandle)
// 		return
// 	}

// 	request := Request{
// 		Body:            Body(aws.StringValue(message.Body)),
// 		originalMessage: *message,
// 		MessageType:     messageType,
// 		context:         context.Background(),
// 		MessageId:       aws.StringValue(message.MessageId),
// 	}

// 	internalHandler := &requestHandler{
// 		request: request,
// 		q:       q,
// 		result:  0,
// 	}

// 	handler.Handle(internalHandler, request)
// }

func (q *queue) retry(msg *awssqs.Message) error {
	attemptNumber := 0

	currentAttemptAttribute := msg.MessageAttributes["RetryCount"]

	if currentAttemptAttribute != nil {
		parsed, err := strconv.Atoi(aws.StringValue(currentAttemptAttribute.StringValue))

		if err != nil {
			attemptNumber = int(parsed)
		}
	}

	attemptNumber++

	if attemptNumber >= len(q.retryPolicy) {
		//Retries exhausted, dead letter
		q.deadLetterMessage(msg)
		return nil
	}

	msg.MessageAttributes["RetryCount"] = &awssqs.MessageAttributeValue{
		StringValue: aws.String(fmt.Sprintf("%d", attemptNumber)),
		DataType:    aws.String("string"),
	}

	delay := q.retryPolicy[attemptNumber-1]

	q.client.SendMessage(&awssqs.SendMessageInput{
		DelaySeconds:      aws.Int64(int64(delay.Seconds())),
		MessageAttributes: msg.MessageAttributes,
		MessageBody:       msg.Body,
		QueueUrl:          &q.url,
	})

	return nil
}

func (q *queue) deadLetterMessage(msg *awssqs.Message) error {
	_, err := q.client.SendMessage(&awssqs.SendMessageInput{
		MessageBody:       msg.Body,
		MessageAttributes: msg.MessageAttributes,
		QueueUrl:          &q.deadLetterQueueUrl,
	})

	if err != nil {
		return err
	}

	q.deleteMessage(msg.ReceiptHandle)

	return nil
}

type Result struct {
	Error          error
	Receiver       ResponseReceiver
	MessageRequest Request
}

func getQueueUrl(s awssqs.SQS, queueName string) (string, error) {
	res, err := s.GetQueueUrl(&awssqs.GetQueueUrlInput{
		QueueName: aws.String(queueName),
	})

	if err != nil {
		return "", nil
	}

	return aws.StringValue(res.QueueUrl), nil
}

func (q *queue) pollMessages() {

	for {

		res := &Result{}

		messages, err := q.client.ReceiveMessage(&awssqs.ReceiveMessageInput{
			QueueUrl:              aws.String(q.url),
			WaitTimeSeconds:       aws.Int64(15),
			MessageAttributeNames: []*string{aws.String("All")},
			AttributeNames:        []*string{aws.String("All")},
		})

		if err != nil {
			res.Error = err
			q.resultChannel <- res
			continue
		}

		if len(messages.Messages) == 0 {
			continue
		}

		for _, message := range messages.Messages {

			messageType := ""

			messageTypeAttribute := message.MessageAttributes["MessageType"]

			if messageTypeAttribute != nil {
				messageType = aws.StringValue(messageTypeAttribute.StringValue)
			}

			h := &requestHandler{
				request: Request{
					context:         context.TODO(),
					MessageId:       aws.StringValue(message.MessageId),
					MessageType:     messageType,
					Attempt:         1, //TODO: Populate from message attributes
					MaxAttempts:     len(q.retryPolicy),
					Body:            Body(aws.StringValue(message.Body)),
					originalMessage: *message,
				},
				acted: false,
				q:     q,
			}

			res.Receiver = h
			res.MessageRequest = h.request

			//Don't block
			go func() {
				q.resultChannel <- res
			}()
		}
	}
}
