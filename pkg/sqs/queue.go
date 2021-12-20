package sqs

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	awssqs "github.com/aws/aws-sdk-go/service/sqs"
)

type queue struct {
	name                string
	deadLetterQueueName string
	url                 string
	deadLetterQueueUrl  string
	retryPolicy         []time.Duration
	sqs                 *SqsConsumer
	handlerRegistration map[string]Handler
	incomingChannel     chan *awssqs.Message
	deadLetterChannel   chan *awssqs.Message
	retryChannel        chan *awssqs.Message
	handleChannel       chan receiptHandle
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

	go func() {
		for message := range q.retryChannel {
			go q.retry(message)
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

func (q *queue) retry(msg *awssqs.Message) {
	attemptNumber := 0

	currentAttemptAttribute := msg.MessageAttributes["RetryCount"]

	parsed, err := strconv.Atoi(aws.StringValue(currentAttemptAttribute.StringValue))

	if err != nil {
		attemptNumber = int(parsed)
	}

	attemptNumber++

	if attemptNumber > len(q.retryPolicy) {
		//Retries exhausted, dead letter
		q.deadLetterMessage(msg)
		return
	}

	msg.MessageAttributes["RetryCount"] = &awssqs.MessageAttributeValue{
		StringValue: aws.String(fmt.Sprintf("%d", attemptNumber)),
		DataType:    aws.String("string"),
	}

	delay := q.retryPolicy[attemptNumber-1]

	q.sqs.sqs.SendMessage(&awssqs.SendMessageInput{
		DelaySeconds:      aws.Int64(int64(delay.Seconds())),
		MessageAttributes: msg.MessageAttributes,
		MessageBody:       msg.Body,
		QueueUrl:          &q.url,
	})

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
