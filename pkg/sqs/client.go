package sqs

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
)

type client interface {
	getQueueUrl(string) (string, error)
	deleteMessage(url string, rh receiptHandle) error
	receiveMessage(url string, waitTime int) ([]*sqs.Message, error)
	sendMessage(url string, body Body, attributes map[string]*sqs.MessageAttributeValue) error
}

func newClient(session session.Session) *wrappedSqsClient {
	return &wrappedSqsClient{
		sqsClient: *sqs.New(&session),
	}
}

type wrappedSqsClient struct {
	sqsClient sqs.SQS
}

func (s *wrappedSqsClient) getQueueUrl(u string) (string, error) {
	res, err := s.sqsClient.GetQueueUrl(&sqs.GetQueueUrlInput{
		QueueName: aws.String(u),
	})

	if err != nil {
		return "", err
	}

	return aws.StringValue(res.QueueUrl), nil
}

func (s *wrappedSqsClient) receiveMessage(url string, waitTime int64) ([]*sqs.Message, error) {
	res, err := s.sqsClient.ReceiveMessage(&sqs.ReceiveMessageInput{
		QueueUrl:              aws.String(url),
		WaitTimeSeconds:       aws.Int64(waitTime),
		MessageAttributeNames: []*string{aws.String("All")},
		AttributeNames:        []*string{aws.String("All")},
	})

	if err != nil {
		return nil, err
	}

	return res.Messages, nil
}
