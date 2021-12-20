package sqs

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	awssqs "github.com/aws/aws-sdk-go/service/sqs"
)

func New(cfg aws.Config) *SqsConsumer {

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
		retryPolicy:         cfg.retryConfig,
		sqs:                 s,
	}

	s.queues = append(s.queues, queue)
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
