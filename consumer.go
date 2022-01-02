package sqs

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
)

func New(cfg aws.Config) *SqsConsumer {

	sess := session.Must(session.NewSessionWithOptions(session.Options{
		Config: cfg,
	}))

	s := &SqsConsumer{
		sqs:    sqs.New(sess),
		queues: make([]*queue, 0),
	}

	return s
}

func (s *SqsConsumer) Consume(queueName string, queueCfg func(queueConfig *QueueConfiguration)) {
	cfg := &QueueConfiguration{}

	queueCfg(cfg)

	queue := queue{
		name:                queueName,
		deadLetterQueueName: cfg.deadLetterQueue,
		retryPolicy:         cfg.retryConfig,
		client:              *s.sqs,
		resultChannel:       make(chan *Result),
	}

	s.queues = append(s.queues, &queue)
}

func (s *SqsConsumer) Listen() (chan *Result, error) {
	agg := make(chan *Result)

	chans := make([]chan *Result, len(s.queues))

	for i, q := range s.queues {
		c, err := q.listenForMessages()
		if err != nil {
			return nil, err
		}
		chans[i] = c
	}

	for _, c := range chans {
		go func() {
			for v := range c {
				agg <- v
			}
		}()
	}

	return agg, nil
}
