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
		sqs:    awssqs.New(sess),
		queues: make([]*queue, 0),
	}

	return s
}

func (s *SqsConsumer) Consume(queueName string, queueCfg func(queueConfig *QueueConfiguration)) {
	cfg := &QueueConfiguration{
		// handlers:    map[string]HandlerFunc{},
		// middlewares: make([]func(Handler) Handler, 0),
	}

	queueCfg(cfg)

	// chained := map[string]Handler{}
	// for k, v := range cfg.handlers {
	// 	chained[k] = Chain(cfg.middlewares...).Handler(v)
	// }

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

	for _, q := range s.queues {
		go func() {
			for v := range q.resultChannel {
				agg <- v
			}
		}()
	}

	for _, q := range s.queues {
		_, err := q.listenForMessages()
		if err != nil {
			return nil, err
		}
	}

	return agg, nil
}
