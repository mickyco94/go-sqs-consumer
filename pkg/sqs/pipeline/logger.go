package pipeline

import (
	"time"

	"github.com/micky-clerkinoliver-cko/go-sqs-consumer/pkg/sqs"
	"github.com/sirupsen/logrus"
)

func Logger(logger logrus.FieldLogger) func(next sqs.Handler) sqs.Handler {
	return func(next sqs.Handler) sqs.Handler {
		return sqs.HandlerFunc(func(w sqs.ResponseReceiver, r sqs.Request) {

			start := time.Now()

			next.Handle(w, r)

			elapsed := time.Since(start)

			logger.WithFields(logrus.Fields{
				"Duration":    elapsed,
				"Result":      w.GetResult(),
				"MessageId":   r.MessageId,
				"MessageType": r.MessageType,
			}).Info("Handled message")
		})
	}
}
