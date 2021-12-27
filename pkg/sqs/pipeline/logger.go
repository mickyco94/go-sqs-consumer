package pipeline

import (
	"time"

	"github.com/micky-clerkinoliver-cko/go-sqs-consumer/pkg/sqs"
	"github.com/micky-clerkinoliver-cko/go-sqs-consumer/pkg/sqs/handler"
	"github.com/sirupsen/logrus"
)

func Logger(logger logrus.FieldLogger) func(next handler.Handler) handler.Handler {
	return func(next handler.Handler) handler.Handler {
		return handler.HandlerFunc(func(w sqs.ResponseReceiver, r sqs.Request) {

			start := time.Now()

			next.Handle(w, r)

			elapsed := time.Since(start)

			logger.WithFields(logrus.Fields{
				"Duration": elapsed,
				// "Result":      w.GetResult(),
				"MessageId":   r.MessageId,
				"MessageType": r.MessageType,
			}).Info("Handled message")
		})
	}
}
