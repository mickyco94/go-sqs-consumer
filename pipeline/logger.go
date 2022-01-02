package pipeline

import (
	"time"

	sqs "github.com/micky-clerkin-oliver-cko/go-sqs-consumer"
	"github.com/micky-clerkin-oliver-cko/go-sqs-consumer/handler"
	"github.com/sirupsen/logrus"
)

//Need a way to tap into the logger from elsewhere
func Logger(logger logrus.FieldLogger) func(next handler.Handler) handler.Handler {
	return func(next handler.Handler) handler.Handler {
		return handler.HandlerFunc(func(w sqs.ResponseReceiver, r sqs.Request) {

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
