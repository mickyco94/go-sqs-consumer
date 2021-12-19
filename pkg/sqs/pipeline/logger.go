package pipeline

import (
	"time"

	"github.com/micky-clerkinoliver-cko/go-sqs-consumer/pkg/sqs"
	"github.com/sirupsen/logrus"
)

func Logger(logger logrus.FieldLogger) func(next sqs.Handler) sqs.Handler {
	return func(next sqs.Handler) sqs.Handler {
		return sqs.HandlerFunc(func(ctx *sqs.HandlerContext, rawMsg string) sqs.HandlerResult {

			start := time.Now()
			res := next.Handle(ctx, rawMsg)

			elapsed := time.Since(start)

			logger.WithFields(logrus.Fields{
				"Duration":  elapsed,
				"Result":    res,
				"MessageId": ctx.MessageId,
			}).Info("Handled message")

			return res
		})
	}
}
