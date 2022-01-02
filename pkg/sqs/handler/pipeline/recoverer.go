package pipeline

import (
	"github.com/micky-clerkinoliver-cko/go-sqs-consumer/pkg/sqs"
	"github.com/micky-clerkinoliver-cko/go-sqs-consumer/pkg/sqs/handler"
)

func Recoverer() func(next handler.Handler) handler.Handler {
	return func(next handler.Handler) handler.Handler {
		return handler.HandlerFunc(func(w sqs.ResponseReceiver, r sqs.Request) {
			defer func() {
				if rvr := recover(); rvr != nil {
					w.Retry()
				}
			}()
			next.Handle(w, r)
		})
	}
}
