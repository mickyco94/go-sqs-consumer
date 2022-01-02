package pipeline

import (
	sqs "github.com/micky-clerkin-oliver-cko/go-sqs-consumer"
	"github.com/micky-clerkin-oliver-cko/go-sqs-consumer/handler"
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
