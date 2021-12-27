package handler

import "github.com/micky-clerkinoliver-cko/go-sqs-consumer/pkg/sqs"

//Split into configure and run
func New(c sqs.Consumer, cfg func(handlerCfg *HandlerQueueConfiguration)) error {
	hCfg := &HandlerQueueConfiguration{
		handlers:    map[string]Handler{},
		middlewares: make([]func(Handler) Handler, 0),
	}

	cfg(hCfg)

	chained := map[string]Handler{}
	for k, v := range hCfg.handlers {
		chained[k] = Chain(hCfg.middlewares...).Handler(v)
	}

	hCfg.handlers = chained

	chn, err := c.Listen()
	if err != nil {
		return err
	}

	go func() {
		for m := range chn {
			handler := hCfg.handlers[m.MessageRequest.MessageType]
			if handler == nil {
				m.Receiver.DeadLetter()
			}
			handler.Handle(m.Receiver, m.MessageRequest)
		}
	}()

	return nil
}

type HandlerFunc func(w sqs.ResponseReceiver, r sqs.Request)

func (f HandlerFunc) Handle(w sqs.ResponseReceiver, r sqs.Request) {
	f(w, r)
}

type Handler interface {
	Handle(sqs.ResponseReceiver, sqs.Request)
}

// HandlerResult is an enum that indicates the action to take upon completion
type HandlerResult int

const (
	//Unhandled indicates the message has yet to be processed by the event pipeline
	Unhandled = 0
	//Handled indicates the message has been successfuly handled and will therefore be deleted from the queue
	Handled = 1
	//Retry will prompt the message to be re-enqueued with the specified delay
	Retry = 2
	//DeadLetter will send the message to the configured dead-letter queue
	DeadLetter = 3
)
