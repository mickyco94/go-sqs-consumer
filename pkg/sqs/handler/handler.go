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
			go handler.Handle(m.Receiver, m.MessageRequest)
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
