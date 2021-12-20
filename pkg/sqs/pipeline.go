package sqs

// Chain returns a Middlewares type from a slice of middleware handlers.
func Chain(middlewares ...func(Handler) Handler) Middlewares {
	return Middlewares(middlewares)
}

// Handler builds and returns a Handler from the chain of middlewares,
// with `h Handler` as the final handler.
func (mws Middlewares) Handler(h Handler) Handler {
	return &ChainHandler{h, chain(mws, h), mws}
}

// HandlerFunc builds and returns a Handler from the chain of middlewares,
// with `h Handler` as the final handler.
func (mws Middlewares) HandlerFunc(h HandlerFunc) Handler {
	return &ChainHandler{h, chain(mws, h), mws}
}

// ChainHandler is a sqs.Handler with support for handler composition and
// execution.
type ChainHandler struct {
	Endpoint    Handler
	chain       Handler
	Middlewares Middlewares
}

func (c *ChainHandler) Handle(w ResponseReceiver, r Request) {
	c.chain.Handle(w, r)
}

// chain builds a sqs.Handler composed of an inline middleware stack and endpoint
// handler in the order they are passed.
func chain(middlewares []func(Handler) Handler, endpoint Handler) Handler {
	// Return ahead of time if there aren't any middlewares for the chain
	if len(middlewares) == 0 {
		return endpoint
	}

	// Wrap the end handler with the middleware chain
	h := middlewares[len(middlewares)-1](endpoint)
	for i := len(middlewares) - 2; i >= 0; i-- {
		h = middlewares[i](h)
	}

	return h
}

// Middlewares type is a slice of standard middleware handlers with methods
// to compose middleware chains and sqs.Handler's.
type Middlewares []func(Handler) Handler
