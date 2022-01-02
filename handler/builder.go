package handler

type HandlerQueueConfiguration struct {
	handlers    map[string]Handler
	middlewares []func(Handler) Handler
}

func (cfg *HandlerQueueConfiguration) WithHandler(messageType string, handler HandlerFunc) {
	cfg.handlers[messageType] = handler
}

func (cfg *HandlerQueueConfiguration) Use(middleware func(Handler) Handler) {
	cfg.middlewares = append(cfg.middlewares, middleware)
}
