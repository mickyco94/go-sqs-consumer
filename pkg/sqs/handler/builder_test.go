package handler

import (
	"testing"

	"github.com/micky-clerkinoliver-cko/go-sqs-consumer/pkg/sqs"
	"github.com/stretchr/testify/assert"
)

type mockReceiver struct{}

func (m mockReceiver) DeadLetter() error           { return nil }
func (m mockReceiver) Retry() error                { return nil }
func (m mockReceiver) Handled() error              { return nil }
func (m mockReceiver) GetResult() sqs.MessageState { return sqs.Handled }

func TestWithHandler(t *testing.T) {
	// arrange
	builder := HandlerQueueConfiguration{}

	invoked := false

	mockHandler := func(w sqs.ResponseReceiver, r sqs.Request) {
		invoked = true
	}

	// act
	builder.WithHandler("test", mockHandler)

	// assert
	actual := builder.handlers["test"]

	assert.NotNil(t, actual)

	actual.Handle(&mockReceiver{}, sqs.Request{})

	assert.True(t, invoked)
}

type mockHandler struct{}

func (m mockHandler) Handle(sqs.ResponseReceiver, sqs.Request) {}

func TestUse(t *testing.T) {
	// arrange
	builder := HandlerQueueConfiguration{}
	invoked := false

	// act
	builder.Use(func(h Handler) Handler {
		invoked = true
		return h
	})

	// assert
	assert.Len(t, builder.middlewares, 1)
	actual := builder.middlewares[0]
	actual(&mockHandler{})
	assert.True(t, invoked, "actual should have invoked the middleware and updated invoked to true")
}
