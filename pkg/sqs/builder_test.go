package sqs

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

type mockReceiver struct{}

func (m mockReceiver) DeadLetter() error        { return nil }
func (m mockReceiver) Retry() error             { return nil }
func (m mockReceiver) Handled() error           { return nil }
func (m mockReceiver) GetResult() HandlerResult { return Handled }

func TestWithChannelSize(t *testing.T) {
	// arrange
	builder := NewQueueConfiguration()

	// act
	builder.WithChannelSize(5)

	// assert
	if builder.channelSize != 5 {
		t.Fatalf("Expected channel size to be %d, actually got %d", 5, builder.channelSize)
	}
}

func TestWithHandler(t *testing.T) {
	// arrange
	builder := NewQueueConfiguration()

	invoked := false

	mockHandler := func(w ResponseReceiver, r Request) {
		invoked = true
	}

	// act
	builder.WithHandler("test", mockHandler)

	// assert
	actual := builder.handlers["test"]

	assert.NotNil(t, actual)

	actual(&mockReceiver{}, Request{})

	if !invoked {
		t.Fatalf("Expected resolved handler to be invoked")
	}
}

func TestWithDeadLetterQueue(t *testing.T) {
	// arrange
	builder := NewQueueConfiguration()

	// act
	builder.WithDeadLetterQueue("test-dl")

	// assert
	assert.Equal(t, "test-dl", builder.deadLetterQueue)
}

func TestWithRetryPolicy(t *testing.T) {
	// arrange
	builder := NewQueueConfiguration()

	retries := []time.Duration{time.Hour, time.Second, time.Minute}

	// act
	builder.WithRetryPolicy(retries...)

	// assert
	assert.Equal(t, retries, builder.retryConfig)
}

type mockHandler struct{}

func (m mockHandler) Handle(ResponseReceiver, Request) {}

func TestUse(t *testing.T) {
	// arrange
	builder := NewQueueConfiguration()
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
