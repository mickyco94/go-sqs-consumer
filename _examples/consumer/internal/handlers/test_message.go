package handlers

import (
	"encoding/json"

	sqs "github.com/micky-clerkin-oliver-cko/go-sqs-consumer"
	"github.com/sirupsen/logrus"
)

type TestMessageHandler struct {
	logger logrus.FieldLogger
}

type TestMessage struct {
	Foo string `json:"foo"`
}

func NewTestMessageHandler(logger logrus.FieldLogger) *TestMessageHandler {
	return &TestMessageHandler{
		logger: logger.WithField("src", "TestMessageHandler"),
	}
}

func (h *TestMessageHandler) Handle(w sqs.ResponseReceiver, r sqs.Request) {
	msg := &TestMessage{}
	err := json.Unmarshal([]byte(r.Body), msg)

	if err != nil {
		h.logger.WithError(err).Error("Error deserialising event")
		w.DeadLetter()
	}

	if msg.Foo == "deadletter" {
		w.DeadLetter()
		return
	}

	if msg.Foo == "retry" {
		w.Retry()
	}

	w.Handled()
}
