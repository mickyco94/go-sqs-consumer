package handlers

import (
	"encoding/json"

	"github.com/micky-clerkinoliver-cko/go-sqs-consumer/pkg/sqs"
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
		w.DeadLetter()
	}

	w.Handled()
}
