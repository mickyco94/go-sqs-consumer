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

func (h *TestMessageHandler) Handle(ctx *sqs.HandlerContext, rawMsg string) sqs.HandlerResult {
	msg := &TestMessage{}
	err := json.Unmarshal([]byte(rawMsg), msg)

	if err != nil {
		h.logger.WithError(err).Error("Error deserialising event")
		return sqs.DeadLetter
	}

	return sqs.Handled
}
