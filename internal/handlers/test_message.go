package handlers

import (
	"encoding/json"
	"time"

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

	h.logger.Debug("Simulating delay by waiting 5 seconds")
	time.Sleep(time.Second * 5)

	h.logger.WithFields(convertHandlerContextToLogFields(ctx)).Info("Completed event")
	return sqs.Handled
}

func convertHandlerContextToLogFields(ctx *sqs.HandlerContext) logrus.Fields {
	return logrus.Fields{
		"MessageId":   ctx.MessageId,
		"MessageType": ctx.MessageType,
	}
}
