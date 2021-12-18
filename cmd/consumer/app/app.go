package app

import (
	"encoding/json"
	"net/http"

	"github.com/micky-clerkinoliver-cko/go-sqs-consumer/cmd/consumer/app/sqs"
	log "github.com/sirupsen/logrus"
)

type App struct {
	logger      log.FieldLogger
	SqsConsumer *sqs.SqsConsumer
}

type TestMessage struct {
	Foo string `json:"foo"`
}

func New() *App {

	logger := log.WithFields(log.Fields{
		"application": "go-sqs-consumer",
		"version":     "0.0.0",
		"env":         "local",
	})

	//Change to a builder pattern and validate in New()
	handlerRegistration := make(map[string]func(ctx *sqs.HandlerContext, rawMsg string) sqs.HandlerResult)

	handlerRegistration["test"] = func(ctx *sqs.HandlerContext, rawMsg string) sqs.HandlerResult {
		msg := &TestMessage{}
		json.Unmarshal([]byte(rawMsg), msg)
		ctx.SetLogProperty("Foo", msg.Foo)
		return sqs.Handled
	}

	sqsConsumer := sqs.New(logger, &sqs.QueueConfiguration{
		Name:                "local-queue",
		ChannelSize:         5,
		HandlerRegistration: handlerRegistration,
	})

	a := &App{
		logger:      logger,
		SqsConsumer: sqsConsumer,
	}
	a.configureLogging()

	return a
}

func (a *App) configureLogging() {
	log.SetLevel(log.TraceLevel)
	log.SetFormatter(&log.TextFormatter{
		ForceColors:   true,
		FullTimestamp: true,
	})

}

func (a *App) Run() {
	a.logger.Info("Starting application")
	defer a.logger.Info("Shutting down application")

	//Use a more sophiscated router like chi or something
	http.HandleFunc("/_system/health", func(rw http.ResponseWriter, r *http.Request) {
		rw.WriteHeader(http.StatusOK)
		rw.Write([]byte("Healthy"))
	})

	//Have some channels at the core of the service that both the SQS service + the HTTP Service push to
	//If either channel sends a close then shut down the app
	err := a.SqsConsumer.Run()

	if err != nil {
		a.logger.WithField("err", err).Error("Error starting SQS Consumers")
		return
	}

	a.logger.Fatal(http.ListenAndServe(":8080", nil)) //Move port to config
}
