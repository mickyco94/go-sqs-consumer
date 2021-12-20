package app

import (
	"net/http"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/micky-clerkinoliver-cko/go-sqs-consumer/internal/handlers"
	"github.com/micky-clerkinoliver-cko/go-sqs-consumer/pkg/sqs"
	"github.com/micky-clerkinoliver-cko/go-sqs-consumer/pkg/sqs/pipeline"
	log "github.com/sirupsen/logrus"
)

type App struct {
	logger      log.FieldLogger
	sqsConsumer *sqs.SqsConsumer
}

func New() *App {

	logger := log.WithFields(log.Fields{
		"application": "go-sqs-consumer",
		"version":     "0.0.0",
		"env":         "local",
	})

	testMessageHandler := handlers.NewTestMessageHandler(logger)

	sqsConsumer := sqs.NewConsumer(aws.Config{
		Region:   aws.String("eu-west-1"),
		Endpoint: aws.String("http://localhost:4566/"),
		Credentials: credentials.NewCredentials(&credentials.StaticProvider{
			Value: credentials.Value{
				AccessKeyID:     "XX",
				SecretAccessKey: "XX",
			},
		}),
	})

	sqsConsumer.Consume("local-queue", func(c *sqs.QueueConfiguration) {
		c.Use(pipeline.Logger(logger))
		c.WithDeadLetterQueue("local-queue-dl")
		c.WithChannelSize(100)
		c.WithHandler("test", testMessageHandler.Handle)
	})

	a := &App{
		logger:      logger,
		sqsConsumer: sqsConsumer,
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

	r := chi.NewRouter()

	r.Use(func(h http.Handler) http.Handler {
		fn := func(w http.ResponseWriter, r *http.Request) {

			h.ServeHTTP(w, r)
		}

		return http.HandlerFunc(fn)
	})
	r.Use(middleware.RequestID)
	r.Use(middleware.RealIP)
	r.Use(middleware.Recoverer)

	r.Get("/_system/health", func(rw http.ResponseWriter, r *http.Request) {
		rw.WriteHeader(http.StatusOK)
		rw.Write([]byte("Healthy"))
	})

	errs := a.sqsConsumer.Listen()

	go func() {
		for err := range errs {
			a.logger.WithError(err).Error("Error in sqs-consumers")
		}
	}()

	a.logger.Fatal(http.ListenAndServe(":8080", r)) //Move port to config
}
