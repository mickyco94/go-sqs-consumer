package app

import (
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/micky-clerkinoliver-cko/go-sqs-consumer/internal/handlers"
	"github.com/micky-clerkinoliver-cko/go-sqs-consumer/pkg/sqs"
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

	sqsConsumer := sqs.NewConsumer()

	sqsConsumer.Consume("local-queue", func(c *sqs.QueueConfiguration) {
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

	r.Use(middleware.RequestID)
	r.Use(middleware.RealIP)
	r.Use(middleware.Logger)
	r.Use(middleware.Recoverer)

	r.Get("/_system/health", func(rw http.ResponseWriter, r *http.Request) {
		rw.WriteHeader(http.StatusOK)
		rw.Write([]byte("Healthy"))
	})

	a.sqsConsumer.Run()

	a.logger.Fatal(http.ListenAndServe(":8080", r)) //Move port to config
}
