package app

import (
	"net/http"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	sqs "github.com/micky-clerkin-oliver-cko/go-sqs-consumer"
	"github.com/micky-clerkin-oliver-cko/go-sqs-consumer/examples/consumer/internal/handlers"
	"github.com/micky-clerkin-oliver-cko/go-sqs-consumer/handler"
	"github.com/micky-clerkin-oliver-cko/go-sqs-consumer/pipeline"
	log "github.com/sirupsen/logrus"
)

type App struct {
	logger      log.FieldLogger
	sqsConsumer sqs.Consumer
}

var consumer sqs.Consumer

func New() *App {

	logger := log.WithFields(log.Fields{
		"application": "go-sqs-consumer",
		"version":     "0.0.0",
		"env":         "local",
	})

	// testMessageHandler := handlers.NewTestMessageHandler(logger)

	consumer = sqs.New(aws.Config{
		Region:   aws.String("eu-west-1"),
		Endpoint: aws.String("http://localhost:4566/"),
		Credentials: credentials.NewCredentials(&credentials.StaticProvider{
			Value: credentials.Value{
				AccessKeyID:     "XX",
				SecretAccessKey: "XX",
			},
		}),
	})

	consumer.Consume("local-queue", func(c *sqs.QueueConfiguration) {
		c.WithRetryPolicy(time.Second*5, time.Second*10, time.Second*30, time.Second*60)
		c.WithDeadLetterQueue("local-queue-dl")
		c.WithChannelSize(100)
	})

	// consumer.Consume("local-queue-dl", func(queueConfig *sqs.QueueConfiguration) {
	// 	queueConfig.WithDeadLetterQueue("local-queue-dl")
	// 	queueConfig.WithChannelSize(1)
	// 	// queueConfig.WithHandler("test", func(w sqs.ResponseReceiver, r sqs.Request) {
	// 	// 	logger.Debug("received DLQ msg")
	// 	// 	w.Handled()
	// 	// })
	// })

	a := &App{
		logger:      logger,
		sqsConsumer: consumer,
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

var r chi.Router

func (a *App) Run() {
	a.logger.Info("Starting application")
	defer a.logger.Info("Shutting down application")

	r = chi.NewRouter()

	r.Use(middleware.RequestID)
	r.Use(middleware.RealIP)
	r.Use(middleware.Recoverer)

	r.Get("/_system/health", func(rw http.ResponseWriter, r *http.Request) {
		rw.WriteHeader(http.StatusOK)
		rw.Write([]byte("Healthy"))
	})

	err := handler.New(a.sqsConsumer, func(handlerCfg *handler.HandlerQueueConfiguration) {
		handlerCfg.Use(pipeline.Logger(a.logger))
		handlerCfg.WithHandler("test", handlers.NewTestMessageHandler(a.logger).Handle)
	})

	if err != nil {
		a.logger.WithError(err).Fatal("")
	}

	a.logger.Fatal(http.ListenAndServe(":8080", r)) //Move port to config
}
