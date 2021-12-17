package main

import (
	"net/http"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	awssqs "github.com/aws/aws-sdk-go/service/sqs"
	log "github.com/sirupsen/logrus"
)

type App struct {
	logger log.FieldLogger
	sqs    *awssqs.SQS
}

//Initialise can be moved to its own package and wraps up all the API level stuff
func Initialise() *App {
	a := &App{}
	a.configureLogging()
	return a
}

func (a *App) configureLogging() {
	log.SetLevel(log.TraceLevel)
	log.SetFormatter(&log.TextFormatter{
		ForceColors:   true,
		FullTimestamp: true,
	})

	a.logger = log.WithFields(log.Fields{
		"application": "go-sqs-consumer",
		"version":     "0.0.0",
		"env":         "local",
	})
}

func (a *App) Run() {
	a.logger.Info("Starting application")

	//Use a more sophiscated router like chi or something
	http.HandleFunc("/_system/health", func(rw http.ResponseWriter, r *http.Request) {
		rw.WriteHeader(http.StatusOK)
		rw.Write([]byte("Healthy"))
	})

	//Have some channels at the core of the service that both the SQS service + the HTTP Service push to
	//If either channel sends a close then shut down the app

	a.logger.Fatal(http.ListenAndServe(":8080", nil)) //Move port to config
}

func (a *App) configureSqs() {
	sess := session.Must(session.NewSessionWithOptions(session.Options{
		Config: aws.Config{
			Region:   aws.String("eu-west-1"),
			Endpoint: aws.String("http://localstack:4566/"),
			Credentials: credentials.NewCredentials(&credentials.StaticProvider{
				Value: credentials.Value{
					AccessKeyID:     "XX",
					SecretAccessKey: "XX",
				},
			}),
		},
	}))

	a.sqs = awssqs.New(sess)
}

func (a *App) pollMessages(chn chan<- *awssqs.Message) {

	for {
		output, err := a.sqs.ReceiveMessage(&awssqs.ReceiveMessageInput{
			QueueUrl:            aws.String("http://localstack:4566/000000000000/local-queue"), //Get queue URL based on name alone
			MaxNumberOfMessages: aws.Int64(2),
			WaitTimeSeconds:     aws.Int64(15),
		})

		if err != nil {
			a.logger.WithField("ErrorMessage", err).Error("Error receiving message")
		}

		for _, message := range output.Messages {
			chn <- message
		}
	}
}

func main() {
	app := Initialise()

	app.configureSqs()
	pollingChannels := make(chan *awssqs.Message, 2)
	go app.pollMessages(pollingChannels)

	//Global message handler
	go func() {
		for message := range pollingChannels {
			app.logger.WithFields(log.Fields{
				"MessageId": aws.StringValue(message.MessageId),
				"Body":      aws.StringValue(message.Body),
			}).Info("Received message")
		}
	}()

	//Global message publisher
	go func() {
		for {
			app.sqs.SendMessage(&awssqs.SendMessageInput{
				MessageBody: aws.String("Hello"),
				QueueUrl:    aws.String("http://localstack:4566/000000000000/local-queue"),
			})
			time.Sleep(time.Second * 5)
		}
	}()

	app.Run()
}
