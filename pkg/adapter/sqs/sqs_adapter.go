package sqs

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/nferreira/adapter/pkg/adapter"
	"github.com/nferreira/app/pkg/app"
	"github.com/nferreira/app/pkg/env"
	"github.com/nferreira/app/pkg/service"
	"strings"
	"time"
)

const (
	Headers       = service.HeadersField
	Body          = service.BodyField
	CorrelationId = "Correlation-Id"
	AdapterId     = "sqs"
)

var (
	ErrBadPayload = errors.New("Bad payload")
)

type Params map[string]interface{}

type Adapter struct {
	app                 app.App
	client              *sqs.SQS
	queueURL            *string
	visibilityTimeout   int64
	maxNumberOfMessages int64
	readMessageTimeout  time.Duration
	businessService     service.BusinessService
	shutdownChannel     chan bool
}

func New() adapter.Adapter {
	return &Adapter{
		app:    nil,
		client: newClient(),
	}
}

func (a *Adapter) BindRules(rules map[adapter.BindingRule]service.BusinessService) {
	if len(rules) == 0 {
		panic("I have to have at least one queue to read from")
	}

	if len(rules) > 1 {
		panic("I only support reading from one queue at the moment")
	}

	for _, businessService := range rules {
		a.businessService = businessService
	}
}

func (a *Adapter) Start(ctx context.Context) error {
	a.app = configureApp(ctx)
	a.queueURL = configureSqs(ctx, a)
	a.visibilityTimeout = int64(env.GetInt("SQS_VISIBILITY_TIMEOUT", 60))
	a.maxNumberOfMessages = int64(env.GetInt("SQS_MAX_NUMBER_OF_MESSAGES", 10))
	a.readMessageTimeout = env.GetDuration("SQS_READ_MESSAGE_TIMEOUT_IN_SECONDS", time.Second*10)
	go a.startWorker(ctx)
	<-a.shutdownChannel
	return nil
}

func (a *Adapter) Stop(_ context.Context) error {
	a.shutdownChannel <- true
	return nil
}

func (a *Adapter) CheckHealth(ctx context.Context) error {
	return nil
}

func newClient() *sqs.SQS {
	return sqs.New(session.Must(session.NewSessionWithOptions(session.Options{
		SharedConfigState: session.SharedConfigEnable,
	})))
}

func configureApp(ctx context.Context) app.App {
	application := ctx.Value("app")
	switch application.(type) {
	case app.App:
		return application.(app.App)
	default:
		panic("I need an application to live!!!")
	}
}

func configureSqs(ctx context.Context, adapter *Adapter) *string {
	var urlResult *sqs.GetQueueUrlOutput
	var err error
	queueName := env.GetString("QUEUE_NAME", "")
	if len(strings.TrimSpace(queueName)) == 0 {
		panic("I need an queue name to start!!!")
	}
	if urlResult, err = adapter.client.GetQueueUrl(&sqs.GetQueueUrlInput{
		QueueName: &queueName,
	}); err != nil {
		panic(fmt.Sprintf("Failed to get QueueUrl %s", err.Error()))
	}
	return urlResult.QueueUrl
}

func (a *Adapter) startWorker(ctx context.Context) {
	shutdownRequested := false
	for !shutdownRequested {
		select {
		case shutdownRequested = <-a.shutdownChannel:
			fmt.Printf("Shutdown Requested: %t\n", shutdownRequested)
			return
		default:
			{
				fmt.Println("Trying to read messages...")
				newCtx, cancel := context.WithTimeout(ctx, a.readMessageTimeout)
				// TODO: check if cancel is being called
				defer cancel()
				msgResult, err := a.client.ReceiveMessageWithContext(newCtx, &sqs.ReceiveMessageInput{
					AttributeNames: []*string{
						aws.String(sqs.MessageSystemAttributeNameSentTimestamp),
					},
					MessageAttributeNames: []*string{
						aws.String(sqs.QueueAttributeNameAll),
					},
					QueueUrl:            a.queueURL,
					MaxNumberOfMessages: &a.maxNumberOfMessages,
					VisibilityTimeout:   &a.visibilityTimeout,
				})
				if err != nil {
					fmt.Printf("Error reading messages. Reason %s\n", err.Error())
				}
				numberOfMessages := len(msgResult.Messages)
				fmt.Printf("Got %d messages\n", numberOfMessages)
				for i, message := range msgResult.Messages {
					fmt.Printf("Processing %d of %d messages\n", i+1, numberOfMessages)
					var messageBuffer []byte
					messageBuffer, err = base64.StdEncoding.DecodeString(*message.Body)
					if err != nil {
						// TODO: handle this
					}

					request := a.businessService.CreateRequest()
					if err = json.Unmarshal(messageBuffer, &request); err != nil {
						// TODO: handle this
					}
					params := service.Params(request.(map[string]interface{}))
					result := a.businessService.Execute(ctx, params)

					if result.Error != nil {
						// TODO: handle
					} else {
						a.client.DeleteMessage(&sqs.DeleteMessageInput{
							QueueUrl:      a.queueURL,
							ReceiptHandle: message.ReceiptHandle,
						})
					}
				}
			}
		}
	}
}
