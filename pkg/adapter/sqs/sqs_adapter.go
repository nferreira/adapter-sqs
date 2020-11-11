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
	Headers        = service.HeadersField
	Body           = service.BodyField
	CorrelationId  = "Correlation-Id"
	AdapterId      = "sqs"
	MessageIdField = "MessageId"
	MessageField   = "Message"
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
	waitTimeSeconds     int64
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
	a.readMessageTimeout = env.GetDuration("SQS_READ_MESSAGE_TIMEOUT_IN_SECONDS", time.Second, time.Second*60)
	a.waitTimeSeconds = int64(env.GetInt("SQS_WAIT_TIME_SECONDS", 20))
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
			msgResult, err := a.readMessages(ctx)
			if err != nil {
				fmt.Printf("Error reading messages. Reason: [%s]\n", err.Error())
			}
			numberOfMessages := len(msgResult.Messages)
			fmt.Printf("Got %d messages\n", numberOfMessages)
			for i, message := range msgResult.Messages {
				fmt.Printf("Processing %d of %d messages\n", i+1, numberOfMessages)
				var correlationId *string
				var request service.Params
				correlationId, request, err = a.parseMessage(message)
				if err != nil {
					fmt.Printf("Failed to parse message, due to error: [%s]. Message Payload: [%s]\n",
						err.Error(),
						sqsMessageToString(message))
				}

				result := a.executeBusinessService(ctx, a.businessService, correlationId, request)

				if result.Error != nil {
					fmt.Printf("Service execution failed, due to error: [%s]\n",
						result.Error.Error())
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

func (a *Adapter) parseMessage(sqsMessage *sqs.Message) (correlationId *string, request service.Params, err error) {
	fmt.Printf("Received Message=[%s]\n", sqsMessageToString(sqsMessage))

	if sqsMessage.Body == nil {
		fmt.Printf("Message Body is empty\n")
		return nil, nil, err
	}

	var decodedBuff []byte
	if decodedBuff, err = base64.StdEncoding.DecodeString(*sqsMessage.Body); err != nil {
		// If for any reason the base64 decoding failed
		// let's assume it is a regular string encoded JSON
		decodedBuff = []byte(*sqsMessage.Body)
	}

	var parsedMessage map[string]interface{}
	err = json.Unmarshal(decodedBuff, &parsedMessage)
	if err != nil {
		fmt.Printf("Failed to unmarshal message. Error: %s\n", err.Error())
		return nil, nil, err
	}

	// we shall get the correlation id from on of these fields
	if parsedMessage[MessageIdField] == nil && sqsMessage.MessageId == nil {
		fmt.Println("Failed to get message id. It is empty")
		return nil, nil, err
	}

	if parsedMessage[MessageField] == nil {
		fmt.Println("Failed to get message body. It is empty")
		return nil, nil, err
	}

	var messageId *string = nil
	if tmp, found := parsedMessage[MessageIdField].(string); found {
		messageId = &tmp;
	}
	if messageId == nil {
		messageId = sqsMessage.MessageId
	}
	correlationId = messageId
	var encodedMessage = parsedMessage[MessageField].(string)
	var messageBuffer []byte
	messageBuffer, err = base64.StdEncoding.DecodeString(encodedMessage)
	if err != nil {
		return nil, nil, err
	}
	request, err = a.createAndPopulateRequest(messageBuffer)

	return correlationId, request, nil
}

func (a *Adapter) createAndPopulateRequest(messageBuffer []byte) (request service.Params, err error) {
	request = a.businessService.CreateRequest().(map[string]interface{})
	if err = json.Unmarshal(messageBuffer, &request); err != nil {
		return nil, err
	}
	return request, nil
}

func (a *Adapter) executeBusinessService(ctx context.Context, businessService service.BusinessService, correlationId *string, params service.Params) *service.Result {
	executionContext := service.NewExecutionContext(*correlationId, a.app)
	execCtx := context.WithValue(ctx, service.ExecutionContextKey, executionContext)
	return a.businessService.Execute(execCtx, params)
}

func (a *Adapter) readMessages(ctx context.Context) (*sqs.ReceiveMessageOutput, error) {
	newCtx, cancel := context.WithTimeout(ctx, a.readMessageTimeout)
	defer cancel()
	return a.client.ReceiveMessageWithContext(newCtx, &sqs.ReceiveMessageInput{
		AttributeNames: []*string{
			aws.String(sqs.MessageSystemAttributeNameSentTimestamp),
		},
		MessageAttributeNames: []*string{
			aws.String(sqs.QueueAttributeNameAll),
		},
		QueueUrl:            a.queueURL,
		MaxNumberOfMessages: &a.maxNumberOfMessages,
		VisibilityTimeout:   &a.visibilityTimeout,
		WaitTimeSeconds:     &a.waitTimeSeconds,
	})
}

func sqsMessageToString(message *sqs.Message) string {
	result, err := json.Marshal(message)
	if err != nil {
		return fmt.Sprintf("<FAILED TO MARSHAL>: Reason: %s", err.Error())
	}
	return string(result)
}
