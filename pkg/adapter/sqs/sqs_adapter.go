package sqs

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/nferreira/adapter-sqs/pkg/adapter/sqs/connector"
	"github.com/nferreira/adapter/pkg/adapter"
	"github.com/nferreira/app/pkg/app"
	"github.com/nferreira/app/pkg/env"
	"github.com/nferreira/app/pkg/service"
)

const (
	Headers        = service.HeadersField
	Body           = service.BodyField
	CorrelationId  = "Correlation-Id"
	AdapterId      = "sqs"
	MessageIdField = "MessageId"
	MessageField   = "Message"
	ReplyTo        = "ReplyTo"
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
	numberOfWorks       int
	shutdownChannel     chan bool
	connector           *connector.Connector
}

func New() adapter.Adapter {
	return &Adapter{
		app:        nil,
		client:     newClient(),
		connector:  connector.New(),
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
	a.numberOfWorks = env.GetInt("SQS_NUMBER_OF_WORKERS", 3)
	for i := 0; i < a.numberOfWorks; i++ {
		go a.startWorker(ctx, i)
	}
	return nil
}

func (a *Adapter) Stop(_ context.Context) error {
	fmt.Printf("Shutdown requested...\n")
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

func (a *Adapter) startWorker(ctx context.Context, workerId int) {
	shutdownRequested := false
	for !shutdownRequested {
		select {
		case shutdownRequested = <-a.shutdownChannel:
			fmt.Printf("Worker-%d: Shutdown Requested: %t\n", workerId, shutdownRequested)
			return
		default:
			msgResult, err := a.readMessages(ctx)
			if err != nil {
				fmt.Printf("Worker-%d: Error reading messages. Reason: [%s]\n", workerId, err.Error())
			}
			for _, message := range msgResult.Messages {
				var correlationId *string
				var request *service.Params
				correlationId, request, err = a.parseMessage(workerId, message)
				if err != nil {
					fmt.Printf("Worker-%d: Failed to parse message, due to error: [%s]. Message Payload: [%s]\n",
						workerId,
						err.Error(),
						sqsMessageToString(message))
					a.client.DeleteMessage(&sqs.DeleteMessageInput{
						QueueUrl:      a.queueURL,
						ReceiptHandle: message.ReceiptHandle,
					})
					continue
				}

				result := a.executeBusinessService(ctx, a.businessService, correlationId, request)

				if r, found := result.Headers[ReplyTo]; found {
					if replyTo, isValid := r.(string); isValid {
						if err = a.replyTo(ctx, replyTo, result); err != nil {
							fmt.Printf("Worker-%d: Replying message to [%s] failed due to [%s]\n",
								workerId,
								replyTo,
								err.Error())
						}
					} else {
						fmt.Printf("Worker-%d: ReplyTo header was present but was not an string. It was %+v\n",
							workerId,
							r)
					}
				}

				if result.Error != nil {
					fmt.Printf("Worker-%d: Service execution failed, due to error: [%s]\n",
						workerId,
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
	fmt.Printf("Worker-%d: Stopping\n", workerId)
}

func (a *Adapter) parseMessage(workerId int, sqsMessage *sqs.Message) (correlationId *string, request *service.Params, err error) {
	if sqsMessage.Body == nil {
		fmt.Printf("Worker-%d: Message Body is empty\n", workerId)
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
		fmt.Printf("Worker-%d: Failed to unmarshal message. Error: %s\n",
			workerId,
			err.Error())
		return nil, nil, err
	}

	// we shall get the correlation id from on of these fields
	// as on our local enrionment the first one is used
	// but in prod using SNS the message is encoded in base64
	if parsedMessage[MessageIdField] == nil && sqsMessage.MessageId == nil {
		fmt.Printf("Worker-%d: Failed to get message id. It is empty\n",
			workerId)
		return nil, nil, err
	}

	// we shall get the correlation id from on of these fields
	// as on our local enrionment the first one is used
	// but in prod using SNS the message is encoded in base64
	if parsedMessage[MessageField] == nil &&
		parsedMessage[service.BodyField] == nil {
		fmt.Printf("Worker-%d: Failed to get message body. It is empty",
			workerId)
		return nil, nil, err
	}

	var messageId *string = nil
	var tmp string
	var found bool
	if tmp, found = parsedMessage[MessageIdField].(string); found {
		messageId = &tmp
	}
	if messageId == nil {
		messageId = sqsMessage.MessageId
	}
	correlationId = messageId

	var encodedMessage *string = nil

	if tmp, found = parsedMessage[MessageField].(string); found {
		encodedMessage = &tmp
		var messageBuffer []byte
		messageBuffer, err = base64.StdEncoding.DecodeString(*encodedMessage)
		if err != nil {
			return nil, nil, err
		}
		encodedMessage = aws.String(string(messageBuffer))
		if request, err = a.createAndPopulateRequest([]byte(*encodedMessage)); err != nil {
			return nil, nil, err
		}
		return correlationId, request, nil
	}

	request = ToServiceParam(&parsedMessage)

	return correlationId, request, nil
}

func ToServiceParam(o *map[string]interface{}) *service.Params {
	p := service.Params(*o)
	return &p
}

func (a *Adapter) createAndPopulateRequest(messageBuffer []byte) (request *service.Params, err error) {
	tmp := a.businessService.CreateRequest().(map[string]interface{})
	request = (*service.Params)(&tmp)
	if err = json.Unmarshal(messageBuffer, &request); err != nil {
		return nil, err
	}
	return request, nil
}

func (a *Adapter) executeBusinessService(ctx context.Context, businessService service.BusinessService, correlationId *string, params *service.Params) *service.Result {
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

func (a *Adapter) replyTo(ctx context.Context, replyTo string, result *service.Result) (err error) {
	response := make(map[string]interface{})
	response["code"] = result.Code
	response["headers"] = result.Headers
	response["body"] = result.Response
	if result.Error != nil {
		response["error"] = result.Error.Error()
	}
	var message []byte
	if message, err = json.Marshal(response); err == nil {
		err = a.connector.Publish(ctx, replyTo, make(map[string]*string), string(message))
	}
	return err
}

func sqsMessageToString(message *sqs.Message) string {
	result, err := json.Marshal(message)
	if err != nil {
		return fmt.Sprintf("<FAILED TO MARSHAL>: Reason: %s", err.Error())
	}
	return string(result)
}
