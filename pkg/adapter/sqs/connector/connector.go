package connector

import (
	"context"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/nferreira/app/pkg/env"
)

var (
	StringType = "String"
)

type Connector struct {
	sqs *sqs.SQS
}

func New() *Connector {
	config := aws.NewConfig().WithRegion(env.GetString("AWS_REGION", "us-east-1"))
	return &Connector{
		sqs: sqs.New(
			session.Must(session.NewSession()),
			config),
	}
}

func (c *Connector) Publish(ctx context.Context, destination string,
	messageAttributes map[string]*string, message string) (err error) {
	_, err = c.sqs.SendMessageWithContext(ctx, &sqs.SendMessageInput{
		MessageBody:       &message,
		QueueUrl:          &destination,
		MessageAttributes: toMessageAttributes(messageAttributes),
	})
	return err
}

func toMessageAttributes(attributes map[string]*string) map[string]*sqs.MessageAttributeValue {
	a := make(map[string]*sqs.MessageAttributeValue)

	for key, value := range attributes {
		a[key] = &sqs.MessageAttributeValue{
			DataType:    &StringType,
			StringValue: value,
		}
	}

	return a
}
