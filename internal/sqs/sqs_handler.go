package sqs

import (
	"encoding/json"
	"fmt"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
)

type SQSClient struct {
	svc      *sqs.SQS
	queueURL string
}

// NewSQSClient initializes a new SQSClient
func NewSQSClient(region, queueURL, accessKey, secretKey string) (*SQSClient, error) {
	sess, err := session.NewSession(&aws.Config{
		Region:      aws.String(region),
		Credentials: credentials.NewStaticCredentials(accessKey, secretKey, ""),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create session: %w", err)
	}

	svc := sqs.New(sess)
	return &SQSClient{
		svc:      svc,
		queueURL: queueURL,
	}, nil
}

// SendMessage sends a message to the SQS queue
func (client *SQSClient) SendMessage(messageBody map[string]string) error {
	messageJSON, err := json.Marshal(messageBody)
	if err != nil {
		return fmt.Errorf("failed to marshal message body: %w", err)
	}

	_, err = client.svc.SendMessage(&sqs.SendMessageInput{
		QueueUrl:    aws.String(client.queueURL),
		MessageBody: aws.String(string(messageJSON)),
	})
	if err != nil {
		return fmt.Errorf("failed to send message: %w", err)
	}

	return nil
}
