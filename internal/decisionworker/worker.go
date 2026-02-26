package decisionworker

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/google/uuid"
	"github.com/sirupsen/logrus"
	"gitlab.com/emilshal/loadone-lambda/internal/rfq"
)

type Config struct {
	ActionQueueURL string
	DecisionMode   string
	DispatcherName string
	SourceName     string
}

var (
	sqsInitOnce sync.Once
	sqsClient   *sqs.SQS
	sqsInitErr  error
)

func loadConfig() Config {
	return Config{
		ActionQueueURL: strings.TrimSpace(os.Getenv("RFQ_ACTION_QUEUE_URL")),
		DecisionMode:   strings.TrimSpace(defaultIfEmpty(os.Getenv("DECISION_MODE"), "manual_review")),
		DispatcherName: strings.TrimSpace(os.Getenv("DISPATCHER_NAME")),
		SourceName:     strings.TrimSpace(defaultIfEmpty(os.Getenv("DECISION_SOURCE_NAME"), "loadone-decision-worker")),
	}
}

func LambdaHandler(ctx context.Context, sqsEvent events.SQSEvent) error {
	cfg := loadConfig()
	logrus.WithFields(logrus.Fields{
		"records":        len(sqsEvent.Records),
		"decisionMode":   cfg.DecisionMode,
		"hasActionQueue": cfg.ActionQueueURL != "",
	}).Info("Decision worker invoked")

	if len(sqsEvent.Records) == 0 {
		return nil
	}

	if cfg.ActionQueueURL != "" {
		if _, err := getSQSClient(); err != nil {
			return fmt.Errorf("initialize SQS client: %w", err)
		}
	}

	var errs []error
	for _, rec := range sqsEvent.Records {
		if err := processRecord(ctx, cfg, rec); err != nil {
			errs = append(errs, err)
		}
	}

	if len(errs) > 0 {
		return fmt.Errorf("decision worker failed %d/%d records (first: %v)", len(errs), len(sqsEvent.Records), errs[0])
	}
	return nil
}

func processRecord(ctx context.Context, cfg Config, rec events.SQSMessage) error {
	var in rfq.ParsedRFQMessage
	if err := json.Unmarshal([]byte(rec.Body), &in); err != nil {
		return fmt.Errorf("unmarshal parsed RFQ message %s: %w", rec.MessageId, err)
	}

	quoteID, err := in.QuoteID()
	if err != nil {
		return fmt.Errorf("parse quoteID from orderNumber=%q: %w", in.OrderNumber, err)
	}

	intent := buildIntent(cfg, in, quoteID)
	if intent.Action == rfq.ActionIgnore {
		logrus.WithFields(logrus.Fields{
			"quoteID": quoteID,
			"reason":  intent.Reason,
		}).Info("Decision worker ignoring RFQ")
		return nil
	}

	if cfg.ActionQueueURL == "" {
		logrus.WithFields(logrus.Fields{
			"quoteID": quoteID,
			"action":  intent.Action,
			"reason":  intent.Reason,
		}).Warn("RFQ action queue URL not configured; intent not sent")
		return nil
	}

	if err := sendIntent(ctx, cfg.ActionQueueURL, intent); err != nil {
		return fmt.Errorf("send action intent for quoteID=%d: %w", quoteID, err)
	}

	logrus.WithFields(logrus.Fields{
		"quoteID":  quoteID,
		"action":   intent.Action,
		"intentID": intent.IntentID,
		"queueURL": cfg.ActionQueueURL,
	}).Info("Decision worker emitted action intent")
	return nil
}

func buildIntent(cfg Config, in rfq.ParsedRFQMessage, quoteID int) rfq.ActionIntent {
	intent := rfq.NewIntent(rfq.ActionManualReview)
	intent.IntentID = uuid.NewString()
	intent.Source = cfg.SourceName
	intent.Reason = "skeleton default: route RFQ to manual review"
	intent.QuoteID = quoteID
	intent.OrderNumber = in.OrderNumber
	intent.MessageID = in.MessageID
	intent.ParserLogID = in.ParserLogID
	intent.Note = in.Notes
	intent.DispatcherName = cfg.DispatcherName
	intent.TrackingLink = in.ExternalLink
	intent.TrackingLinkRaw = in.ExternalLinkRaw
	intent.Metadata = map[string]string{
		"truckType":         in.SuggestedTruckSize,
		"originalTruckType": in.OriginalTruckSize,
		"pickup":            strings.TrimSpace(strings.Join([]string{in.PickupCity, in.PickupStateCode}, ", ")),
		"delivery":          strings.TrimSpace(strings.Join([]string{in.DeliveryCity, in.DeliveryStateCode}, ", ")),
	}

	switch strings.ToLower(strings.TrimSpace(cfg.DecisionMode)) {
	case "", "manual_review", "manual":
		// Safe default.
	case "ignore":
		intent.Action = rfq.ActionIgnore
		intent.Reason = "decision mode=ignore"
	case "bid_if_carrier_pay_present":
		if in.CarrierPay > 0 {
			rate := float64(in.CarrierPay)
			intent.Action = rfq.ActionBid
			intent.AllInRate = &rate
			intent.Reason = "skeleton rule: bid using parsed carrierPay"
		} else {
			intent.Action = rfq.ActionManualReview
			intent.Reason = "carrierPay missing; routed to manual review"
		}
	default:
		intent.Action = rfq.ActionManualReview
		intent.Reason = "unknown DECISION_MODE; routed to manual review"
	}

	return intent
}

func sendIntent(ctx context.Context, queueURL string, intent rfq.ActionIntent) error {
	client, err := getSQSClient()
	if err != nil {
		return err
	}

	body, err := json.Marshal(intent)
	if err != nil {
		return fmt.Errorf("marshal intent: %w", err)
	}

	input := &sqs.SendMessageInput{
		QueueUrl:    aws.String(queueURL),
		MessageBody: aws.String(string(body)),
	}
	if isFifoQueue(queueURL) {
		input.MessageGroupId = aws.String("loadone-rfq-actions")
		input.MessageDeduplicationId = aws.String(intent.IntentID)
	}

	ctxSend, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	_, err = client.SendMessageWithContext(ctxSend, input)
	return err
}

func getSQSClient() (*sqs.SQS, error) {
	sqsInitOnce.Do(func() {
		sess, err := session.NewSession()
		if err != nil {
			sqsInitErr = err
			return
		}
		sqsClient = sqs.New(sess)
	})
	return sqsClient, sqsInitErr
}

func isFifoQueue(queueURL string) bool {
	return strings.HasSuffix(strings.ToLower(strings.TrimSpace(queueURL)), ".fifo")
}

func defaultIfEmpty(v, fallback string) string {
	if strings.TrimSpace(v) == "" {
		return fallback
	}
	return v
}
