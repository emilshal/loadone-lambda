package bidworker

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-lambda-go/events"
	"github.com/sirupsen/logrus"
	"gitlab.com/emilshal/loadone-lambda/internal/loadoneapi"
	"gitlab.com/emilshal/loadone-lambda/internal/rfq"
)

type Config struct {
	DryRun                bool
	DefaultDispatcherName string
	RequestTimeout        time.Duration
}

func loadConfig() Config {
	return Config{
		DryRun:                parseBoolEnv("BID_WORKER_DRY_RUN", true),
		DefaultDispatcherName: strings.TrimSpace(os.Getenv("DISPATCHER_NAME")),
		RequestTimeout:        time.Duration(parseIntEnv("LOAD1_API_TIMEOUT_SECONDS", 10)) * time.Second,
	}
}

func LambdaHandler(ctx context.Context, sqsEvent events.SQSEvent) error {
	cfg := loadConfig()
	client := loadoneapi.NewClientFromEnv()

	logrus.WithFields(logrus.Fields{
		"records":        len(sqsEvent.Records),
		"dryRun":         cfg.DryRun,
		"hasCredentials": client.HasCredentials(),
	}).Info("Bid worker invoked")

	var errs []error
	for _, rec := range sqsEvent.Records {
		if err := processRecord(ctx, cfg, client, rec); err != nil {
			errs = append(errs, err)
		}
	}
	if len(errs) > 0 {
		return fmt.Errorf("bid worker failed %d/%d records (first: %v)", len(errs), len(sqsEvent.Records), errs[0])
	}
	return nil
}

func processRecord(ctx context.Context, cfg Config, client *loadoneapi.Client, rec events.SQSMessage) error {
	var intent rfq.ActionIntent
	if err := json.Unmarshal([]byte(rec.Body), &intent); err != nil {
		return fmt.Errorf("unmarshal action intent %s: %w", rec.MessageId, err)
	}

	if strings.TrimSpace(intent.IntentID) == "" {
		intent.IntentID = rec.MessageId
	}

	logFields := logrus.Fields{
		"intentID": intent.IntentID,
		"action":   intent.Action,
		"quoteID":  intent.QuoteID,
		"source":   intent.Source,
	}

	switch intent.Action {
	case rfq.ActionIgnore, "":
		logrus.WithFields(logFields).Info("Ignoring RFQ action intent")
		return nil
	case rfq.ActionManualReview:
		logrus.WithFields(logFields).Info("Manual review intent received; no API call made")
		return nil
	case rfq.ActionBid, rfq.ActionDecline, rfq.ActionRetractBid:
		// continue
	default:
		return fmt.Errorf("unsupported action %q", intent.Action)
	}

	if intent.QuoteID <= 0 {
		return fmt.Errorf("quoteID missing or invalid for intent %s", intent.IntentID)
	}

	accessKey := strings.TrimSpace(intent.AccessKey)
	if accessKey == "" {
		if v, err := loadoneapi.ExtractAccessKeyFromURL(intent.TrackingLinkRaw); err == nil {
			accessKey = v
		} else if v, err2 := loadoneapi.ExtractAccessKeyFromURL(intent.TrackingLink); err2 == nil {
			accessKey = v
		}
	}

	if accessKey == "" {
		logrus.WithFields(logFields).Warn("accessKey not present in intent and not extractable from tracking link; likely requires redirect resolution")
		return fmt.Errorf("accessKey unavailable for quoteID=%d", intent.QuoteID)
	}

	if cfg.DryRun {
		logrus.WithFields(logrus.Fields{
			"intentID":    intent.IntentID,
			"action":      intent.Action,
			"quoteID":     intent.QuoteID,
			"accessKey":   mask(accessKey),
			"allInRate":   intent.AllInRate,
			"note":        intent.Note,
			"trackingURL": firstNonEmpty(intent.TrackingLinkRaw, intent.TrackingLink),
		}).Info("BID_WORKER_DRY_RUN enabled; skipping Load One API request")
		return nil
	}

	if !client.HasCredentials() {
		return loadoneapi.ErrMissingCredentials
	}

	ctxReq, cancel := context.WithTimeout(ctx, cfg.RequestTimeout)
	defer cancel()

	switch intent.Action {
	case rfq.ActionBid:
		if intent.AllInRate == nil {
			return fmt.Errorf("bid intent missing allInRate for quoteID=%d", intent.QuoteID)
		}
		req := loadoneapi.CarrierQuoteBid{
			QuoteID:             intent.QuoteID,
			AccessKey:           accessKey,
			AllInRate:           *intent.AllInRate,
			Note:                intent.Note,
			AlternativePickup:   intent.AlternativePickup,
			AlternativeDelivery: intent.AlternativeDelivery,
			MilesFromPickup:     intent.MilesFromPickup,
			BoxLength:           intent.BoxLength,
			BoxWidth:            intent.BoxWidth,
			BoxHeight:           intent.BoxHeight,
			IsVehicleEmpty:      intent.IsVehicleEmpty,
			IsTeamDriver:        intent.IsTeamDriver,
			DispatcherName:      firstNonEmpty(intent.DispatcherName, cfg.DefaultDispatcherName),
		}
		if err := client.Bid(ctxReq, req); err != nil {
			return fmt.Errorf("load1 bid failed for quoteID=%d: %w", intent.QuoteID, err)
		}
	case rfq.ActionDecline:
		req := loadoneapi.CarrierQuoteDecline{
			QuoteID:   intent.QuoteID,
			AccessKey: accessKey,
			Note:      intent.Note,
		}
		if err := client.Decline(ctxReq, req); err != nil {
			return fmt.Errorf("load1 decline failed for quoteID=%d: %w", intent.QuoteID, err)
		}
	case rfq.ActionRetractBid:
		req := loadoneapi.CarrierQuoteRetractBid{
			QuoteID:   intent.QuoteID,
			AccessKey: accessKey,
		}
		if err := client.RetractBid(ctxReq, req); err != nil {
			return fmt.Errorf("load1 retract bid failed for quoteID=%d: %w", intent.QuoteID, err)
		}
	}

	logrus.WithFields(logFields).Info("Load One RFQ action API call succeeded")
	return nil
}

func parseBoolEnv(key string, fallback bool) bool {
	raw := strings.TrimSpace(strings.ToLower(os.Getenv(key)))
	if raw == "" {
		return fallback
	}
	switch raw {
	case "1", "true", "yes", "y", "on":
		return true
	case "0", "false", "no", "n", "off":
		return false
	default:
		return fallback
	}
}

func parseIntEnv(key string, fallback int) int {
	raw := strings.TrimSpace(os.Getenv(key))
	if raw == "" {
		return fallback
	}
	n, err := strconv.Atoi(raw)
	if err != nil || n <= 0 {
		return fallback
	}
	return n
}

func firstNonEmpty(values ...string) string {
	for _, v := range values {
		if strings.TrimSpace(v) != "" {
			return strings.TrimSpace(v)
		}
	}
	return ""
}

func mask(s string) string {
	s = strings.TrimSpace(s)
	if len(s) <= 8 {
		if s == "" {
			return ""
		}
		return "****"
	}
	return s[:4] + "..." + s[len(s)-4:]
}
