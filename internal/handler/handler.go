package handler

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"html"
	"log"
	"net/url"
	"os"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/google/uuid"
	"github.com/sirupsen/logrus"
	models "gitlab.com/emilshal/loadone-lambda/internal/model"
	"gitlab.com/emilshal/loadone-lambda/internal/parser"
	"gitlab.com/emilshal/loadone-lambda/internal/rfq"
	"gitlab.com/emilshal/loadone-lambda/internal/s3client"
	config "gitlab.com/emilshal/loadone-lambda/pkg"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
)

const (
	externalLinkMaxLength = 255
)

var (
	db          *gorm.DB
	initDB      sync.Once
	sqsClient   *sqs.SQS
	logFilePath = "/tmp/handler_logs.log"
)

var logRotateMutex sync.Mutex
var currentLogFile *os.File
var (
	rePostedBy     = regexp.MustCompile(`(?i)\bPosted\s+By\s*:\s*([^\n;]+)`)
	reLoadPostedBy = regexp.MustCompile(`(?i)\bLoad\s+posted\s+by\s*:\s*([^\n]+)`)
	// Simple, robust MC extractor
	reMC    = regexp.MustCompile(`(?i)\bMC#\s*([0-9]{3,})\b`)
	rePhone = regexp.MustCompile(`(?i)\bPhone\s*:\s*([+()0-9\-\.\s]{7,})`)
)

var (
	easternLocationOnce sync.Once
	easternLocation     *time.Location
)

type loadOneWebhookEvent struct {
	ID              string             `json:"Id"`
	Object          string             `json:"Object"`
	Type            string             `json:"Type"`
	CreatedDateTime string             `json:"CreatedDateTime"`
	Livemode        bool               `json:"Livemode"`
	Data            loadOneWebhookData `json:"Data"`
}

type loadOneWebhookData struct {
	QuoteID              int                     `json:"QuoteID"`
	AccessKey            string                  `json:"AccessKey"`
	ExpiryDateTime       string                  `json:"ExpiryDateTime"`
	RequestedVehicleSize string                  `json:"RequestedVehicleSize"`
	Contacts             []loadOneWebhookContact `json:"Contacts"`
	Notes                []string                `json:"Notes"`
	Stops                []loadOneWebhookStop    `json:"Stops"`
	FreightDetails       []loadOneFreightDetail  `json:"FreightDetails"`
}

type loadOneWebhookContact struct {
	FirstName string `json:"FirstName"`
	LastName  string `json:"LastName"`
	Email     string `json:"Email"`
	Phone     string `json:"Phone"`
	Type      string `json:"Type"`
	Ext       string `json:"Ext"`
}

type loadOneWebhookStop struct {
	Type              string                 `json:"Type"`
	Location          loadOneWebhookLocation `json:"Location"`
	ScheduledDateTime string                 `json:"ScheduledDateTime"`
	LatestDateTime    string                 `json:"LatestDateTime"`
	Verb              string                 `json:"Verb"`
}

type loadOneWebhookLocation struct {
	City    string `json:"City"`
	State   string `json:"State"`
	Zip     string `json:"Zip"`
	Country string `json:"Country"`
}

type loadOneFreightDetail struct {
	Pieces    int     `json:"Pieces"`
	Weight    float64 `json:"Weight"`
	Length    float64 `json:"Length"`
	Width     float64 `json:"Width"`
	Height    float64 `json:"Height"`
	Stackable bool    `json:"Stackable"`
	Hazardous bool    `json:"Hazardous"`
}

func SetDB(database *gorm.DB) {
	db = database
}

func init() {
	log.Println("Initializing SQS client")
	logrus.Info("Initializing SQS client (logrus mirror)")

	sess := session.Must(session.NewSession())
	sqsClient = sqs.New(sess, aws.NewConfig().WithMaxRetries(3))

	f, err := os.OpenFile(logFilePath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		log.Fatalf("open log: %v", err)
	}
	currentLogFile = f
	log.SetOutput(currentLogFile)
	log.Println("Initialized log file for Lambda")
	logrus.Info("Initialized log file for Lambda (logrus mirror)")
}

func InitializeDB() (*gorm.DB, error) {
	var err error
	initDB.Do(func() {
		dsn := strings.TrimSpace(config.AppConfig.MySQLDSN)
		if dsn == "" {
			err = fmt.Errorf("MYSQL_DSN empty")
			return
		}
		db, err = gorm.Open(mysql.Open(dsn), &gorm.Config{})
		if err != nil {
			return
		}
		sqlDB, err2 := db.DB()
		if err2 != nil {
			err = err2
			return
		}
		if pingErr := sqlDB.Ping(); pingErr != nil {
			err = pingErr
			return
		}
		sqlDB.SetMaxOpenConns(10)
		sqlDB.SetMaxIdleConns(5)
		sqlDB.SetConnMaxLifetime(5 * time.Minute)
	})
	return db, err
}
func LambdaHandler(ctx context.Context, request events.APIGatewayProxyRequest) (events.APIGatewayProxyResponse, error) {
	defer func() {
		if r := recover(); r != nil {
			logrus.Errorf("panic in LambdaHandler: %v", r)
		}
	}()

	log.Println("Mailgun route accessed (LoadOne parser)")
	logrus.Info("Mailgun route accessed (LoadOne parser)")

	queueURL := strings.TrimSpace(config.AppConfig.AWSSQSQueueURL)
	logrus.WithFields(logrus.Fields{
		"queueURL": queueURL,
	}).Info("Resolved SQS queue URL")

	echoMode := request.QueryStringParameters["echo"] == "1" ||
		strings.EqualFold(header(request, "X-Echo"), "1") ||
		strings.EqualFold(os.Getenv("RAW_ECHO"), "1")
	isDryRun := request.QueryStringParameters["dry_run"] == "1" ||
		strings.EqualFold(header(request, "X-Dry-Run"), "1")

	logrus.WithFields(logrus.Fields{
		"echoMode":  echoMode,
		"dry_run":   isDryRun,
		"queueURL":  queueURL,
		"isFIFO":    isFifoQueue(queueURL),
		"awsRegion": os.Getenv("AWS_REGION"),
	}).Info("Ingress toggles")
	logrus.WithFields(logrus.Fields{
		"httpMethod":      request.HTTPMethod,
		"path":            request.Path,
		"isBase64Encoded": request.IsBase64Encoded,
		"bodyBytes":       len(request.Body),
		"contentType":     header(request, "Content-Type"),
		"userAgent":       header(request, "User-Agent"),
		"hasHeaders":      len(request.Headers) > 0,
	}).Info("Inbound request metadata")

	// Decode body
	var err error
	var decodedBody string
	if request.IsBase64Encoded {
		decodedBody, err = DecodeBase64(request.Body)
		if err != nil {
			logrus.WithError(err).WithField("rawBodyPreview", truncateForLog(request.Body, 1000)).Error("Invalid Base64 payload")
			return events.APIGatewayProxyResponse{StatusCode: 400, Body: "Invalid Base64 data"}, nil
		}
	} else {
		decodedBody = request.Body
	}
	logrus.WithFields(logrus.Fields{
		"decodedBytes": len(decodedBody),
		"preview":      truncateForLog(decodedBody, 1200),
	}).Info("Decoded request body preview")

	contentType := strings.TrimSpace(strings.ToLower(header(request, "Content-Type")))
	if looksLikeJSONWebhook(contentType, decodedBody) {
		return handleLoadOneJSONWebhook(ctx, queueURL, decodedBody, isDryRun)
	}

	// Parse form data
	formData, err := url.ParseQuery(decodedBody)
	if err != nil || len(formData) == 0 {
		logrus.WithError(err).WithField("decodedBodyPreview", truncateForLog(decodedBody, 1200)).Warn("Failed to parse form data")
		return events.APIGatewayProxyResponse{StatusCode: 400, Body: "Invalid or empty form data"}, nil
	}
	logFormDataSummary(formData)

	subject := formData.Get("subject")
	bodyHTML := formData.Get("body-html")
	bodyPlain := formData.Get("body-plain")
	messageID := formData.Get("Message-Id")
	if messageID == "" {
		if v := strings.TrimSpace(formData.Get("message-id")); v != "" {
			messageID = v
		}
	}
	contactName := parser.ExtractOfferedByName(bodyHTML, bodyPlain)
	replyTo := strings.TrimSpace(headerKV(formData, "Reply-To"))
	if replyTo == "" {
		replyTo = strings.TrimSpace(headerKV(formData, "from"))
	}

	// Fallbacks from Mailgun helpers
	strippedHTML := strings.TrimSpace(formData.Get("stripped-html"))
	strippedText := strings.TrimSpace(formData.Get("stripped-text"))
	if strippedHTML != "" && bodyHTML == "" {
		bodyHTML = strippedHTML
	}
	if strippedText != "" && bodyPlain == "" {
		bodyPlain = strippedText
	}

	if bodyHTML == "" && bodyPlain == "" {
		logrus.WithFields(logrus.Fields{
			"hasBodyHTML":     bodyHTML != "",
			"hasBodyPlain":    bodyPlain != "",
			"hasStrippedHTML": strippedHTML != "",
			"hasStrippedText": strippedText != "",
		}).Warn("Inbound form had no email body fields")
		return events.APIGatewayProxyResponse{StatusCode: 400, Body: "Empty body content"}, nil
	}

	// Echo mode
	truncate := func(s string, n int) string {
		if len(s) <= n {
			return s
		}
		return s[:n] + "...(truncated)"
	}
	if echoMode {
		logrus.Info("=== ECHO MODE: printing raw Mailgun fields and returning them ===")
		logrus.Infof("[subject]   %s", subject)
		logrus.Infof("[messageID] %s", messageID)
		logrus.Infof("[replyTo]   %s", replyTo)
		logrus.Infof("[body-plain] (%d bytes)\n%s", len(bodyPlain), truncate(bodyPlain, 8000))
		logrus.Infof("[body-html ] (%d bytes)\n%s", len(bodyHTML), truncate(bodyHTML, 8000))

		out := map[string]interface{}{
			"subject":    subject,
			"messageID":  messageID,
			"replyTo":    replyTo,
			"bodyPlain":  bodyPlain,
			"bodyHTML":   bodyHTML,
			"bytesPlain": len(bodyPlain),
			"bytesHTML":  len(bodyHTML),
		}
		js, _ := json.MarshalIndent(out, "", "  ")
		rotateLog()
		return events.APIGatewayProxyResponse{
			StatusCode: 200,
			Headers:    map[string]string{"Content-Type": "application/json"},
			Body:       string(js),
		}, nil
	}

	// Parse email using LoadOneParser
	loadOneParser := &parser.LoadOneParser{}
	parserResult, err := loadOneParser.Parse(bodyHTML, bodyPlain)
	if err != nil {
		logrus.WithError(err).Error("Parse failed")
		return events.APIGatewayProxyResponse{StatusCode: 500, Body: "Failed to parse email"}, nil
	}

	// Use time.Now (optionally replaced with MySQL NOW())
	mysqlTime := time.Now()
	var parserLogID int64

	// Optional DB logging (ParserLog) — skip entirely on dry-run
	if !isDryRun && strings.TrimSpace(config.AppConfig.MySQLDSN) != "" {
		if db, err := InitializeDB(); err == nil && db != nil {
			if err := db.Raw("SELECT NOW()").Scan(&mysqlTime).Error; err != nil {
				logrus.WithError(err).Warn("Using local time (MySQL NOW() failed)")
			}
			pl := &models.ParserLog{
				ParserID:   7,
				ParserType: "mail",
				BodyHtml:   bodyHTML,
				BodyPlain:  bodyPlain,
				MessageID:  messageID,
				Subject:    subject,
				CreatedAt:  mysqlTime,
				UpdatedAt:  mysqlTime,
			}
			if err := db.Create(pl).Error; err != nil {
				logrus.WithError(err).Warn("Failed to create parser log (continuing)")
			} else {
				parserLogID = pl.ID
			}
		} else {
			logrus.WithError(err).Warn("DB unavailable, continuing without DB")
		}
	} else if !isDryRun {
		logrus.Warn("MYSQL_DSN empty; continuing without DB")
	}

	// Convenience locals
	o := parserResult.Order
	loc := parserResult.Location
	it := parserResult.Item

	pickupDateValue, pickupDateDisplay, pickupDateOK := normalizeEasternTimestamp(parserResult.PickupDateStr)
	if !pickupDateOK && parserResult.PickupDateStr != "" {
		logrus.WithField("pickupDateRaw", parserResult.PickupDateStr).Warn("Failed to normalize pickup date; leaving blank for DB payload")
	}
	deliveryDateValue, deliveryDateDisplay, deliveryDateOK := normalizeEasternTimestamp(parserResult.DeliveryDateStr)
	if !deliveryDateOK && parserResult.DeliveryDateStr != "" {
		logrus.WithField("deliveryDateRaw", parserResult.DeliveryDateStr).Warn("Failed to normalize delivery date; leaving blank for DB payload")
	}

	originalExternalLink := strings.TrimSpace(parserResult.ExternalLink)
	externalLink := normalizeExternalLink(originalExternalLink)
	if externalLink != originalExternalLink {
		logrus.WithFields(logrus.Fields{
			"originalLen":   len(originalExternalLink),
			"normalizedLen": len(externalLink),
		}).Debug("Normalized external link value")
	}

	data := map[string]interface{}{
		"orderNumber":         o.OrderNumber,
		"pickupLocation":      buildLabel(loc.PickupPostalCode, loc.PickupCity, loc.PickupStateCode, loc.PickupCountryName),
		"deliveryLocation":    buildLabel(loc.DeliveryPostalCode, loc.DeliveryCity, loc.DeliveryStateCode, loc.DeliveryCountryName),
		"pickupDate":          pickupDateValue,
		"pickupDateDisplay":   pickupDateDisplay,
		"deliveryDate":        deliveryDateValue,
		"deliveryDateDisplay": deliveryDateDisplay,
		"suggestedTruckSize":  o.SuggestedTruckSize,
		"truckTypeID":         o.TruckTypeID,
		"originalTruckSize":   o.OriginalTruckSize,
		"notes":               o.Notes,
		"pickupZip":           loc.PickupPostalCode,
		"pickupCity":          loc.PickupCity,
		"pickupState":         loc.PickupState,
		"pickupStateCode":     loc.PickupStateCode,
		"pickupCountry":       loc.PickupCountryName,
		"pickupCountryCode":   loc.PickupCountryCode,
		"pickupCountryName":   loc.PickupCountryName,
		"deliveryZip":         loc.DeliveryPostalCode,
		"deliveryCity":        loc.DeliveryCity,
		"deliveryState":       loc.DeliveryState,
		"deliveryStateCode":   loc.DeliveryStateCode,
		"deliveryCountry":     loc.DeliveryCountryName,
		"deliveryCountryCode": loc.DeliveryCountryCode,
		"deliveryCountryName": loc.DeliveryCountryName,
		"estimatedMiles":      int(loc.EstimatedMiles),
		"orderTypeID":         7,
		"length":              it.Length,
		"width":               it.Width,
		"height":              it.Height,
		"weight":              it.Weight,
		"pieces":              it.Pieces,
		"stackable":           it.Stackable,
		"hazardous":           it.Hazardous,
		"replyTo":             replyTo,
		"subject":             subject,
		"bodyHTML":            bodyHTML,
		"bodyPlain":           bodyPlain,
		"messageID":           messageID,
		"parserLogID":         parserLogID,
		"createdAt":           mysqlTime,
		"updatedAt":           mysqlTime,
		"carrierPay":          o.CarrierPay,
		"carrierPayRate":      o.CarrierPayRate,
		"externalLink":        externalLink,
		"externalLinkRaw":     originalExternalLink,
		"brokerName":          contactName,
	}

	pretty, _ := json.MarshalIndent(data, "", "  ")
	logrus.Infof("[PREVIEW] SQS payload:\n%s", string(pretty))

	if isDryRun {
		return events.APIGatewayProxyResponse{
			StatusCode: 200,
			Body:       string(pretty),
			Headers:    map[string]string{"Content-Type": "application/json"},
		}, nil
	}

	// Normal path: send to SQS
	if queueURL == "" {
		logrus.Error("AWSSQSQueueURL is empty; refusing to send")
		return events.APIGatewayProxyResponse{StatusCode: 500, Body: "SQS queue URL not configured"}, nil
	}

	messageBodyBytes, err := json.Marshal(data)
	if err != nil {
		logrus.WithError(err).Error("Marshal to JSON failed")
		return events.APIGatewayProxyResponse{StatusCode: 500, Body: "Failed to encode message"}, nil
	}
	if parserLogID != 0 {
		// Avoid persisting raw email bodies inside parser_log.parsed_data
		sanitized := make(map[string]interface{}, len(data))
		for k, v := range data {
			if k == "bodyHTML" || k == "bodyPlain" {
				continue
			}
			sanitized[k] = v
		}
		sanitizedBytes, err := json.Marshal(sanitized)
		if err != nil {
			logrus.WithError(err).Warn("Failed to sanitize parsed_data payload")
		} else if err := updateParserLogParsedData(parserLogID, string(sanitizedBytes)); err != nil {
			logrus.WithError(err).Warn("Failed to update parser log parsed_data")
		}
	}
	logrus.WithFields(logrus.Fields{
		"bytes": len(messageBodyBytes),
		"fifo":  isFifoQueue(queueURL),
	}).Info("Prepared compact SQS message body")

	ctxSend, cancel := context.WithTimeout(ctx, 7*time.Second)
	defer cancel()

	out, err := sendToSQS(ctxSend, queueURL, messageBodyBytes)
	if err != nil {
		logrus.WithError(err).Error("SQS send failed")
		return events.APIGatewayProxyResponse{StatusCode: 500, Body: "Failed to send message"}, nil
	}

	// CRITICAL: Make it obvious in CloudWatch when a message is actually SENT
	logrus.WithFields(logrus.Fields{
		"messageId":        aws.StringValue(out.MessageId),
		"md5OfMessageBody": aws.StringValue(out.MD5OfMessageBody),
		"queueURL":         queueURL,
		"isFIFO":           isFifoQueue(queueURL),
	}).Info("SQS send succeeded")

	rotateLog()
	return events.APIGatewayProxyResponse{
		StatusCode: 200,
		Body:       fmt.Sprintf("Email parsed and sent to SQS (LoadOne). MessageId=%s", aws.StringValue(out.MessageId)),
	}, nil
}

func normalizeEasternTimestamp(raw string) (string, string, bool) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return "", "", false
	}

	cleaned := raw
	if idx := strings.Index(cleaned, "("); idx >= 0 {
		cleaned = strings.TrimSpace(cleaned[:idx])
	}

	if cleaned == "" {
		return "", raw, false
	}

	if ts, ok := parser.ParseUSNYToUTC(cleaned); ok {
		eastern := ts.In(loadEasternLocation())
		return eastern.Format("2006-01-02 15:04:05"), raw, true
	}

	fallbackLayouts := []string{
		"2006-01-02 15:04:05",
		"2006-01-02 15:04",
		"2006-1-2 15:04:05",
		"2006-1-2 15:04",
		"01/02/2006 15:04:05",
		"01/02/2006 15:04",
		"1/2/2006 15:04:05",
		"1/2/2006 15:04",
		time.RFC3339,
	}

	for _, layout := range fallbackLayouts {
		if ts, err := time.ParseInLocation(layout, cleaned, loadEasternLocation()); err == nil {
			return ts.Format("2006-01-02 15:04:05"), raw, true
		}
	}

	return "", raw, false
}

func looksLikeJSONWebhook(contentType, body string) bool {
	if strings.Contains(contentType, "application/json") {
		return true
	}
	trimmed := strings.TrimSpace(body)
	return strings.HasPrefix(trimmed, "{") || strings.HasPrefix(trimmed, "[")
}

func handleLoadOneJSONWebhook(ctx context.Context, queueURL, decodedBody string, isDryRun bool) (events.APIGatewayProxyResponse, error) {
	event, err := parseLoadOneWebhookEvent(decodedBody)
	if err != nil {
		logrus.WithError(err).Warn("Invalid JSON payload")
		return events.APIGatewayProxyResponse{StatusCode: 400, Body: "Invalid JSON payload"}, nil
	}

	msg, err := normalizeLoadOneWebhook(event)
	if err != nil {
		logrus.WithError(err).WithField("eventType", event.Type).Warn("Unsupported Load1 webhook payload")
		return events.APIGatewayProxyResponse{StatusCode: 400, Body: err.Error()}, nil
	}

	logrus.WithFields(logrus.Fields{
		"eventID":      event.ID,
		"eventType":    event.Type,
		"quoteID":      msg.OrderNumber,
		"accessKey":    maskSecret(msg.AccessKey),
		"pickup":       strings.TrimSpace(strings.Join([]string{msg.PickupCity, msg.PickupStateCode}, ", ")),
		"delivery":     strings.TrimSpace(strings.Join([]string{msg.DeliveryCity, msg.DeliveryStateCode}, ", ")),
		"stopsPresent": msg.PickupCity != "" || msg.DeliveryCity != "",
	}).Info("Normalized Load1 JSON webhook")

	payload, err := json.Marshal(msg)
	if err != nil {
		logrus.WithError(err).Error("Marshal Load1 payload to JSON failed")
		return events.APIGatewayProxyResponse{StatusCode: 500, Body: "Failed to encode message"}, nil
	}

	if isDryRun {
		pretty, _ := json.MarshalIndent(msg, "", "  ")
		return events.APIGatewayProxyResponse{
			StatusCode: 200,
			Body:       string(pretty),
			Headers:    map[string]string{"Content-Type": "application/json"},
		}, nil
	}

	if queueURL == "" {
		logrus.Error("AWSSQSQueueURL is empty; refusing to send")
		return events.APIGatewayProxyResponse{StatusCode: 500, Body: "SQS queue URL not configured"}, nil
	}

	ctxSend, cancel := context.WithTimeout(ctx, 7*time.Second)
	defer cancel()

	out, err := sendToSQS(ctxSend, queueURL, payload)
	if err != nil {
		logrus.WithError(err).Error("SQS send failed for Load1 JSON webhook")
		return events.APIGatewayProxyResponse{StatusCode: 500, Body: "Failed to send message"}, nil
	}

	logrus.WithFields(logrus.Fields{
		"eventID":          event.ID,
		"eventType":        event.Type,
		"quoteID":          msg.OrderNumber,
		"messageId":        aws.StringValue(out.MessageId),
		"queueURL":         queueURL,
		"md5OfMessageBody": aws.StringValue(out.MD5OfMessageBody),
		"isFIFO":           isFifoQueue(queueURL),
	}).Info("Load1 JSON webhook sent to SQS")

	return events.APIGatewayProxyResponse{
		StatusCode: 200,
		Body:       fmt.Sprintf("Load1 webhook accepted. MessageId=%s", aws.StringValue(out.MessageId)),
	}, nil
}

func parseLoadOneWebhookEvent(decodedBody string) (loadOneWebhookEvent, error) {
	var event loadOneWebhookEvent
	if err := json.Unmarshal([]byte(decodedBody), &event); err != nil {
		return loadOneWebhookEvent{}, err
	}
	return event, nil
}

func normalizeLoadOneWebhook(event loadOneWebhookEvent) (rfq.ParsedRFQMessage, error) {
	quoteID := event.Data.QuoteID
	if quoteID <= 0 {
		return rfq.ParsedRFQMessage{}, fmt.Errorf("quoteID missing or invalid")
	}

	pickupStop := firstStopByType(event.Data.Stops, "pickup")
	deliveryStop := firstStopByType(event.Data.Stops, "delivery")
	firstContact := firstContact(event.Data.Contacts)
	firstFreight := firstFreightDetail(event.Data.FreightDetails)

	pickupValue, pickupDisplay := normalizeRFC3339Field(pickupStop.ScheduledDateTime)
	deliveryValue, deliveryDisplay := normalizeRFC3339Field(deliveryStop.ScheduledDateTime)

	noteParts := make([]string, 0, len(event.Data.Notes)+2)
	for _, n := range event.Data.Notes {
		if strings.TrimSpace(n) != "" {
			noteParts = append(noteParts, strings.TrimSpace(n))
		}
	}
	if strings.TrimSpace(event.Type) != "" {
		noteParts = append(noteParts, "eventType="+strings.TrimSpace(event.Type))
	}
	if strings.TrimSpace(event.Data.ExpiryDateTime) != "" {
		noteParts = append(noteParts, "expiresAt="+strings.TrimSpace(event.Data.ExpiryDateTime))
	}

	return rfq.ParsedRFQMessage{
		OrderNumber:         strconv.Itoa(quoteID),
		Subject:             fmt.Sprintf("Load1 webhook: %s", strings.TrimSpace(event.Type)),
		MessageID:           firstNonEmpty(event.ID, strconv.Itoa(quoteID)),
		ReplyTo:             firstContact.Email,
		BrokerName:          strings.TrimSpace(strings.Join([]string{firstContact.FirstName, firstContact.LastName}, " ")),
		SuggestedTruckSize:  strings.TrimSpace(event.Data.RequestedVehicleSize),
		OriginalTruckSize:   strings.TrimSpace(event.Data.RequestedVehicleSize),
		Notes:               strings.Join(noteParts, "\n"),
		PickupCity:          strings.TrimSpace(pickupStop.Location.City),
		PickupStateCode:     strings.TrimSpace(pickupStop.Location.State),
		PickupZip:           strings.TrimSpace(pickupStop.Location.Zip),
		PickupDate:          pickupValue,
		PickupDateDisplay:   firstNonEmpty(pickupDisplay, strings.TrimSpace(pickupStop.ScheduledDateTime)),
		DeliveryCity:        strings.TrimSpace(deliveryStop.Location.City),
		DeliveryStateCode:   strings.TrimSpace(deliveryStop.Location.State),
		DeliveryZip:         strings.TrimSpace(deliveryStop.Location.Zip),
		DeliveryDate:        deliveryValue,
		DeliveryDateDisplay: firstNonEmpty(deliveryDisplay, strings.TrimSpace(deliveryStop.ScheduledDateTime)),
		Length:              firstFreight.Length,
		Width:               firstFreight.Width,
		Height:              firstFreight.Height,
		Weight:              firstFreight.Weight,
		Pieces:              firstFreight.Pieces,
		Stackable:           firstFreight.Stackable,
		Hazardous:           firstFreight.Hazardous,
		AccessKey:           strings.TrimSpace(event.Data.AccessKey),
	}, nil
}

func normalizeRFC3339Field(raw string) (string, string) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return "", ""
	}
	if ts, err := time.Parse(time.RFC3339, raw); err == nil {
		local := ts.In(loadEasternLocation())
		return local.Format("2006-01-02 15:04:05"), raw
	}
	return "", raw
}

func firstStopByType(stops []loadOneWebhookStop, stopType string) loadOneWebhookStop {
	stopType = strings.TrimSpace(strings.ToLower(stopType))
	for _, stop := range stops {
		if strings.TrimSpace(strings.ToLower(stop.Type)) == stopType {
			return stop
		}
	}
	if len(stops) > 0 {
		return stops[0]
	}
	return loadOneWebhookStop{}
}

func firstContact(contacts []loadOneWebhookContact) loadOneWebhookContact {
	if len(contacts) == 0 {
		return loadOneWebhookContact{}
	}
	return contacts[0]
}

func firstFreightDetail(items []loadOneFreightDetail) loadOneFreightDetail {
	if len(items) == 0 {
		return loadOneFreightDetail{}
	}
	return items[0]
}

func maskSecret(s string) string {
	s = strings.TrimSpace(s)
	if s == "" {
		return ""
	}
	if len(s) <= 8 {
		return "****"
	}
	return s[:4] + "..." + s[len(s)-4:]
}

func firstNonEmpty(values ...string) string {
	for _, v := range values {
		if strings.TrimSpace(v) != "" {
			return strings.TrimSpace(v)
		}
	}
	return ""
}

func truncateForLog(s string, n int) string {
	if n <= 0 {
		return ""
	}
	if len(s) <= n {
		return s
	}
	return s[:n] + "...(truncated)"
}

func logFormDataSummary(form url.Values) {
	keys := make([]string, 0, len(form))
	for k := range form {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	summary := make(map[string]string, len(keys))
	for _, k := range keys {
		v := strings.TrimSpace(form.Get(k))
		summary[k] = fmt.Sprintf("len=%d preview=%q", len(v), truncateForLog(v, 220))
	}

	logrus.WithFields(logrus.Fields{
		"formKeyCount": len(keys),
		"formKeys":     keys,
		"formSummary":  summary,
	}).Info("Inbound Mailgun form fields")
}

func normalizeExternalLink(raw string) string {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return ""
	}

	if decoded, ok := decodeSendgridLink(raw); ok {
		if len(decoded) <= externalLinkMaxLength {
			return decoded
		}
		return truncateString(decoded, externalLinkMaxLength)
	}

	if len(raw) <= externalLinkMaxLength {
		return raw
	}

	return truncateString(raw, externalLinkMaxLength)
}

func decodeSendgridLink(raw string) (string, bool) {
	u, err := url.Parse(raw)
	if err != nil {
		return "", false
	}
	if !strings.Contains(u.Host, "sendgrid.net") {
		return "", false
	}

	upn := u.Query().Get("upn")
	if upn == "" {
		return "", false
	}

	if idx := strings.Index(upn, "__"); idx >= 0 {
		upn = upn[:idx]
	}
	if idx := strings.Index(upn, "."); idx >= 0 {
		upn = upn[idx+1:]
	}
	if upn == "" {
		return "", false
	}

	replaced := make([]byte, 0, len(upn)*2)
	for i := 0; i < len(upn); {
		if upn[i] == '-' && i+2 < len(upn) && isHex(upn[i+1]) && isHex(upn[i+2]) {
			replaced = append(replaced, '%', upn[i+1], upn[i+2])
			i += 3
			continue
		}
		replaced = append(replaced, upn[i])
		i++
	}

	decoded, err := url.QueryUnescape(string(replaced))
	if err != nil {
		return "", false
	}

	decoded = strings.TrimSpace(decoded)
	if decoded == "" {
		return "", false
	}

	return decoded, true
}

func truncateString(s string, max int) string {
	if len(s) <= max {
		return s
	}
	if max <= 0 {
		return ""
	}
	for i := range s {
		if i >= max {
			return s[:i]
		}
	}
	if len(s) > max {
		return s[:max]
	}
	return s
}

func isHex(b byte) bool {
	return ('0' <= b && b <= '9') || ('a' <= b && b <= 'f') || ('A' <= b && b <= 'F')
}

func loadEasternLocation() *time.Location {
	easternLocationOnce.Do(func() {
		if loc, err := time.LoadLocation("America/New_York"); err == nil {
			easternLocation = loc
			return
		}
		easternLocation = time.FixedZone("EST", -5*3600)
	})
	return easternLocation
}

func rotateLog() {
	logRotateMutex.Lock()
	timestamp := time.Now().Format("2006-01-02-15-04-05")
	oldLogPath := fmt.Sprintf("%s.%s", logFilePath, timestamp)

	// If current log does not exist, nothing to rotate
	if _, err := os.Stat(logFilePath); os.IsNotExist(err) {
		logRotateMutex.Unlock()
		return
	}

	// Close current file handle before rotation to avoid descriptor leaks
	if currentLogFile != nil {
		_ = currentLogFile.Close()
		currentLogFile = nil
	}

	// Rotate current log file
	if err := os.Rename(logFilePath, oldLogPath); err != nil {
		// Best effort: if rename fails, try to reopen current log to keep logging
		if f, openErr := os.OpenFile(logFilePath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644); openErr == nil {
			currentLogFile = f
			log.SetOutput(currentLogFile)
		}
		logRotateMutex.Unlock()
		return
	}

	// Open a fresh log file and set as output
	if f, err := os.OpenFile(logFilePath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644); err == nil {
		currentLogFile = f
		log.SetOutput(currentLogFile)
	}
	logRotateMutex.Unlock()

	// Upload and cleanup old log outside the lock
	_ = s3client.UploadLogToS3(oldLogPath)
	_ = os.Remove(oldLogPath)
}

func header(request events.APIGatewayProxyRequest, name string) string {
	for k, v := range request.Headers {
		if strings.EqualFold(k, name) {
			return v
		}
	}
	return ""
}

// Some Mailgun fields arrive as form keys; helper to get them case-insensitively.
func headerKV(vals url.Values, key string) string {
	for k, v := range vals {
		if strings.EqualFold(k, key) && len(v) > 0 {
			return v[0]
		}
	}
	return ""
}

func DecodeBase64(encoded string) (string, error) {
	decodedBytes, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil {
		return "", err
	}
	return string(decodedBytes), nil
}

// Reuse the label builder you used in your payload sketches.
func buildLabel(zip, city, stateCode, country string) string {
	zip = strings.TrimSpace(zip)
	if zip == "" {
		return strings.TrimSpace(strings.Join([]string{city, strings.ToUpper(stateCode), country}, ", "))
	}
	return strings.TrimSpace(strings.Join([]string{zip, city, strings.ToUpper(stateCode), country}, ", "))
}
func isFifoQueue(queueURL string) bool {
	return strings.HasSuffix(strings.ToLower(strings.TrimSpace(queueURL)), ".fifo")
}

func sendToSQS(ctx context.Context, queueURL string, payload []byte) (*sqs.SendMessageOutput, error) {
	if queueURL == "" {
		return nil, fmt.Errorf("AWSSQSQueueURL is empty")
	}
	input := &sqs.SendMessageInput{
		QueueUrl:    aws.String(queueURL),
		MessageBody: aws.String(string(payload)),
	}
	if isFifoQueue(queueURL) {
		// For FIFO queues, both fields are required.
		msgID := uuid.New().String()
		input.MessageGroupId = aws.String("loadone-parser")
		input.MessageDeduplicationId = aws.String(msgID)
	}

	// Simple retry: 3 attempts with jittered backoff
	var out *sqs.SendMessageOutput
	var err error
	for attempt := 1; attempt <= 3; attempt++ {
		out, err = sqsClient.SendMessageWithContext(ctx, input)
		if err == nil {
			return out, nil
		}
		logrus.WithError(err).Warnf("SQS send attempt %d failed", attempt)
		// backoff 150ms, 400ms, 800ms (approx) with small jitter
		sleep := time.Duration(100*(1<<attempt)) * time.Millisecond
		time.Sleep(sleep + time.Duration(25*attempt)*time.Millisecond)
	}
	return nil, err
}

func updateParserLogParsedData(parserLogID int64, parsedData string) error {
	db, err := InitializeDB()
	if err != nil {
		return err
	}
	if db == nil {
		return fmt.Errorf("database handle unavailable")
	}
	return db.Model(&models.ParserLog{}).
		Where("id = ?", parserLogID).
		Updates(map[string]interface{}{
			"parsed_data": parsedData,
			"updated_at":  time.Now(),
		}).Error
}

// Unescape + normalize line breaks and spaces (works for both HTML and plain)
func normalizeForScan(s string) string {
	s = html.UnescapeString(s)
	s = strings.ReplaceAll(s, "\r\n", "\n")
	s = strings.ReplaceAll(s, "\r", "\n")
	s = strings.ReplaceAll(s, "\u00a0", " ")
	// collapse runs of spaces, keep newlines
	lines := strings.Split(s, "\n")
	for i := range lines {
		lines[i] = strings.TrimSpace(regexp.MustCompile(`\s+`).ReplaceAllString(lines[i], " "))
	}
	return strings.Join(lines, "\n")
}
