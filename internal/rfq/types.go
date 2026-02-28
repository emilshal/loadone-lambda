package rfq

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

type ParsedRFQMessage struct {
	OrderNumber        string  `json:"orderNumber"`
	Subject            string  `json:"subject"`
	MessageID          string  `json:"messageID"`
	ReplyTo            string  `json:"replyTo"`
	BrokerName         string  `json:"brokerName"`
	ParserLogID        int64   `json:"parserLogID"`
	AccessKey          string  `json:"accessKey,omitempty"`
	SuggestedTruckSize string  `json:"suggestedTruckSize"`
	TruckTypeID        int     `json:"truckTypeID"`
	OriginalTruckSize  string  `json:"originalTruckSize"`
	EstimatedMiles     int     `json:"estimatedMiles"`
	CarrierPay         int     `json:"carrierPay"`
	CarrierPayRate     float64 `json:"carrierPayRate"`
	Notes              string  `json:"notes"`

	PickupCity          string `json:"pickupCity"`
	PickupStateCode     string `json:"pickupStateCode"`
	PickupZip           string `json:"pickupZip"`
	PickupDate          string `json:"pickupDate"`
	PickupDateDisplay   string `json:"pickupDateDisplay"`
	DeliveryCity        string `json:"deliveryCity"`
	DeliveryStateCode   string `json:"deliveryStateCode"`
	DeliveryZip         string `json:"deliveryZip"`
	DeliveryDate        string `json:"deliveryDate"`
	DeliveryDateDisplay string `json:"deliveryDateDisplay"`

	Length float64 `json:"length"`
	Width  float64 `json:"width"`
	Height float64 `json:"height"`
	Weight float64 `json:"weight"`
	Pieces int     `json:"pieces"`

	Stackable bool `json:"stackable"`
	Hazardous bool `json:"hazardous"`

	ExternalLink    string `json:"externalLink"`
	ExternalLinkRaw string `json:"externalLinkRaw"`
}

func (m ParsedRFQMessage) QuoteID() (int, error) {
	s := strings.TrimSpace(m.OrderNumber)
	if s == "" {
		return 0, fmt.Errorf("orderNumber is empty")
	}
	n, err := strconv.Atoi(s)
	if err != nil {
		return 0, fmt.Errorf("invalid orderNumber %q: %w", s, err)
	}
	return n, nil
}

type ActionType string

const (
	ActionManualReview ActionType = "manual_review"
	ActionBid          ActionType = "bid"
	ActionDecline      ActionType = "decline"
	ActionRetractBid   ActionType = "retract_bid"
	ActionIgnore       ActionType = "ignore"
)

type ActionIntent struct {
	IntentID string `json:"intentID"`

	Action ActionType `json:"action"`
	Reason string     `json:"reason,omitempty"`
	Source string     `json:"source,omitempty"`

	QuoteID     int    `json:"quoteID"`
	AccessKey   string `json:"accessKey,omitempty"`
	OrderNumber string `json:"orderNumber,omitempty"`
	MessageID   string `json:"messageID,omitempty"`
	ParserLogID int64  `json:"parserLogID,omitempty"`

	AllInRate           *float64 `json:"allInRate,omitempty"`
	Note                string   `json:"note,omitempty"`
	AlternativePickup   *string  `json:"alternativePickup,omitempty"`
	AlternativeDelivery *string  `json:"alternativeDelivery,omitempty"`
	MilesFromPickup     *float64 `json:"milesFromPickup,omitempty"`
	BoxLength           *float64 `json:"boxLength,omitempty"`
	BoxWidth            *float64 `json:"boxWidth,omitempty"`
	BoxHeight           *float64 `json:"boxHeight,omitempty"`
	IsVehicleEmpty      *bool    `json:"isVehicleEmpty,omitempty"`
	IsTeamDriver        *bool    `json:"isTeamDriver,omitempty"`
	DispatcherName      string   `json:"dispatcherName,omitempty"`

	TrackingLink    string            `json:"trackingLink,omitempty"`
	TrackingLinkRaw string            `json:"trackingLinkRaw,omitempty"`
	Metadata        map[string]string `json:"metadata,omitempty"`

	CreatedAt string `json:"createdAt"`
}

func NewIntent(action ActionType) ActionIntent {
	return ActionIntent{
		Action:    action,
		CreatedAt: time.Now().UTC().Format(time.RFC3339),
	}
}
