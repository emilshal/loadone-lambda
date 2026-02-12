package models

import "time"

type Order struct {
	ID                 int       `gorm:"primaryKey;autoIncrement" json:"id"`
	OrderNumber        string    `json:"order_number"`
	PickupLocation     string    `json:"pickup_location"`
	DeliveryLocation   string    `json:"delivery_location"`
	PickupDate         time.Time `json:"pickup_date"`
	DeliveryDate       time.Time `json:"delivery_date"`
	SuggestedTruckSize string    `json:"suggested_truck_size"`
	Notes              string    `json:"notes"`
	CreatedAt          time.Time `json:"created_at"`
	UpdatedAt          time.Time `json:"updated_at"`
	PickupZip          string    `json:"pickup_zip"`
	DeliveryZip        string    `json:"delivery_zip"`
	OrderTypeID        int       `json:"order_type_id"`
	EstimatedMiles     int       `json:"estimated_miles"`
	TruckTypeID        int       `json:"truck_type_id"`
	OriginalTruckSize  string    `json:"original_truck_size"`
	// Overwrite the mapping for CarrierPay and CarrierPayRate
	// Match struct fields to existing database columns
	CarrierPay     int     `gorm:"column:pays" json:"pays"`
	CarrierPayRate float64 `gorm:"column:pays_rate" json:"pays_rate"`
}

type ParserLog struct {
	ID         int64     `gorm:"column:id;primaryKey;autoIncrement" json:"id"`
	BodyHtml   string    `gorm:"column:body_html;type:text"`
	BodyPlain  string    `gorm:"column:body_plain;type:text"`
	MessageID  string    `gorm:"column:message_id;type:varchar(255)" json:"message_id"`
	ErrorType  string    `gorm:"column:error_type;type:varchar(255)"`
	ErrorText  string    `gorm:"column:error_text;type:text"`
	OrderID    int       `gorm:"column:order_id"`
	ParserID   uint64    `gorm:"column:parser_id"`
	ParserType string    `gorm:"column:parser_type;type:enum('mail','api')"`
	Subject    string    `gorm:"column:subject;type:text"`
	ParsedData string    `gorm:"column:parsed_data;type:text" json:"parsed_data"`
	CreatedAt  time.Time `gorm:"column:created_at"`
	UpdatedAt  time.Time `gorm:"column:updated_at"`
}

func (ParserLog) TableName() string {
	return "parser_log"
}

type OrderLocation struct {
	ID                  int       `gorm:"primaryKey;autoIncrement" json:"id"`
	OrderID             int       `json:"order_id"`
	PickupLabel         string    `json:"pickup_label"`
	PickupCountryCode   string    `gorm:"column:pickup_countryCode" json:"pickup_countryCode"`
	PickupCountryName   string    `gorm:"column:pickup_countryName" json:"pickup_countryName"`
	PickupStateCode     string    `gorm:"column:pickup_stateCode" json:"pickup_stateCode"`
	PickupState         string    `gorm:"column:pickup_state" json:"pickup_state"`
	PickupCounty        string    `gorm:"column:pickup_county" json:"pickup_county"`
	PickupCity          string    `gorm:"column:pickup_city" json:"pickup_city"`
	PickupStreet        string    `gorm:"column:pickup_street" json:"pickup_street"`
	PickupPostalCode    string    `gorm:"column:pickup_postalCode" json:"pickup_postalCode"`
	PickupHouseNumber   string    `gorm:"column:pickup_housenumber" json:"pickup_housenumber"`
	PickupLat           float64   `gorm:"column:pickup_lat" json:"pickup_lat"`
	PickupLng           float64   `gorm:"column:pickup_lng" json:"pickup_lng"`
	DeliveryLabel       string    `gorm:"column:delivery_label" json:"delivery_label"`
	DeliveryCountryCode string    `gorm:"column:delivery_countryCode" json:"delivery_countryCode"`
	DeliveryCountryName string    `gorm:"column:delivery_countryName" json:"delivery_countryName"`
	DeliveryStateCode   string    `gorm:"column:delivery_stateCode" json:"delivery_stateCode"`
	DeliveryState       string    `gorm:"column:delivery_state" json:"delivery_state"`
	DeliveryCounty      string    `gorm:"column:delivery_county" json:"delivery_county"`
	DeliveryCity        string    `gorm:"column:delivery_city" json:"delivery_city"`
	DeliveryStreet      string    `gorm:"column:delivery_street" json:"delivery_street"`
	DeliveryPostalCode  string    `gorm:"column:delivery_postalCode" json:"delivery_postalCode"`
	DeliveryHouseNumber string    `gorm:"column:delivery_housenumber" json:"delivery_housenumber"`
	DeliveryLat         float64   `gorm:"column:delivery_lat" json:"delivery_lat"`
	DeliveryLng         float64   `gorm:"column:delivery_lng" json:"delivery_lng"`
	EstimatedMiles      float64   `gorm:"column:estimated_miles" json:"estimated_miles"`
	UpdatedAt           time.Time `gorm:"column:updated_at" json:"updated_at"`
	CreatedAt           time.Time `gorm:"column:created_at" json:"created_at"`
}

type OrderItem struct {
	ID        int       `gorm:"primaryKey;autoIncrement" json:"id"`
	OrderID   int       `json:"order_id"`
	Length    float64   `json:"length"`
	Width     float64   `json:"width"`
	Height    float64   `json:"height"`
	Weight    float64   `json:"weight"`
	Pieces    int       `json:"pieces"`
	Stackable bool      `json:"stackable"`
	Hazardous bool      `json:"hazardous"`
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

type OrderEmail struct {
	ID        int       `gorm:"primaryKey;autoIncrement" json:"id"`
	ReplyTo   string    `json:"reply_to"`
	Subject   string    `json:"subject"`
	MessageID string    `json:"message_id"`
	OrderID   int       `json:"order_id"`
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

// TableName overrides the default table name used by Gorm
func (OrderEmail) TableName() string {
	return "order_email"
}
