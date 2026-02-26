package loadoneapi

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"
)

const defaultBaseURL = "https://app.load1.com"

var (
	ErrMissingCredentials = errors.New("load1 credentials missing")
	ErrAccessKeyNotFound  = errors.New("accessKey not found")
)

type Client struct {
	baseURL      string
	clientID     string
	clientSecret string
	httpClient   *http.Client

	mu          sync.Mutex
	accessToken string
	expiresAt   time.Time
}

type Config struct {
	BaseURL      string
	ClientID     string
	ClientSecret string
	HTTPClient   *http.Client
}

func NewClient(cfg Config) *Client {
	baseURL := strings.TrimRight(strings.TrimSpace(cfg.BaseURL), "/")
	if baseURL == "" {
		baseURL = defaultBaseURL
	}
	hc := cfg.HTTPClient
	if hc == nil {
		hc = &http.Client{Timeout: 10 * time.Second}
	}
	return &Client{
		baseURL:      baseURL,
		clientID:     strings.TrimSpace(cfg.ClientID),
		clientSecret: strings.TrimSpace(cfg.ClientSecret),
		httpClient:   hc,
	}
}

func NewClientFromEnv() *Client {
	return NewClient(Config{
		BaseURL:      os.Getenv("LOAD1_BASE_URL"),
		ClientID:     os.Getenv("LOAD1_CLIENT_ID"),
		ClientSecret: os.Getenv("LOAD1_CLIENT_SECRET"),
	})
}

func (c *Client) HasCredentials() bool {
	return c != nil && strings.TrimSpace(c.clientID) != "" && strings.TrimSpace(c.clientSecret) != ""
}

type APITokenRequest struct {
	ClientID     string `json:"clientId"`
	ClientSecret string `json:"clientSecret"`
}

type APITokenResponse struct {
	AccessToken           string    `json:"accessToken"`
	AccessTokenExpiration time.Time `json:"accessTokenExpiration"`
}

type CarrierQuoteBid struct {
	QuoteID             int      `json:"quoteID"`
	AccessKey           string   `json:"accessKey"`
	AllInRate           float64  `json:"allInRate"`
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
}

type CarrierQuoteDecline struct {
	QuoteID   int    `json:"quoteID"`
	AccessKey string `json:"accessKey"`
	Note      string `json:"note,omitempty"`
}

type CarrierQuoteRetractBid struct {
	QuoteID   int    `json:"quoteID"`
	AccessKey string `json:"accessKey"`
}

type HTTPError struct {
	StatusCode int
	Body       string
}

func (e *HTTPError) Error() string {
	if e == nil {
		return ""
	}
	if e.Body == "" {
		return fmt.Sprintf("load1 API returned HTTP %d", e.StatusCode)
	}
	return fmt.Sprintf("load1 API returned HTTP %d: %s", e.StatusCode, e.Body)
}

func (c *Client) Authenticate(ctx context.Context) error {
	if !c.HasCredentials() {
		return ErrMissingCredentials
	}

	reqBody := APITokenRequest{
		ClientID:     c.clientID,
		ClientSecret: c.clientSecret,
	}

	var resp APITokenResponse
	if err := c.doJSON(ctx, http.MethodPost, "/api/auth/token", "", reqBody, &resp); err != nil {
		return err
	}

	if strings.TrimSpace(resp.AccessToken) == "" {
		return fmt.Errorf("load1 auth succeeded but accessToken was empty")
	}

	c.mu.Lock()
	c.accessToken = resp.AccessToken
	c.expiresAt = resp.AccessTokenExpiration
	c.mu.Unlock()
	return nil
}

func (c *Client) ensureToken(ctx context.Context) (string, error) {
	c.mu.Lock()
	token := c.accessToken
	expiresAt := c.expiresAt
	c.mu.Unlock()

	if strings.TrimSpace(token) != "" && time.Until(expiresAt) > 60*time.Second {
		return token, nil
	}

	if err := c.Authenticate(ctx); err != nil {
		return "", err
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	return c.accessToken, nil
}

func (c *Client) Bid(ctx context.Context, in CarrierQuoteBid) error {
	token, err := c.ensureToken(ctx)
	if err != nil {
		return err
	}
	return c.doJSON(ctx, http.MethodPost, "/api/RequestForQuote/Bid", token, in, nil)
}

func (c *Client) Decline(ctx context.Context, in CarrierQuoteDecline) error {
	token, err := c.ensureToken(ctx)
	if err != nil {
		return err
	}
	return c.doJSON(ctx, http.MethodPost, "/api/RequestForQuote/Decline", token, in, nil)
}

func (c *Client) RetractBid(ctx context.Context, in CarrierQuoteRetractBid) error {
	token, err := c.ensureToken(ctx)
	if err != nil {
		return err
	}
	return c.doJSON(ctx, http.MethodPost, "/api/RequestForQuote/RetractBid", token, in, nil)
}

func (c *Client) doJSON(ctx context.Context, method, path, bearerToken string, in any, out any) error {
	if c == nil {
		return fmt.Errorf("nil load1 client")
	}

	var bodyReader io.Reader
	if in != nil {
		b, err := json.Marshal(in)
		if err != nil {
			return fmt.Errorf("marshal request: %w", err)
		}
		bodyReader = bytes.NewReader(b)
	}

	req, err := http.NewRequestWithContext(ctx, method, c.baseURL+path, bodyReader)
	if err != nil {
		return fmt.Errorf("build request: %w", err)
	}
	req.Header.Set("Accept", "application/json")
	if in != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if strings.TrimSpace(bearerToken) != "" {
		req.Header.Set("Authorization", "Bearer "+strings.TrimSpace(bearerToken))
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("request failed: %w", err)
	}
	defer resp.Body.Close()

	respBody, readErr := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if readErr != nil {
		return fmt.Errorf("read response: %w", readErr)
	}

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return &HTTPError{StatusCode: resp.StatusCode, Body: strings.TrimSpace(string(respBody))}
	}

	if out == nil || len(respBody) == 0 {
		return nil
	}

	if err := json.Unmarshal(respBody, out); err != nil {
		return fmt.Errorf("decode response: %w", err)
	}
	return nil
}
