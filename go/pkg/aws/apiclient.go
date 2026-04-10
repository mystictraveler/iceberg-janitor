package aws

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	awspkg "github.com/aws/aws-sdk-go-v2/aws"
)

// APIClient is a janitor-server HTTP client with SigV4 signing for
// private API Gateway authentication.
type APIClient struct {
	baseURL string
	creds   awspkg.CredentialsProvider
	region  string
	http    *http.Client
}

// NewAPIClient creates a SigV4-authenticated client for a janitor-server
// behind a private API Gateway.
func NewAPIClient(ctx context.Context, baseURL, region string) (*APIClient, error) {
	creds, resolvedRegion, err := Creds(ctx, region)
	if err != nil {
		return nil, err
	}
	return &APIClient{
		baseURL: strings.TrimRight(baseURL, "/"),
		creds:   creds,
		region:  resolvedRegion,
		http:    &http.Client{Timeout: 5 * time.Minute},
	}, nil
}

func (c *APIClient) do(ctx context.Context, method, path string, body []byte) ([]byte, error) {
	url := c.baseURL + path
	headers := map[string]string{"Accept": "application/json"}
	if body != nil {
		headers["Content-Type"] = "application/json"
	}
	return DoJSON(ctx, c.creds, method, url, "execute-api", c.region, headers, body)
}

// ListTables calls GET /v1/tables.
func (c *APIClient) ListTables(ctx context.Context, prefix string) ([]TableEntry, error) {
	path := "/v1/tables"
	if prefix != "" {
		path += "?prefix=" + prefix
	}
	data, err := c.do(ctx, "GET", path, nil)
	if err != nil {
		return nil, err
	}
	var result struct {
		Tables []TableEntry `json:"tables"`
	}
	if err := json.Unmarshal(data, &result); err != nil {
		return nil, fmt.Errorf("decoding: %w", err)
	}
	return result.Tables, nil
}

// Analyze calls GET /v1/tables/{ns}/{name}/health.
func (c *APIClient) Analyze(ctx context.Context, ns, name string) (json.RawMessage, error) {
	data, err := c.do(ctx, "GET", fmt.Sprintf("/v1/tables/%s/%s/health", ns, name), nil)
	return json.RawMessage(data), err
}

// Compact calls POST /v1/tables/{ns}/{name}/compact.
func (c *APIClient) Compact(ctx context.Context, ns, name string) (json.RawMessage, error) {
	data, err := c.do(ctx, "POST", fmt.Sprintf("/v1/tables/%s/%s/compact", ns, name), nil)
	return json.RawMessage(data), err
}

// Healthz calls GET /v1/healthz.
func (c *APIClient) Healthz(ctx context.Context) error {
	_, err := c.do(ctx, "GET", "/v1/healthz", nil)
	return err
}

// TableEntry matches the server's discover response shape.
type TableEntry struct {
	Prefix          string `json:"prefix"`
	CurrentVersion  int    `json:"current_version"`
	CurrentMetadata string `json:"current_metadata"`
}

// DiscoverResponse wraps the /v1/tables response.
type DiscoverResponse struct {
	Tables []TableEntry `json:"tables"`
	Count  int          `json:"count"`
}

func readError(resp *http.Response) string {
	return fmt.Sprintf("HTTP %d", resp.StatusCode)
}
