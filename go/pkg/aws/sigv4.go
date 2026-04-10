// Package aws provides AWS-specific integrations for the janitor:
// SigV4-signed API Gateway client, Glue table registration for Athena,
// and Iceberg-to-Glue type mapping. No AWS service SDKs — just raw
// HTTP + SigV4 against the JSON APIs.
package aws

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	awspkg "github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
)

// Creds loads AWS credentials and region from the default chain.
func Creds(ctx context.Context, region string) (awspkg.CredentialsProvider, string, error) {
	cfg, err := awsconfig.LoadDefaultConfig(ctx, awsconfig.WithRegion(region))
	if err != nil {
		return nil, "", fmt.Errorf("loading AWS config: %w", err)
	}
	return cfg.Credentials, cfg.Region, nil
}

// SignedRequest creates and SigV4-signs an HTTP request.
func SignedRequest(ctx context.Context, creds awspkg.CredentialsProvider, method, url, service, region string, headers map[string]string, body []byte) (*http.Request, error) {
	var bodyReader io.Reader
	if body != nil {
		bodyReader = bytes.NewReader(body)
	}

	req, err := http.NewRequestWithContext(ctx, method, url, bodyReader)
	if err != nil {
		return nil, err
	}
	for k, v := range headers {
		req.Header.Set(k, v)
	}

	hash := sha256Hash(body)
	c, err := creds.Retrieve(ctx)
	if err != nil {
		return nil, fmt.Errorf("retrieving credentials: %w", err)
	}
	signer := v4.NewSigner()
	if err := signer.SignHTTP(ctx, c, req, hash, service, region, time.Now()); err != nil {
		return nil, fmt.Errorf("signing: %w", err)
	}
	return req, nil
}

// DoJSON sends a SigV4-signed request and returns the response body.
// Returns an error if the status code is >= 400.
func DoJSON(ctx context.Context, creds awspkg.CredentialsProvider, method, url, service, region string, headers map[string]string, body []byte) ([]byte, error) {
	req, err := SignedRequest(ctx, creds, method, url, service, region, headers, body)
	if err != nil {
		return nil, err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode >= 400 {
		return data, fmt.Errorf("HTTP %d: %s", resp.StatusCode, string(data))
	}
	return data, nil
}

// CallAWSJSON calls an AWS JSON API (like Glue) with the given target
// header and JSON payload, signing with SigV4.
func CallAWSJSON(ctx context.Context, creds awspkg.CredentialsProvider, endpoint, service, region, target string, payload any) error {
	body, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	_, err = DoJSON(ctx, creds, "POST", endpoint, service, region, map[string]string{
		"Content-Type": "application/x-amz-json-1.1",
		"X-Amz-Target": target,
	}, body)
	return err
}

func sha256Hash(data []byte) string {
	if data == nil {
		data = []byte{}
	}
	h := sha256.Sum256(data)
	return hex.EncodeToString(h[:])
}
