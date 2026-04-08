package config

import (
	"fmt"
	"os"
	"strconv"
	"time"
)

// Config is the fully-resolved, validated set of runtime knobs for all
// janitor binaries (CLI, server, lambda). It is loaded once at process
// startup via Load and then passed down to the things that need it.
//
// Fields are grouped by concern so that unrelated parts of the system
// don't accidentally grow cross-cutting knowledge of each other.
type Config struct {
	Warehouse   WarehouseConfig
	Cloud       CloudConfig
	Safety      SafetyConfig
	Maintenance MaintenanceConfig
	Logging     LoggingConfig
}

// WarehouseConfig locates the Iceberg warehouse the janitor operates on.
type WarehouseConfig struct {
	// URL is a gocloud.dev/blob URL of the warehouse bucket. Required.
	// Examples:
	//   file:///tmp/warehouse
	//   s3://warehouse?endpoint=http://localhost:9000&s3ForcePathStyle=true&region=us-east-1
	// Sourced from JANITOR_WAREHOUSE_URL.
	URL string

	// APIURL, when set, tells the CLI to dispatch compaction to a
	// running janitor-server over HTTP instead of running it in
	// process. Optional. Sourced from JANITOR_API_URL.
	APIURL string
}

// CloudConfig carries cloud/object-store endpoint and credential
// information.
//
// SECURITY: per design decision #27, production credentials must come
// from the cloud-native credential chain (IAM role, workload identity,
// managed identity) and NEVER from these env vars. S3AccessKeyID and
// S3SecretAccessKey exist solely as a local-development fallback (e.g.
// MinIO on a laptop). They are optional; Load will not fail if they
// are unset. Do not log them, do not print them, do not serialize them.
type CloudConfig struct {
	// S3Endpoint overrides the S3 endpoint URL. Sourced from S3_ENDPOINT.
	S3Endpoint string
	// S3Region is the S3 region. Sourced from S3_REGION, falling back
	// to AWS_REGION if S3_REGION is unset.
	S3Region string
	// S3AccessKeyID is a static access key. LOCAL DEV ONLY. Optional.
	// Sourced from AWS_ACCESS_KEY_ID.
	S3AccessKeyID string
	// S3SecretAccessKey is a static secret key. LOCAL DEV ONLY. Optional.
	// Sourced from AWS_SECRET_ACCESS_KEY.
	S3SecretAccessKey string
	// S3PathStyle forces path-style addressing (required by MinIO).
	// Sourced from S3_PATH_STYLE (truthy = "1", "true").
	S3PathStyle bool
}

// SafetyConfig controls the CB8 circuit breaker and related safety knobs.
type SafetyConfig struct {
	// DisableCircuitBreaker disables the CB8 breaker entirely. This is
	// the escape hatch for the bench harness when we want to observe
	// raw failure modes without the breaker hiding them. Default off.
	// Sourced from JANITOR_DISABLE_CB=1.
	DisableCircuitBreaker bool
	// CB8Threshold is the number of consecutive failed runs before the
	// CB8 breaker auto-pauses a table. Defaults to 3 (matching
	// safety.CB8DefaultThreshold). Sourced from JANITOR_CB8_THRESHOLD.
	CB8Threshold int
}

// MaintenanceConfig controls the commit-retry loop used by the
// compaction pipeline.
type MaintenanceConfig struct {
	// MaxAttempts is the maximum number of commit attempts before a
	// compaction run is considered failed. Defaults to 15. Sourced
	// from JANITOR_MAX_ATTEMPTS.
	MaxAttempts int
	// InitialBackoff is the starting sleep between retries (doubles on
	// each attempt). Defaults to 100ms. Sourced from
	// JANITOR_INITIAL_BACKOFF (Go duration syntax, e.g. "250ms").
	InitialBackoff time.Duration
}

// LoggingConfig controls log verbosity.
type LoggingConfig struct {
	// Level is one of "debug", "info", "warn", "error". Defaults to
	// "info". Sourced from JANITOR_LOG_LEVEL.
	Level string
}

// Load reads configuration from the process environment, applies
// defaults, validates required fields, and returns a populated Config.
// On a missing required value it returns an error whose message names
// the offending environment variable.
func Load() (*Config, error) {
	cfg := &Config{}

	// Warehouse.
	cfg.Warehouse.URL = os.Getenv("JANITOR_WAREHOUSE_URL")
	cfg.Warehouse.APIURL = os.Getenv("JANITOR_API_URL")

	// Cloud / S3.
	cfg.Cloud.S3Endpoint = os.Getenv("S3_ENDPOINT")
	if v := os.Getenv("S3_REGION"); v != "" {
		cfg.Cloud.S3Region = v
	} else {
		cfg.Cloud.S3Region = os.Getenv("AWS_REGION")
	}
	cfg.Cloud.S3AccessKeyID = os.Getenv("AWS_ACCESS_KEY_ID")
	cfg.Cloud.S3SecretAccessKey = os.Getenv("AWS_SECRET_ACCESS_KEY")
	cfg.Cloud.S3PathStyle = envBool("S3_PATH_STYLE")

	// Safety.
	cfg.Safety.DisableCircuitBreaker = os.Getenv("JANITOR_DISABLE_CB") == "1"
	cfg.Safety.CB8Threshold = 3
	if v := os.Getenv("JANITOR_CB8_THRESHOLD"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n <= 0 {
			return nil, fmt.Errorf("JANITOR_CB8_THRESHOLD must be a positive integer, got %q", v)
		}
		cfg.Safety.CB8Threshold = n
	}

	// Maintenance.
	cfg.Maintenance.MaxAttempts = 15
	if v := os.Getenv("JANITOR_MAX_ATTEMPTS"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n <= 0 {
			return nil, fmt.Errorf("JANITOR_MAX_ATTEMPTS must be a positive integer, got %q", v)
		}
		cfg.Maintenance.MaxAttempts = n
	}
	cfg.Maintenance.InitialBackoff = 100 * time.Millisecond
	if v := os.Getenv("JANITOR_INITIAL_BACKOFF"); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil || d <= 0 {
			return nil, fmt.Errorf("JANITOR_INITIAL_BACKOFF must be a positive Go duration, got %q", v)
		}
		cfg.Maintenance.InitialBackoff = d
	}

	// Logging.
	cfg.Logging.Level = "info"
	if v := os.Getenv("JANITOR_LOG_LEVEL"); v != "" {
		switch v {
		case "debug", "info", "warn", "error":
			cfg.Logging.Level = v
		default:
			return nil, fmt.Errorf("JANITOR_LOG_LEVEL must be one of debug|info|warn|error, got %q", v)
		}
	}

	if err := cfg.validate(); err != nil {
		return nil, err
	}
	return cfg, nil
}

func (c *Config) validate() error {
	if c.Warehouse.URL == "" {
		return fmt.Errorf("JANITOR_WAREHOUSE_URL is required")
	}
	return nil
}

// ToBlobProps returns the property map iceberg-go's blob/IO layer
// expects. It mirrors the legacy propsFromEnv() function in
// cmd/janitor-cli so call sites can be migrated incrementally without
// changing the on-the-wire property shape.
//
// Note: unlike propsFromEnv, this method does NOT mutate the process
// environment. The legacy function set AWS_S3_ENDPOINT /
// AWS_ENDPOINT_URL_S3 / AWS_REGION / AWS_DEFAULT_REGION as a side
// effect so that iceberg-go's table-level IO (which reads AWS_* fallbacks
// from the env) could find them. Callers that need that side effect
// should call ApplyAWSEnv explicitly.
func (c *Config) ToBlobProps() map[string]string {
	props := map[string]string{}
	if c.Cloud.S3Endpoint != "" {
		props["s3.endpoint"] = c.Cloud.S3Endpoint
	}
	if c.Cloud.S3Region != "" {
		props["s3.region"] = c.Cloud.S3Region
	}
	if c.Cloud.S3AccessKeyID != "" {
		props["s3.access-key-id"] = c.Cloud.S3AccessKeyID
	}
	if c.Cloud.S3SecretAccessKey != "" {
		props["s3.secret-access-key"] = c.Cloud.S3SecretAccessKey
	}
	return props
}

// ApplyAWSEnv exports the S3 endpoint/region from the config into the
// AWS_* environment variables that iceberg-go's table-level IO reads as
// fallbacks. Call this once at startup if you need the side-effect
// behavior of the legacy propsFromEnv().
func (c *Config) ApplyAWSEnv() {
	if c.Cloud.S3Endpoint != "" {
		os.Setenv("AWS_S3_ENDPOINT", c.Cloud.S3Endpoint)
		os.Setenv("AWS_ENDPOINT_URL_S3", c.Cloud.S3Endpoint)
	}
	if c.Cloud.S3Region != "" {
		os.Setenv("AWS_REGION", c.Cloud.S3Region)
		os.Setenv("AWS_DEFAULT_REGION", c.Cloud.S3Region)
	}
}

func envBool(key string) bool {
	switch os.Getenv(key) {
	case "1", "true", "TRUE", "True", "yes":
		return true
	}
	return false
}
