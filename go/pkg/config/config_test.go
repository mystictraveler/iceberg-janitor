package config

import (
	"reflect"
	"strings"
	"testing"
	"time"
)

// clearEnv unsets every env var that Load consults so tests start from
// a known-empty baseline. t.Setenv restores the prior value at the end
// of the test, so this is safe to call even when the caller's shell
// has some of these vars set.
func clearEnv(t *testing.T) {
	t.Helper()
	for _, k := range []string{
		"JANITOR_WAREHOUSE_URL",
		"JANITOR_API_URL",
		"S3_ENDPOINT",
		"S3_REGION",
		"AWS_REGION",
		"AWS_DEFAULT_REGION",
		"AWS_ACCESS_KEY_ID",
		"AWS_SECRET_ACCESS_KEY",
		"S3_PATH_STYLE",
		"JANITOR_DISABLE_CB",
		"JANITOR_CB8_THRESHOLD",
		"JANITOR_MAX_ATTEMPTS",
		"JANITOR_INITIAL_BACKOFF",
		"JANITOR_LOG_LEVEL",
		"AWS_S3_ENDPOINT",
		"AWS_ENDPOINT_URL_S3",
	} {
		t.Setenv(k, "")
	}
}

func TestLoad_RequiresWarehouseURL(t *testing.T) {
	clearEnv(t)
	_, err := Load()
	if err == nil {
		t.Fatal("expected error when JANITOR_WAREHOUSE_URL is unset, got nil")
	}
	if !strings.Contains(err.Error(), "JANITOR_WAREHOUSE_URL is required") {
		t.Errorf("error message should name the missing var; got %q", err.Error())
	}
}

func TestLoad_DefaultsApplied(t *testing.T) {
	clearEnv(t)
	t.Setenv("JANITOR_WAREHOUSE_URL", "file:///tmp/warehouse")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.Warehouse.URL != "file:///tmp/warehouse" {
		t.Errorf("Warehouse.URL = %q, want file:///tmp/warehouse", cfg.Warehouse.URL)
	}
	if cfg.Safety.CB8Threshold != 3 {
		t.Errorf("Safety.CB8Threshold = %d, want 3", cfg.Safety.CB8Threshold)
	}
	if cfg.Safety.DisableCircuitBreaker {
		t.Error("Safety.DisableCircuitBreaker = true, want false")
	}
	if cfg.Maintenance.MaxAttempts != 15 {
		t.Errorf("Maintenance.MaxAttempts = %d, want 15", cfg.Maintenance.MaxAttempts)
	}
	if cfg.Maintenance.InitialBackoff != 100*time.Millisecond {
		t.Errorf("Maintenance.InitialBackoff = %v, want 100ms", cfg.Maintenance.InitialBackoff)
	}
	if cfg.Logging.Level != "info" {
		t.Errorf("Logging.Level = %q, want info", cfg.Logging.Level)
	}
	if cfg.Cloud.S3PathStyle {
		t.Error("Cloud.S3PathStyle = true, want false")
	}
}

func TestLoad_AllFields(t *testing.T) {
	clearEnv(t)
	t.Setenv("JANITOR_WAREHOUSE_URL", "s3://bucket?region=us-east-1")
	t.Setenv("JANITOR_API_URL", "http://localhost:8080")
	t.Setenv("S3_ENDPOINT", "http://localhost:9000")
	t.Setenv("S3_REGION", "us-west-2")
	t.Setenv("AWS_ACCESS_KEY_ID", "AKIA_TEST")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "secret_test")
	t.Setenv("S3_PATH_STYLE", "true")
	t.Setenv("JANITOR_DISABLE_CB", "1")
	t.Setenv("JANITOR_CB8_THRESHOLD", "7")
	t.Setenv("JANITOR_MAX_ATTEMPTS", "25")
	t.Setenv("JANITOR_INITIAL_BACKOFF", "250ms")
	t.Setenv("JANITOR_LOG_LEVEL", "debug")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}

	want := &Config{
		Warehouse: WarehouseConfig{
			URL:    "s3://bucket?region=us-east-1",
			APIURL: "http://localhost:8080",
		},
		Cloud: CloudConfig{
			S3Endpoint:        "http://localhost:9000",
			S3Region:          "us-west-2",
			S3AccessKeyID:     "AKIA_TEST",
			S3SecretAccessKey: "secret_test",
			S3PathStyle:       true,
		},
		Safety: SafetyConfig{
			DisableCircuitBreaker: true,
			CB8Threshold:          7,
		},
		Maintenance: MaintenanceConfig{
			MaxAttempts:    25,
			InitialBackoff: 250 * time.Millisecond,
		},
		Logging: LoggingConfig{
			Level: "debug",
		},
	}
	if !reflect.DeepEqual(cfg, want) {
		t.Errorf("Load() =\n%+v\nwant\n%+v", cfg, want)
	}
}

func TestLoad_S3RegionFallsBackToAWSRegion(t *testing.T) {
	clearEnv(t)
	t.Setenv("JANITOR_WAREHOUSE_URL", "file:///tmp/warehouse")
	t.Setenv("AWS_REGION", "eu-central-1")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.Cloud.S3Region != "eu-central-1" {
		t.Errorf("Cloud.S3Region = %q, want eu-central-1 (from AWS_REGION)", cfg.Cloud.S3Region)
	}
}

// TestToBlobProps verifies ToBlobProps produces the same property keys
// that cmd/janitor-cli's legacy propsFromEnv() produces for the same
// inputs. This is the bridge contract that lets existing call sites
// keep working while they migrate.
func TestToBlobProps(t *testing.T) {
	clearEnv(t)
	t.Setenv("JANITOR_WAREHOUSE_URL", "s3://bucket")
	t.Setenv("S3_ENDPOINT", "http://localhost:9000")
	t.Setenv("S3_REGION", "us-east-1")
	t.Setenv("AWS_ACCESS_KEY_ID", "minioadmin")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "minioadmin")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}

	props := cfg.ToBlobProps()
	want := map[string]string{
		"s3.endpoint":          "http://localhost:9000",
		"s3.region":            "us-east-1",
		"s3.access-key-id":     "minioadmin",
		"s3.secret-access-key": "minioadmin",
	}
	if !reflect.DeepEqual(props, want) {
		t.Errorf("ToBlobProps() =\n%v\nwant\n%v", props, want)
	}
}

func TestToBlobProps_OmitsEmpty(t *testing.T) {
	clearEnv(t)
	t.Setenv("JANITOR_WAREHOUSE_URL", "file:///tmp/warehouse")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	props := cfg.ToBlobProps()
	if len(props) != 0 {
		t.Errorf("ToBlobProps() = %v, want empty map when no S3 env vars set", props)
	}
}

func TestLoad_InvalidLogLevel(t *testing.T) {
	clearEnv(t)
	t.Setenv("JANITOR_WAREHOUSE_URL", "file:///tmp/warehouse")
	t.Setenv("JANITOR_LOG_LEVEL", "trace")
	if _, err := Load(); err == nil {
		t.Error("expected error for invalid log level, got nil")
	}
}
