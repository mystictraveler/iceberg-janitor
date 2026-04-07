// Package config implements 12-factor configuration for the janitor.
//
// All cloud-specific identifiers, endpoints, ARNs, project IDs, KMS key IDs,
// retention windows, tier budgets, policy paths, and feature flags are
// configuration, not code. Config is loaded with the precedence
// (highest wins): CLI flags > env vars > $JANITOR_CONFIG_FILE > built-in
// defaults. Validation runs at startup; missing required values fail fast.
//
// Credentials are NEVER stored in env vars directly. The cloud SDKs use
// their default credential chains (IAM roles, workload identity, managed
// identity). For local dev / MinIO that genuinely needs static credentials,
// secret-manager refs (e.g. JANITOR_BLOB_ACCESS_KEY_REF=secretsmanager://...)
// are resolved at startup; resolved values never appear in env or logs.
//
// `janitor config show --redact-secrets` prints the merged config grouped
// by concern so operators can confirm what the binary actually sees.
package config
