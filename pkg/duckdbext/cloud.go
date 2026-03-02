// Package duckdbext provides shared DuckDB extension helpers
// used by both source and sink packages.
package duckdbext

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"strings"
)

// CloudConfig holds credentials for S3, GCS, and Azure Blob Storage
// that DuckDB needs to access remote files via httpfs.
type CloudConfig struct {
	// S3
	S3AccessKeyID     string
	S3SecretAccessKey string
	S3Region          string
	S3Endpoint        string
	S3URLStyle        string // "path" or "vhost"

	// GCS
	GCSServiceAccountKey string // path to JSON key file

	// Azure
	AzureAccountName string
	AzureAccountKey  string
	AzureConnectionString string
}

// CloudConfigFromMap builds a CloudConfig from a YAML config map,
// falling back to environment variables.
func CloudConfigFromMap(cfg map[string]any) CloudConfig {
	return CloudConfig{
		// S3
		S3AccessKeyID:     resolve(cfg, "s3_access_key_id", "AWS_ACCESS_KEY_ID", ""),
		S3SecretAccessKey: resolve(cfg, "s3_secret_access_key", "AWS_SECRET_ACCESS_KEY", ""),
		S3Region:          resolve(cfg, "s3_region", "AWS_REGION", "us-east-1"),
		S3Endpoint:        resolve(cfg, "s3_endpoint", "AWS_ENDPOINT_URL", ""),
		S3URLStyle:        resolve(cfg, "s3_url_style", "", "path"),

		// GCS
		GCSServiceAccountKey: resolve(cfg, "gcs_service_account_key", "GOOGLE_APPLICATION_CREDENTIALS", ""),

		// Azure
		AzureAccountName:      resolve(cfg, "azure_account_name", "AZURE_STORAGE_ACCOUNT", ""),
		AzureAccountKey:       resolve(cfg, "azure_account_key", "AZURE_STORAGE_KEY", ""),
		AzureConnectionString: resolve(cfg, "azure_connection_string", "AZURE_STORAGE_CONNECTION_STRING", ""),
	}
}

// NeedsHTTPFS returns true if the given path references a remote cloud store.
func NeedsHTTPFS(path string) bool {
	lower := strings.ToLower(path)
	return strings.HasPrefix(lower, "s3://") ||
		strings.HasPrefix(lower, "s3a://") ||
		strings.HasPrefix(lower, "gs://") ||
		strings.HasPrefix(lower, "gcs://") ||
		strings.HasPrefix(lower, "az://") ||
		strings.HasPrefix(lower, "azure://") ||
		strings.HasPrefix(lower, "abfss://") ||
		strings.HasPrefix(lower, "https://storage.googleapis.com") ||
		strings.HasPrefix(lower, "https://") && strings.Contains(lower, ".blob.core.windows.net") ||
		strings.HasPrefix(lower, "https://") && strings.Contains(lower, ".s3.amazonaws.com")
}

// NeedsHTTPFSQuery returns true if a SQL query likely references remote paths.
// This is a heuristic check for common patterns like read_parquet('s3://...').
func NeedsHTTPFSQuery(query string) bool {
	lower := strings.ToLower(query)
	return strings.Contains(lower, "s3://") ||
		strings.Contains(lower, "s3a://") ||
		strings.Contains(lower, "gs://") ||
		strings.Contains(lower, "gcs://") ||
		strings.Contains(lower, "az://") ||
		strings.Contains(lower, "azure://") ||
		strings.Contains(lower, "abfss://") ||
		strings.Contains(lower, ".blob.core.windows.net") ||
		strings.Contains(lower, ".s3.amazonaws.com") ||
		strings.Contains(lower, "storage.googleapis.com")
}

// LoadCloudExtensions installs and loads the httpfs extension, then configures
// cloud credentials (S3, GCS, Azure) if provided. This should be called in
// Open() after the DuckDB connection is established.
//
// The label parameter is used for log messages (e.g. "source", "sink").
func LoadCloudExtensions(ctx context.Context, db *sql.DB, cc CloudConfig, label string) error {
	// Install and load httpfs
	if _, err := db.ExecContext(ctx, "INSTALL httpfs; LOAD httpfs;"); err != nil {
		return fmt.Errorf("duckdb %s: httpfs extension: %w", label, err)
	}
	fmt.Fprintf(os.Stderr, "[duckdb] %s: loaded httpfs extension\n", label)

	// S3 credentials
	if cc.S3AccessKeyID != "" {
		setCmds := []string{
			fmt.Sprintf("SET s3_access_key_id = '%s'", escapeSQLString(cc.S3AccessKeyID)),
			fmt.Sprintf("SET s3_secret_access_key = '%s'", escapeSQLString(cc.S3SecretAccessKey)),
			fmt.Sprintf("SET s3_region = '%s'", escapeSQLString(cc.S3Region)),
		}
		if cc.S3Endpoint != "" {
			setCmds = append(setCmds, fmt.Sprintf("SET s3_endpoint = '%s'", escapeSQLString(cc.S3Endpoint)))
			setCmds = append(setCmds, fmt.Sprintf("SET s3_url_style = '%s'", escapeSQLString(cc.S3URLStyle)))
		}
		for _, cmd := range setCmds {
			if _, err := db.ExecContext(ctx, cmd); err != nil {
				return fmt.Errorf("duckdb %s: s3 config: %w", label, err)
			}
		}
		fmt.Fprintf(os.Stderr, "[duckdb] %s: configured S3 credentials (region=%s)\n", label, cc.S3Region)
	}

	// GCS credentials (via service account key file)
	if cc.GCSServiceAccountKey != "" {
		cmd := fmt.Sprintf("SET gcs_service_account_key_path = '%s'", escapeSQLString(cc.GCSServiceAccountKey))
		if _, err := db.ExecContext(ctx, cmd); err != nil {
			// Older DuckDB versions might not support this — try alternative
			fmt.Fprintf(os.Stderr, "[duckdb] %s: gcs key path config: %v (continuing)\n", label, err)
		} else {
			fmt.Fprintf(os.Stderr, "[duckdb] %s: configured GCS credentials\n", label)
		}
	}

	// Azure credentials
	if cc.AzureConnectionString != "" {
		cmd := fmt.Sprintf("SET azure_storage_connection_string = '%s'", escapeSQLString(cc.AzureConnectionString))
		if _, err := db.ExecContext(ctx, cmd); err != nil {
			return fmt.Errorf("duckdb %s: azure config: %w", label, err)
		}
		fmt.Fprintf(os.Stderr, "[duckdb] %s: configured Azure credentials (connection string)\n", label)
	} else if cc.AzureAccountName != "" {
		setCmds := []string{
			fmt.Sprintf("SET azure_account_name = '%s'", escapeSQLString(cc.AzureAccountName)),
		}
		if cc.AzureAccountKey != "" {
			setCmds = append(setCmds, fmt.Sprintf("SET azure_account_key = '%s'", escapeSQLString(cc.AzureAccountKey)))
		}
		for _, cmd := range setCmds {
			if _, err := db.ExecContext(ctx, cmd); err != nil {
				return fmt.Errorf("duckdb %s: azure config: %w", label, err)
			}
		}
		fmt.Fprintf(os.Stderr, "[duckdb] %s: configured Azure credentials (account=%s)\n", label, cc.AzureAccountName)
	}

	return nil
}

// escapeSQLString escapes single quotes for use in DuckDB SET commands.
func escapeSQLString(s string) string {
	return strings.ReplaceAll(s, "'", "''")
}

// resolve reads a value from the config map, then falls back to an
// environment variable, then to a default value.
func resolve(cfg map[string]any, key, envKey, defaultVal string) string {
	if cfg != nil {
		if v, ok := cfg[key].(string); ok && v != "" {
			return v
		}
	}
	if envKey != "" {
		if v := os.Getenv(envKey); v != "" {
			return v
		}
	}
	return defaultVal
}
