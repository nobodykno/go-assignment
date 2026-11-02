package database

import (
	"net/url"
	"testing"
)

func TestDSNFromEnvEscapesCredentials(t *testing.T) {
	t.Setenv("DB_HOST", "localhost")
	t.Setenv("DB_PORT", "5432")
	t.Setenv("DB_USER", "pipe line")
	t.Setenv("DB_PASSWORD", "p@ss:w/rd ?#%")
	t.Setenv("DB_NAME", "pipeline")
	t.Setenv("DB_SSLMODE", "")

	dsn, err := DSNFromEnv()
	if err != nil {
		t.Fatalf("DSNFromEnv() returned error: %v", err)
	}

	parsed, err := url.Parse(dsn)
	if err != nil {
		t.Fatalf("DSN is not a valid URL: %v", err)
	}

	password, _ := parsed.User.Password()
	if parsed.User.Username() != "pipe line" || password != "p@ss:w/rd ?#%" {
		t.Fatalf("credentials not preserved: user=%q password=%q", parsed.User.Username(), password)
	}

	if parsed.Host != "localhost:5432" || parsed.Path != "/pipeline" {
		t.Fatalf("unexpected host/path: %q %q", parsed.Host, parsed.Path)
	}

	if got := parsed.Query().Get("sslmode"); got != "disable" {
		t.Fatalf("expected default sslmode=disable, got %q", got)
	}
}

func TestDSNFromEnvRequiresConfiguration(t *testing.T) {
	t.Setenv("DB_HOST", "")
	t.Setenv("DB_PORT", "5432")
	t.Setenv("DB_USER", "user")
	t.Setenv("DB_NAME", "db")

	if _, err := DSNFromEnv(); err == nil {
		t.Fatal("expected an error when DB_HOST is missing")
	}
}
