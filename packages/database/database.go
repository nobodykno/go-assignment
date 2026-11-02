package database

import (
	"fmt"
	"net"
	"net/url"
	"os"
	"strconv"
	"time"

	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

// Connection pool defaults, overridable through environment variables.
const (
	defaultMaxOpenConns    = 25
	defaultMaxIdleConns    = 10
	defaultConnMaxLifetime = 5 * time.Minute
	defaultConnMaxIdleTime = 1 * time.Minute
)

func Connect() (*gorm.DB, error) {
	dsn, err := DSNFromEnv()
	if err != nil {
		return nil, err
	}

	db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{})
	if err != nil {
		return nil, fmt.Errorf("open PostgreSQL connection: %w", err)
	}

	sqlDB, err := db.DB()
	if err != nil {
		return nil, fmt.Errorf("get SQL connection: %w", err)
	}

	// Bound the pool so many service replicas cannot exhaust
	// PostgreSQL's max_connections.
	sqlDB.SetMaxOpenConns(envInt("DB_MAX_OPEN_CONNS", defaultMaxOpenConns))
	sqlDB.SetMaxIdleConns(envInt("DB_MAX_IDLE_CONNS", defaultMaxIdleConns))
	sqlDB.SetConnMaxLifetime(envDuration("DB_CONN_MAX_LIFETIME", defaultConnMaxLifetime))
	sqlDB.SetConnMaxIdleTime(envDuration("DB_CONN_MAX_IDLE_TIME", defaultConnMaxIdleTime))

	if err := sqlDB.Ping(); err != nil {
		_ = sqlDB.Close()
		return nil, fmt.Errorf("ping PostgreSQL: %w", err)
	}

	return db, nil
}

// DSNFromEnv builds a PostgreSQL URL from DB_* environment variables.
// Credentials are URL-encoded, so passwords may contain any character.
func DSNFromEnv() (string, error) {
	host := os.Getenv("DB_HOST")
	port := os.Getenv("DB_PORT")
	user := os.Getenv("DB_USER")
	password := os.Getenv("DB_PASSWORD")
	dbName := os.Getenv("DB_NAME")
	sslMode := os.Getenv("DB_SSLMODE")

	if host == "" || port == "" || user == "" || dbName == "" {
		return "", fmt.Errorf("missing required database configuration")
	}

	if sslMode == "" {
		sslMode = "disable"
	}

	dsn := url.URL{
		Scheme:   "postgres",
		User:     url.UserPassword(user, password),
		Host:     net.JoinHostPort(host, port),
		Path:     "/" + dbName,
		RawQuery: url.Values{"sslmode": []string{sslMode}}.Encode(),
	}

	return dsn.String(), nil
}

// envInt returns a positive integer from the environment, or fallback.
func envInt(key string, fallback int) int {
	value, err := strconv.Atoi(os.Getenv(key))
	if err != nil || value < 1 {
		return fallback
	}
	return value
}

// envDuration returns a positive duration from the environment, or fallback.
func envDuration(key string, fallback time.Duration) time.Duration {
	value, err := time.ParseDuration(os.Getenv(key))
	if err != nil || value <= 0 {
		return fallback
	}
	return value
}
