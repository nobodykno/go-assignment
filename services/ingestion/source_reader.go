package ingestion

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/netip"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	"go-assignment/packages/models"
)

const (
	maxAPIResponseSize = 10 * 1024 * 1024
	maxAPIErrorBody    = 4096
)

// ReadSources reads and combines the records of every configured source
// in memory, in configured order. API sources are fetched at most three
// at a time; source.Options["provider"] selects the Open-Meteo or
// CoinGecko adapter. Prefer OpenSources for large inputs.
func ReadSources(
	ctx context.Context,
	sources []models.SourceDefinition,
	client *http.Client,
) ([]RawRecord, error) {
	stream, err := OpenSources(ctx, sources, client)
	if err != nil {
		return nil, err
	}

	return collect(func(emit EmitFunc) error {
		return stream.Each(ctx, emit)
	})
}

func resolveSourceFile(filePath string) (string, error) {
	uploadsDir := strings.TrimSpace(os.Getenv("UPLOADS_DIR"))
	if uploadsDir == "" {
		uploadsDir = "uploads"
	}

	basePath, err := filepath.Abs(uploadsDir)
	if err != nil {
		return "", fmt.Errorf("resolve uploads directory: %w", err)
	}

	basePath, err = filepath.EvalSymlinks(basePath)
	if err != nil {
		return "", fmt.Errorf(
			"resolve uploads directory symlinks: %w",
			err,
		)
	}

	var candidate string

	if filepath.IsAbs(filePath) {
		candidate = filepath.Clean(filePath)
	} else {
		workingPath, absErr := filepath.Abs(filePath)
		if absErr != nil {
			return "", fmt.Errorf("resolve file path: %w", absErr)
		}

		if isWithinDirectory(basePath, workingPath) {
			candidate = workingPath
		} else {
			candidate = filepath.Join(basePath, filePath)
		}
	}

	candidate, err = filepath.Abs(candidate)
	if err != nil {
		return "", fmt.Errorf("resolve file path: %w", err)
	}

	resolvedPath, err := filepath.EvalSymlinks(candidate)
	if err != nil {
		return "", fmt.Errorf("resolve file symlinks: %w", err)
	}

	if !isWithinDirectory(basePath, resolvedPath) {
		return "", fmt.Errorf(
			"file path must be inside the uploads directory",
		)
	}

	info, err := os.Stat(resolvedPath)
	if err != nil {
		return "", fmt.Errorf("inspect source file: %w", err)
	}

	if !info.Mode().IsRegular() {
		return "", fmt.Errorf("source path must identify a regular file")
	}

	return resolvedPath, nil
}

func isWithinDirectory(basePath, candidate string) bool {
	relative, err := filepath.Rel(basePath, candidate)
	if err != nil {
		return false
	}

	return relative != ".." &&
		!strings.HasPrefix(
			relative,
			".."+string(filepath.Separator),
		) &&
		!filepath.IsAbs(relative)
}

func ReadAPI(
	ctx context.Context,
	client *http.Client,
	rawURL string,
) ([]RawRecord, error) {
	body, err := readAPIResponse(ctx, client, rawURL)
	if err != nil {
		return nil, err
	}

	return parseAPIRecords(body)
}

// readAPIResponse fetches an API response with URL, destination, redirect,
// status-code, timeout (through the client), and response-size checks.
func readAPIResponse(
	ctx context.Context,
	client *http.Client,
	rawURL string,
) ([]byte, error) {
	parsedURL, err := url.ParseRequestURI(strings.TrimSpace(rawURL))
	if err != nil {
		return nil, fmt.Errorf("invalid API URL: %w", err)
	}

	if parsedURL.Scheme != "https" ||
		parsedURL.Hostname() == "" ||
		parsedURL.User != nil {
		return nil, fmt.Errorf(
			"API URL must be a valid HTTPS URL without user credentials",
		)
	}

	if err := validatePublicHost(ctx, parsedURL.Hostname()); err != nil {
		return nil, fmt.Errorf("API destination rejected: %w", err)
	}

	if client == nil {
		client = &http.Client{
			Timeout: 30 * time.Second,
		}
	}

	// Do not follow redirects to another destination.
	safeClient := *client
	safeClient.CheckRedirect = func(
		req *http.Request,
		via []*http.Request,
	) error {
		return http.ErrUseLastResponse
	}

	req, err := http.NewRequestWithContext(
		ctx,
		http.MethodGet,
		parsedURL.String(),
		nil,
	)
	if err != nil {
		return nil, fmt.Errorf("create API request: %w", err)
	}

	req.Header.Set("Accept", "application/json")

	resp, err := safeClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("request API: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		_, _ = io.Copy(
			io.Discard,
			io.LimitReader(resp.Body, maxAPIErrorBody),
		)

		return nil, fmt.Errorf("API returned HTTP %d", resp.StatusCode)
	}

	body, err := io.ReadAll(
		io.LimitReader(resp.Body, maxAPIResponseSize+1),
	)
	if err != nil {
		return nil, fmt.Errorf("read API response: %w", err)
	}

	if len(body) > maxAPIResponseSize {
		return nil, fmt.Errorf(
			"API response exceeds %d bytes",
			maxAPIResponseSize,
		)
	}

	return body, nil
}

// parseAPIRecords converts a JSON array or a "data" array into RawRecord values.
func parseAPIRecords(body []byte) ([]RawRecord, error) {
	var rawRecords []json.RawMessage

	if err := json.Unmarshal(body, &rawRecords); err != nil {
		var envelope struct {
			Data []json.RawMessage `json:"data"`
		}

		if envelopeErr := json.Unmarshal(body, &envelope); envelopeErr != nil ||
			envelope.Data == nil {
			return nil, fmt.Errorf(
				"API response must be a JSON array or an object containing a data array",
			)
		}

		rawRecords = envelope.Data
	}

	records := make([]RawRecord, 0, len(rawRecords))

	for i, raw := range rawRecords {
		var fields map[string]json.RawMessage

		if err := json.Unmarshal(raw, &fields); err != nil ||
			fields == nil {
			return nil, fmt.Errorf(
				"API record %d must be a JSON object",
				i+1,
			)
		}

		record := make(RawRecord, len(fields))

		for key, value := range fields {
			var text string

			if err := json.Unmarshal(value, &text); err != nil {
				// Preserve numbers, booleans, arrays, and objects as JSON text.
				text = string(value)
			}

			record[key] = text
		}

		records = append(records, record)
	}

	return records, nil
}

func validatePublicHost(ctx context.Context, hostname string) error {
	if ip, err := netip.ParseAddr(hostname); err == nil {
		if !isPublicIP(ip) {
			return fmt.Errorf("host resolves to a non-public IP address")
		}
		return nil
	}

	ips, err := net.DefaultResolver.LookupIPAddr(ctx, hostname)
	if err != nil {
		return fmt.Errorf("resolve API hostname: %w", err)
	}

	if len(ips) == 0 {
		return fmt.Errorf("hostname has no IP addresses")
	}

	for _, result := range ips {
		ip, ok := netip.AddrFromSlice(result.IP)
		if !ok || !isPublicIP(ip) {
			return fmt.Errorf(
				"hostname resolves to a non-public IP address",
			)
		}
	}

	return nil
}

func isPublicIP(ip netip.Addr) bool {
	ip = ip.Unmap()

	return ip.IsValid() &&
		!ip.IsPrivate() &&
		!ip.IsLoopback() &&
		!ip.IsLinkLocalUnicast() &&
		!ip.IsLinkLocalMulticast() &&
		!ip.IsMulticast() &&
		!ip.IsUnspecified()
}
