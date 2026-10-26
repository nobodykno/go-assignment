
package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"go-assignment/packages/models"
)

// AggregatedInput is the structure produced by the aggregation service.
type AggregatedInput struct {
	Source      string             `json:"source"`
	Date        string             `json:"date"`
	Dimensions  map[string]string  `json:"dimensions"`
	Metrics     map[string]float64 `json:"metrics"`
	RecordCount int                `json:"record_count"`
}

// ReportGroup represents one source-specific group, such as a country,
// crypto asset, or weather location.
type ReportGroup struct {
	Dimensions  map[string]string  `json:"dimensions"`
	Metrics     map[string]float64 `json:"metrics"`
	RecordCount int                `json:"record_count"`
}

// SourceReport contains all groups for a source on a particular date.
type SourceReport struct {
	Groups []ReportGroup `json:"groups"`
}

// GlobalDailyReport combines all sources for one date.
type GlobalDailyReport struct {
	Date    string                  `json:"date"`
	Sources map[string]SourceReport `json:"sources"`
}

// createReportGroupKey generates the stable group key for a daily report.
// The database uniqueness constraint also includes job_id and report_date,
// so one report can be stored for each job and date.
func createReportGroupKey() string {
	hash := sha256.Sum256([]byte("global-daily-report"))
	return hex.EncodeToString(hash[:])
}

// ExportFile reads aggregated JSON, creates one Global Daily Report per date,
// and writes the final report to outputPath.
func ExportFile(
	inputPath string,
	outputPath string,
	jobID uint,
) ([]models.ExportedReport, error) {
	input, err := os.ReadFile(inputPath)
	if err != nil {
		return nil, fmt.Errorf("read aggregated file: %w", err)
	}

	decoder := json.NewDecoder(bytes.NewReader(input))

	var records []AggregatedInput

	if err := decoder.Decode(&records); err != nil {
		return nil, fmt.Errorf("decode aggregated JSON: %w", err)
	}

	// Ensure the file contains exactly one JSON value.
	var extra interface{}

	if err := decoder.Decode(&extra); err != io.EOF {
		if err == nil {
			return nil, fmt.Errorf(
				"aggregated file contains trailing JSON data",
			)
		}

		return nil, fmt.Errorf(
			"aggregated file contains trailing content: %w",
			err,
		)
	}

	if len(records) == 0 {
		return nil, fmt.Errorf("aggregated report contains no records")
	}

	reportsByDate := make(map[string]*GlobalDailyReport)

	// Track source + dimensions within each date to reject duplicate groups.
	groupKeysByDate := make(map[string]map[string]struct{})

	for index, record := range records {
		dateValue := strings.TrimSpace(record.Date)

		if dateValue == "" {
			return nil, fmt.Errorf(
				"record %d has no valid date field",
				index+1,
			)
		}

		reportDate, err := time.Parse("2006-01-02", dateValue)
		if err != nil {
			return nil, fmt.Errorf(
				"record %d has invalid date %q: %w",
				index+1,
				dateValue,
				err,
			)
		}

		source := strings.TrimSpace(record.Source)
		if source == "" {
			source = "unknown"
		}

		if record.Dimensions == nil {
			record.Dimensions = make(map[string]string)
		}

		if record.Metrics == nil {
			record.Metrics = make(map[string]float64)
		}

		if record.RecordCount < 1 {
			return nil, fmt.Errorf(
				"record %d has invalid record_count",
				index+1,
			)
		}

		dateKey := reportDate.Format("2006-01-02")

		if _, exists := reportsByDate[dateKey]; !exists {
			reportsByDate[dateKey] = &GlobalDailyReport{
				Date:    dateKey,
				Sources: make(map[string]SourceReport),
			}

			groupKeysByDate[dateKey] = make(map[string]struct{})
		}

		dimensionJSON, err := json.Marshal(record.Dimensions)
		if err != nil {
			return nil, fmt.Errorf(
				"encode dimensions for record %d: %w",
				index+1,
				err,
			)
		}

		groupKey := source + ":" + string(dimensionJSON)

		if _, exists := groupKeysByDate[dateKey][groupKey]; exists {
			return nil, fmt.Errorf(
				"duplicate source/dimensions group for date %s and source %s",
				dateKey,
				source,
			)
		}

		groupKeysByDate[dateKey][groupKey] = struct{}{}

		report := reportsByDate[dateKey]

		sourceReport := report.Sources[source]

		sourceReport.Groups = append(
			sourceReport.Groups,
			ReportGroup{
				Dimensions:  record.Dimensions,
				Metrics:     record.Metrics,
				RecordCount: record.RecordCount,
			},
		)

		report.Sources[source] = sourceReport
	}

	// Sort dates to produce deterministic report output.
	dates := make([]string, 0, len(reportsByDate))

	for date := range reportsByDate {
		dates = append(dates, date)
	}

	sort.Strings(dates)

	finalReports := make([]GlobalDailyReport, 0, len(dates))
	dbReports := make([]models.ExportedReport, 0, len(dates))

	groupKey := createReportGroupKey()

	for _, date := range dates {
		report := reportsByDate[date]

		// Sort each source's groups by their dimensions.
		for source, sourceReport := range report.Sources {
			sort.Slice(
				sourceReport.Groups,
				func(i, j int) bool {
					left, _ := json.Marshal(
						sourceReport.Groups[i].Dimensions,
					)

					right, _ := json.Marshal(
						sourceReport.Groups[j].Dimensions,
					)

					return string(left) < string(right)
				},
			)

			report.Sources[source] = sourceReport
		}

		finalReports = append(finalReports, *report)

		reportDate, err := time.Parse("2006-01-02", report.Date)
		if err != nil {
			return nil, fmt.Errorf(
				"parse report date %q: %w",
				report.Date,
				err,
			)
		}

		reportData, err := toReportMap(*report)
		if err != nil {
			return nil, fmt.Errorf(
				"encode report for %s: %w",
				date,
				err,
			)
		}

		dbReports = append(
			dbReports,
			models.ExportedReport{
				JobID:      jobID,
				ReportDate: reportDate,
				GroupKey:   groupKey,
				ReportData: reportData,
			},
		)
	}

	outputDir := filepath.Dir(outputPath)

	if err := os.MkdirAll(outputDir, 0755); err != nil {
		return nil, fmt.Errorf(
			"create export directory: %w",
			err,
		)
	}

	// Write atomically so readers do not see incomplete JSON.
	tempFile, err := os.CreateTemp(outputDir, ".report-*.tmp")
	if err != nil {
		return nil, fmt.Errorf(
			"create temporary report: %w",
			err,
		)
	}

	tempPath := tempFile.Name()

	defer func() {
		_ = os.Remove(tempPath)
	}()

	encoder := json.NewEncoder(tempFile)
	encoder.SetIndent("", "  ")

	if err := encoder.Encode(finalReports); err != nil {
		_ = tempFile.Close()
		return nil, fmt.Errorf("write final report: %w", err)
	}

	if err := tempFile.Sync(); err != nil {
		_ = tempFile.Close()
		return nil, fmt.Errorf("sync final report: %w", err)
	}

	if err := tempFile.Close(); err != nil {
		return nil, fmt.Errorf("close final report: %w", err)
	}

	if err := os.Rename(tempPath, outputPath); err != nil {
		return nil, fmt.Errorf("publish final report: %w", err)
	}

	return dbReports, nil
}

// toReportMap converts the daily report to the map expected by
// models.ExportedReport.ReportData.
func toReportMap(
	report GlobalDailyReport,
) (map[string]interface{}, error) {
	encoded, err := json.Marshal(report)
	if err != nil {
		return nil, err
	}

	var result map[string]interface{}

	if err := json.Unmarshal(encoded, &result); err != nil {
		return nil, err
	}

	return result, nil
}
