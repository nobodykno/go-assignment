package main

import (
	"fmt"
	"strings"
	"testing"
)

func TestValidationSummaryCapsStoredMessages(t *testing.T) {
	var summary validationSummary

	for i := 1; i <= maxStoredErrorMessages+5; i++ {
		summary.addError(fmt.Sprintf("record %d: invalid", i))
	}

	message := summary.errorMessage()

	if strings.Count(message, "invalid") != maxStoredErrorMessages {
		t.Fatalf("expected %d stored messages", maxStoredErrorMessages)
	}
	if !strings.HasSuffix(message, "; and 5 more validation error(s)") {
		t.Fatalf("expected an omitted-count suffix, got %q", message[len(message)-60:])
	}
}

func TestValidationSummaryEmpty(t *testing.T) {
	var summary validationSummary
	if got := summary.errorMessage(); got != "" {
		t.Fatalf("expected an empty message, got %q", got)
	}
}
