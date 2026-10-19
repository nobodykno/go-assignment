
package ingestion

import (
	"fmt"
	"path/filepath"
	"strings"
)

// ReadFile selects the correct reader based on the file extension.
func ReadFile(filePath string) ([]RawRecord, error) {
	switch strings.ToLower(filepath.Ext(filePath)) {
	case ".csv":
		return ReadCSV(filePath)
	case ".json":
		return ReadJSON(filePath)
	default:
		return nil, fmt.Errorf(
			"unsupported file extension: %s",
			filepath.Ext(filePath),
		)
	}
}
