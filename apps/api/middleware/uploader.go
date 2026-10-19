
package middleware

import (
	"fmt"
	"path/filepath"
	"strings"
)

const MaxUploadSize int64 = 10 << 20 // 10 MiB

var allowedExtensions = map[string]bool{
	".csv":  true,
	".json": true,
}

func ValidateFileExtension(filename string) error {
	// Extract the file extension.
	ext := strings.ToLower(filepath.Ext(filename))

	// Check whether the extension is supported.
	if !allowedExtensions[ext] {
		return fmt.Errorf("unsupported file extension: %s", ext)
	}

	return nil
}
