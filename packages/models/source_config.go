package models

type SourceDefinition struct {
	Name     string            `json:"name"`
	Type     string            `json:"type"`
	FilePath string            `json:"file_path,omitempty"`
	URL      string            `json:"url,omitempty"`
	Options  map[string]string `json:"options,omitempty"`
}

type SourceConfig struct {
	Sources []SourceDefinition `json:"sources"`
}
