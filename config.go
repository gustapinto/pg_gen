package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"

	"github.com/goccy/go-yaml"
)

type ConfigSchemaGO struct {
	Dest         string `json:"dest" yaml:"dest"`
	Package      string `json:"package" yaml:"package"`
	EmitJsonTags bool   `json:"emit_json_tags" yaml:"emit_json_tags"`
}

func (csg *ConfigSchemaGO) Validate(name string) error {
	if strIsEmpty(csg.Dest) {
		return fmt.Errorf("$.schemas.%s.go.dest must be present and not be blank", name)
	}

	if strIsEmpty(csg.Package) {
		return fmt.Errorf("$.schemas.%s.go.package must be present and not be blank", name)
	}

	return nil
}

type ConfigSchema struct {
	IncludeViews bool            `json:"include_views" yaml:"include_views"`
	Ignore       []string        `json:"ignore" yaml:"ignore"`
	GO           *ConfigSchemaGO `json:"go" yaml:"go"`
}

func (cs *ConfigSchema) Validate(name string) error {
	if cs.GO == nil {
		return fmt.Errorf("$.schemas.%s.go must be present and not be blank", name)
	}

	if err := cs.GO.Validate(name); err != nil {
		return err
	}

	return nil
}

func (cs *ConfigSchema) ShouldIgnore(objectName string) bool {
	if len(cs.Ignore) == 0 {
		return false
	}

	return slices.ContainsFunc(cs.Ignore, func(str string) bool {
		return strings.ToLower(objectName) == strings.ToLower(str)
	})
}

type Config struct {
	DSN     string                  `json:"dsn" yaml:"dsn"`
	Schemas map[string]ConfigSchema `json:"schemas" yaml:"schemas"`
}

func (c *Config) Validate() error {
	if strIsEmpty(c.DSN) {
		return errors.New("$.dsn must be present and not be blank")
	}

	if len(c.Schemas) == 0 {
		return errors.New("$.schemas must be present and not be empty")
	}

	for name, schema := range c.Schemas {
		if err := schema.Validate(name); err != nil {
			return err
		}
	}

	return nil
}

func LoadConfigFromFile(path string) (*Config, error) {
	if strIsEmpty(path) {
		return nil, errors.New("invalid path to config, it must be a non blank, valid YAML or JSON file")
	}

	ext := filepath.Ext(path)
	configContent, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read file [%s]", path)
	}

	var config Config
	switch ext {
	case ".json":
		if err := json.Unmarshal(configContent, &config); err != nil {
			return nil, fmt.Errorf("failed to decode file [%s] as JSON", path)
		}

	case ".yaml", ".yml":
		if err := yaml.Unmarshal(configContent, &config); err != nil {
			return nil, fmt.Errorf("failed to decode file [%s] as YAML", path)
		}

	default:
		return nil, errors.New("filepath must point to a valid JSON or YAML file")
	}

	return &config, nil
}
