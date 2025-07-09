package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

type ConfigSchemaGO struct {
	Dest         string `json:"dest,omitempty"`
	Package      string `json:"package,omitempty"`
	EmitJsonTags bool   `json:"emit_json_tags,omitempty"`
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
	GO *ConfigSchemaGO `json:"go,omitempty"`
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

type Config struct {
	DSN     string                  `json:"dsn,omitempty"`
	Schemas map[string]ConfigSchema `json:"schemas,omitempty"`
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
		return nil, errors.New("invalid path to config, it must be a non blank, valid JSON file")
	}

	ext := filepath.Ext(path)
	if ext != ".json" {
		return nil, errors.New("filepath must point to a valid .json file")
	}

	configContent, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read file [%s]", path)
	}

	var config Config
	if err := json.Unmarshal(configContent, &config); err != nil {
		return nil, fmt.Errorf("failed to decode file [%s] as JSON", path)
	}

	return &config, nil
}
