package main

type ConfigSchemaGen struct {
	Dest         string `json:"dest,omitempty"`
	Package      string `json:"package,omitempty"`
	EmitJsonTags bool   `json:"emit_json_tags,omitempty"`
}

type ConfigSchema struct {
	Gen ConfigSchemaGen `json:"gen,omitempty"`
}

type Config struct {
	DSN     string                  `json:"dsn,omitempty"`
	Schemas map[string]ConfigSchema `json:"schemas,omitempty"`
}
