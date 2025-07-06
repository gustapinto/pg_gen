package main

import (
	"encoding/json"
	"flag"
	"log"
	"os"

	_ "embed"

	_ "github.com/jackc/pgx/v5/stdlib"
)

func run() error {
	configPath := flag.String("config", "pg_gen.json", "The config file path")
	flag.Parse()

	configContent, err := os.ReadFile(*configPath)
	if err != nil {
		return err
	}

	var config Config
	if err := json.Unmarshal(configContent, &config); err != nil {
		return err
	}

	pgGen, err := NewPgCodeGenerator(&config)
	if err != nil {
		return err
	}
	defer pgGen.Close()

	return pgGen.Generate()
}

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}
