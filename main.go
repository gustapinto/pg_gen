package main

import (
	"flag"
	"log"
)

func run() error {
	configPath := flag.String("config", "", "The config JSON file path")
	flag.Parse()

	config, err := LoadConfigFromFile(*configPath)
	if err != nil {
		return err
	}

	if err := config.Validate(); err != nil {
		return err
	}

	pgGen, err := NewPgCodeGenerator(config)
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
