//go:generate go run github.com/gustapinto/pg_gen@latest -config=./example.cfg.json
package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"

	"github.com/gustapinto/pg_gen/example/gen"
	_ "github.com/jackc/pgx/v5/stdlib"
)

const (
	dsn = "postgresql://example_db_user:example_db_passw@localhost:5432/example_db?sslmode=disable"
)

func main() {
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		panic(err)
	}
	defer db.Close()

	projects := gen.Projects{}
	res, err := projects.Select(context.Background(), db, &gen.SelectOptions{
		Where: gen.Where(
			gen.NewFilter("tier", "!=", "free"),
		),
		OrderBy: gen.OrderBy(
			gen.NewDirection("name", "asc"),
		),
	})
	if err != nil {
		panic(err)
	}

	j, _ := json.MarshalIndent(res, "", "  ")
	fmt.Println(string(j))
}
