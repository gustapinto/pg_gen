package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"go/format"
	"log"
	"os"
	"strconv"
	"strings"

	_ "embed"

	"github.com/iancoleman/strcase"
)

//go:embed templates/table.txt
var _tableTemplate string

type PgColumn struct {
	Name         string `json:"name"`
	SqlDataType  string `json:"sql_data_type"`
	GoDataType   string `json:"go_data_type"`
	Nullable     bool   `json:"nullable"`
	IsPrimaryKey bool   `json:"is_primary_key"`
}

func (c *PgColumn) GoName() string {
	return strcase.ToCamel(c.Name)
}

func (c *PgColumn) JsonTags() string {
	var sb strings.Builder
	sb.WriteString("`json:\"")
	sb.WriteString(strcase.ToSnake(c.Name))
	sb.WriteString(",omitempty\"`")

	return sb.String()
}

type PgTable struct {
	Name    string     `json:"name,omitempty"`
	Columns []PgColumn `json:"columns,omitempty"`
}

func (t *PgTable) GenerateCode(packageName string, emitJsonTags bool) ([]byte, error) {
	tableReplacer := strings.NewReplacer(
		"{goPackageName}", packageName,
		"{goEntityName}", t.entityName(),
		"{goEntityFields}", t.goEntityFields(emitJsonTags),
		"{goSelectOneScanFields}", t.goSelectOneScanFields(),
		"{goSelectManyScanFields}", t.goSelectManyScanFields(),
		"{goInsertValues}", t.goInsertValues(),
		"{goUpdateValues}", t.goInsertValues(),
		"{sqlSelectFields}", t.sqlSelectFields(),
		"{sqlTableName}", t.sqlTableName(),
		"{sqlPrimaryKeyColumn}", t.sqlPrimaryKey(),
		"{sqlUpdatePlaceholders}", t.sqlUpdatePlaceholders(),
		"{sqlInsertFields}", t.sqlInsertFields(),
		"{sqlInsertPlaceholders}", t.sqlInsertPlaceholders(),
	)

	tableRawCode := tableReplacer.Replace(_tableTemplate)
	tableFormattedCode, err := format.Source([]byte(tableRawCode))
	if err != nil {
		return nil, err
	}

	return tableFormattedCode, nil
}

func (t *PgTable) entityName() string {
	return strcase.ToCamel(t.Name)
}

func (t *PgTable) FileName() string {
	return strcase.ToSnake(t.Name)
}

func (t *PgTable) goEntityFields(emitJsonTags bool) string {
	var sb strings.Builder

	for _, col := range t.Columns {
		sb.WriteString(col.GoName())
		sb.WriteString(" ")

		if col.Nullable && col.GoDataType != "any" {
			sb.WriteString("*")
		}

		sb.WriteString(col.GoDataType)

		if emitJsonTags {
			sb.WriteString(col.JsonTags())
		}

		sb.WriteString("\n")
	}

	return sb.String()
}

func (t *PgTable) sqlPrimaryKey() string {
	for _, col := range t.Columns {
		if col.IsPrimaryKey {
			return col.Name
		}
	}

	return "id" // assumed default
}

func (t *PgTable) sqlTableName() string {
	return t.Name
}

func (t *PgTable) sqlSelectFields() string {
	var sb strings.Builder

	colSize := len(t.Columns) - 1
	for i, col := range t.Columns {
		sb.WriteString("\"")
		sb.WriteString(col.Name)
		sb.WriteString("\"")

		if i < colSize {
			sb.WriteString(", ")
		}
	}

	return sb.String()
}

func (t *PgTable) sqlInsertFields() string {
	var sb strings.Builder

	colSize := len(t.Columns) - 1
	for i, col := range t.Columns {
		sb.WriteString("\"")
		sb.WriteString(col.Name)
		sb.WriteString("\"")

		if i < colSize {
			sb.WriteString(", ")
		}
	}

	return sb.String()
}

func (t *PgTable) goSelectOneScanFields() string {
	var sb strings.Builder

	colSize := len(t.Columns) - 1
	for i, col := range t.Columns {
		sb.WriteString("&result.Data.")
		sb.WriteString(col.GoName())

		if i < colSize {
			sb.WriteString(", ")
		}
	}

	return sb.String()
}

func (t *PgTable) goSelectManyScanFields() string {
	var sb strings.Builder

	colSize := len(t.Columns) - 1
	for i, col := range t.Columns {
		sb.WriteString("&entity.")
		sb.WriteString(col.GoName())

		if i < colSize {
			sb.WriteString(", ")
		}
	}

	return sb.String()
}

func (t *PgTable) sqlInsertPlaceholders() string {
	var sb strings.Builder

	colSize := len(t.Columns) - 1
	for i, col := range t.Columns {
		sb.WriteString("$")
		sb.WriteString(strconv.Itoa(i + 1))
		sb.WriteString("::")
		sb.WriteString(col.SqlDataType)

		if i < colSize {
			sb.WriteString(", ")
		}
	}

	return sb.String()
}

func (t *PgTable) sqlUpdatePlaceholders() string {
	var sb strings.Builder

	colSize := len(t.Columns) - 1
	position := 2 // $1 Is used for the primary key filter
	for i, col := range t.Columns {
		if col.IsPrimaryKey {
			continue
		}

		sb.WriteString("\"")
		sb.WriteString(col.Name)
		sb.WriteString("\" = $")
		sb.WriteString(strconv.Itoa(position))
		sb.WriteString("::")
		sb.WriteString(col.SqlDataType)

		if i < colSize {
			sb.WriteString(", ")
		}

		position++
	}

	return sb.String()
}

func (t *PgTable) goInsertValues() string {
	var sb strings.Builder

	colSize := len(t.Columns) - 1
	for i, col := range t.Columns {
		sb.WriteString("values.")
		sb.WriteString(col.GoName())

		if i < colSize {
			sb.WriteString(", ")
		}
	}

	return sb.String()
}

type PgCodeGenerator struct {
	db  *sql.DB
	cfg *Config
}

func NewPgCodeGenerator(cfg *Config) (*PgCodeGenerator, error) {
	db, err := sql.Open("pgx", cfg.DSN)
	if err != nil {
		return nil, err
	}

	return &PgCodeGenerator{
		db:  db,
		cfg: cfg,
	}, nil
}

func (pcg *PgCodeGenerator) Close() error {
	return pcg.db.Close()
}

func (pcg *PgCodeGenerator) Generate() error {
	for schemaName, schema := range pcg.cfg.Schemas {
		tables, err := pcg.getPgTables(schemaName)
		if err != nil {
			return err
		}

		err = pcg.generateCodeForTables(
			tables,
			schema.Gen.Dest,
			schema.Gen.Package,
			schema.Gen.EmitJsonTags)
		if err != nil {
			return err
		}
	}

	return nil
}

func (pcg *PgCodeGenerator) getPgTables(schema string) ([]PgTable, error) {
	const query = `
	SELECT
		t.tablename AS name,
		json_agg(json_build_object(
			'name', c.column_name,
			'nullable', (c.is_nullable = 'YES'),
			'sql_data_type', UPPER(c.udt_name),
			'go_data_type', (CASE
				WHEN UPPER(c.udt_name) = 'UUID' THEN 'uuid.UUID'
				WHEN UPPER(c.udt_name) IN ('VARCHAR', 'TEXT') THEN 'string'
				WHEN UPPER(c.udt_name) IN ('TIMESTAMP', 'DATE', 'DATETIME') THEN 'time.Time'
				WHEN UPPER(c.udt_name) IN ('INT4', 'INTEGER', 'BIGINT', 'SMALLINT') THEN 'int64'
				WHEN UPPER(c.udt_name) IN ('DECIMAL', 'FLOAT', 'DOUBLE PRECISION') THEN 'float64'
				WHEN UPPER(c.udt_name) = 'BOOLEAN' THEN 'bool'
				ELSE 'any'
			END),
			'is_primary_key', (ccu.column_name IS NOT NULL)
		)) AS columns
	FROM
		pg_catalog.pg_tables t
	INNER JOIN information_schema.columns c ON
		c.table_name = t.tablename
	LEFT JOIN information_schema.table_constraints tc ON
		tc.table_name = t.tablename
		AND tc.constraint_type = 'PRIMARY KEY'
	LEFT JOIN information_schema.constraint_column_usage ccu ON
		ccu.constraint_name = tc.constraint_name
		AND ccu.constraint_schema = tc.constraint_schema
		AND ccu.column_name = c.column_name
	WHERE
		t.schemaname = $1
	GROUP BY
		t.tablename
	`

	// Tables
	rows, err := pcg.db.Query(query, schema)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var tables []PgTable
	for rows.Next() {
		var table PgTable
		var columnsJson []byte

		if err := rows.Scan(&table.Name, &columnsJson); err != nil {
			return nil, err
		}

		if err := json.Unmarshal(columnsJson, &table.Columns); err != nil {
			return nil, err
		}

		tables = append(tables, table)
	}

	return tables, nil
}

func (pcg *PgCodeGenerator) generateCodeForTables(
	tables []PgTable,
	rootDirectory string,
	packageName string,
	emitJsonTags bool,
) error {
	log.Printf("Generating code for tables")

	if err := os.MkdirAll(rootDirectory, 0777); err != nil {
		return fmt.Errorf("failed to create root directory [%s], got error [%s]", rootDirectory, err.Error())
	}

	for _, table := range tables {
		code, err := table.GenerateCode(packageName, emitJsonTags)
		if err != nil {
			return fmt.Errorf("failed to generate code for table [%s], got error [%s]", table.Name, err.Error())
		}

		filepath := fmt.Sprintf("%s/%s.go", rootDirectory, table.FileName())
		if err := pcg.writeToFile(filepath, code); err != nil {
			return fmt.Errorf("failed to write file [%s], got error [%s]", filepath, err.Error())
		}

		log.Printf("- Generated [%s] for table [%s]\n", filepath, table.Name)
	}

	return nil
}

func (pcg *PgCodeGenerator) writeToFile(filepath string, data []byte) error {
	dest, err := os.Create(filepath)
	if err != nil {
		return err
	}
	defer dest.Close()

	if _, err := dest.Write(data); err != nil {
		return err
	}

	return dest.Sync()
}
