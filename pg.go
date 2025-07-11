package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"go/format"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	_ "embed"

	"github.com/iancoleman/strcase"

	_ "github.com/jackc/pgx/v5/stdlib"
)

var (
	//go:embed templates/go/table.txt
	_tableTemplate string

	//go:embed templates/go/common.txt
	_commonTemplate string

	//go:embed templates/go/view.txt
	_viewTemplate string
)

const (
	_table  = "table"
	_view   = "view"
	_commom = "commom"
)

type pgColumn struct {
	Name         string `json:"name,omitempty"`
	SqlDataType  string `json:"sql_data_type,omitempty"`
	GoDataType   string `json:"go_data_type,omitempty"`
	Nullable     bool   `json:"nullable,omitempty"`
	IsPrimaryKey bool   `json:"is_primary_key,omitempty"`
}

func (c *pgColumn) goName() string {
	return strcase.ToCamel(c.Name)
}

func (c *pgColumn) jsonTags() string {
	var sb strings.Builder
	sb.WriteString("`json:\"")
	sb.WriteString(strcase.ToSnake(c.Name))
	sb.WriteString("\"`")

	return sb.String()
}

type pgTable struct {
	Kind    string     `json:"kind,omitempty"`
	Name    string     `json:"name,omitempty"`
	Columns []pgColumn `json:"columns,omitempty"`
}

func (t *pgTable) replacer(packageName string, emitJsonTags bool) *strings.Replacer {
	return strings.NewReplacer(
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
}

func (t *pgTable) generateGoCode(packageName, template string, emitJsonTags bool) ([]byte, error) {
	rawCode := t.replacer(packageName, emitJsonTags).Replace(template)
	formattedCode, err := format.Source([]byte(rawCode))
	if err != nil {
		return nil, err
	}

	return formattedCode, nil
}

func (t *pgTable) entityName() string {
	return strcase.ToCamel(t.Name)
}

func (t *pgTable) goFilepath(rootDirectory string) string {
	var sb strings.Builder
	sb.WriteString(rootDirectory)
	sb.WriteString("/")
	sb.WriteString(strcase.ToSnake(t.Name))
	sb.WriteString(".go")

	return filepath.Clean(sb.String())
}

func (t *pgTable) goEntityFields(emitJsonTags bool) string {
	var sb strings.Builder

	for _, col := range t.Columns {
		sb.WriteString(col.goName())
		sb.WriteString(" ")

		if col.Nullable && col.GoDataType != "any" {
			sb.WriteString("*")
		}

		sb.WriteString(col.GoDataType)

		if emitJsonTags {
			sb.WriteString(col.jsonTags())
		}

		sb.WriteString("\n")
	}

	return sb.String()
}

func (t *pgTable) sqlPrimaryKey() string {
	for _, col := range t.Columns {
		if col.IsPrimaryKey {
			return col.Name
		}
	}

	return "id" // assumed default
}

func (t *pgTable) sqlTableName() string {
	return t.Name
}

func (t *pgTable) sqlSelectFields() string {
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

func (t *pgTable) sqlInsertFields() string {
	return t.sqlSelectFields()
}

func (t *pgTable) goSelectOneScanFields() string {
	var sb strings.Builder

	colSize := len(t.Columns) - 1
	for i, col := range t.Columns {
		sb.WriteString("&result.Data.")
		sb.WriteString(col.goName())

		if i < colSize {
			sb.WriteString(", ")
		}
	}

	return sb.String()
}

func (t *pgTable) goSelectManyScanFields() string {
	var sb strings.Builder

	colSize := len(t.Columns) - 1
	for i, col := range t.Columns {
		sb.WriteString("&entity.")
		sb.WriteString(col.goName())

		if i < colSize {
			sb.WriteString(", ")
		}
	}

	return sb.String()
}

func (t *pgTable) sqlInsertPlaceholders() string {
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

func (t *pgTable) sqlUpdatePlaceholders() string {
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

func (t *pgTable) goInsertValues() string {
	var sb strings.Builder

	colSize := len(t.Columns) - 1
	for i, col := range t.Columns {
		sb.WriteString("values.")
		sb.WriteString(col.goName())

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

		if schema.IncludeViews {
			views, err := pcg.getPgViews(schemaName)
			if err != nil {
				return err
			}

			for _, view := range views {
				tables = append(tables, view)
			}
		}

		err = pcg.generateCodeForTables(
			tables,
			schema,
			schema.GO.Dest,
			schema.GO.Package,
			schema.GO.EmitJsonTags)
		if err != nil {
			return err
		}
	}

	return nil
}

func (pcg *PgCodeGenerator) pgTableFromRows(rows *sql.Rows, kind string) ([]pgTable, error) {
	var pgTables []pgTable
	for rows.Next() {
		var pgTable pgTable
		var columnsJson []byte

		if err := rows.Scan(&pgTable.Name, &columnsJson); err != nil {
			return nil, err
		}

		if err := json.Unmarshal(columnsJson, &pgTable.Columns); err != nil {
			return nil, err
		}

		pgTable.Kind = kind
		pgTables = append(pgTables, pgTable)
	}

	return pgTables, nil
}

func (pcg *PgCodeGenerator) getPgTables(schema string) ([]pgTable, error) {
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

	tables, err := pcg.pgTableFromRows(rows, _table)
	if err != nil {
		return nil, err
	}

	return tables, nil
}

func (pcg *PgCodeGenerator) getPgViews(schema string) ([]pgTable, error) {
	const query = `
	SELECT
		v.table_name as name,
		json_agg(json_build_object(
			'name', a.attname,
			'sql_data_type', t.typname,
			'go_data_type', (CASE
				WHEN UPPER(t.typname) = 'UUID' THEN 'uuid.UUID'
				WHEN UPPER(t.typname) IN ('VARCHAR', 'TEXT') THEN 'string'
				WHEN UPPER(t.typname) IN ('TIMESTAMP', 'DATE', 'DATETIME') THEN 'time.Time'
				WHEN UPPER(t.typname) IN ('INT4', 'INTEGER', 'BIGINT', 'SMALLINT') THEN 'int64'
				WHEN UPPER(t.typname) IN ('DECIMAL', 'FLOAT', 'DOUBLE PRECISION') THEN 'float64'
				WHEN UPPER(t.typname) = 'BOOLEAN' THEN 'bool'
				ELSE 'any'
			END),
			'nullable', false,
			'is_primary_key', false
		)) as columns
	FROM
		information_schema.views v
	INNER JOIN pg_class c ON
		c.relname = v.table_name
	INNER JOIN pg_attribute a ON
		a.attrelid = c.oid
	INNER JOIN pg_type t ON
		t.oid = a.atttypid
	WHERE
		c.relkind = 'v'
		AND table_schema = $1
	GROUP BY
		v.table_name;
	`

	rows, err := pcg.db.Query(query, schema)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	views, err := pcg.pgTableFromRows(rows, _view)
	if err != nil {
		return nil, err
	}

	return views, nil
}

func (pcg *PgCodeGenerator) commomFilepath(rootDirectory, packageName string) string {
	var sb strings.Builder
	sb.WriteString(rootDirectory)
	sb.WriteString("/")
	sb.WriteString(packageName)
	sb.WriteString(".go")

	return filepath.Clean(sb.String())
}

func (pcg *PgCodeGenerator) generateGoCommonFile(rootDirectory, packageName string) error {
	rawCode := strings.ReplaceAll(_commonTemplate, "{goPackageName}", packageName)
	formattedCode, err := format.Source([]byte(rawCode))
	if err != nil {
		return fmt.Errorf("failed to generate commoon code for package [%s], got error [%s]", packageName, err.Error())
	}

	path := pcg.commomFilepath(rootDirectory, packageName)
	if err := pcg.writeToFile(path, formattedCode); err != nil {
		return fmt.Errorf("failed to write file [%s], got error [%s]", path, err.Error())
	}

	return nil
}

func (pcg *PgCodeGenerator) generateGoFile(
	table pgTable,
	rootDirectory string,
	packageName string,
	emitJsonTags bool,
) error {
	template := _tableTemplate
	if table.Kind == _view {
		template = _viewTemplate
	}

	code, err := table.generateGoCode(packageName, template, emitJsonTags)
	if err != nil {
		return fmt.Errorf("failed to generate code for %s [%s], got error [%s]", table.Kind, table.Name, err.Error())
	}

	path := table.goFilepath(rootDirectory)
	if err := pcg.writeToFile(path, code); err != nil {
		return fmt.Errorf("failed to write file [%s], got error [%s]", path, err.Error())
	}

	log.Printf("- Generated [%s] for %s [%s]\n", path, table.Kind, table.Name)

	return nil
}

func (pcg *PgCodeGenerator) generateCodeForTables(
	tables []pgTable,
	schema ConfigSchema,
	rootDirectory string,
	packageName string,
	emitJsonTags bool,
) error {
	log.Printf("Generating code for tables and views")

	if err := os.MkdirAll(rootDirectory, 0777); err != nil {
		return fmt.Errorf("failed to create root directory [%s], got error [%s]", rootDirectory, err.Error())
	}

	if schema.GO != nil {
		if err := pcg.generateGoCommonFile(rootDirectory, packageName); err != nil {
			return err
		}

		for _, table := range tables {
			if schema.ShouldIgnore(table.Name) {
				log.Printf("- Ignored code generation for %s [%s]\n", table.Kind, table.Name)
				continue
			}

			if err := pcg.generateGoFile(table, rootDirectory, packageName, emitJsonTags); err != nil {
				return err
			}
		}
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
