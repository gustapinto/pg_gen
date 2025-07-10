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

	_ "github.com/jackc/pgx/v5/stdlib"
)

var (
	//go:embed templates/go/dao.txt
	_daoTemplate string

	//go:embed templates/go/common.txt
	_commonTemplate string

	//go:embed templates/go/view.txt
	_viewTemplate string
)

const (
	_table = "table"
	_view  = "view"
)

type pgColumn struct {
	Name         string `json:"name"`
	SqlDataType  string `json:"sql_data_type"`
	GoDataType   string `json:"go_data_type"`
	Nullable     bool   `json:"nullable"`
	IsPrimaryKey bool   `json:"is_primary_key"`
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

func (t *pgTable) generateDAOCode(packageName string, emitJsonTags bool) ([]byte, error) {
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

	tableRawCode := tableReplacer.Replace(_daoTemplate)
	tableFormattedCode, err := format.Source([]byte(tableRawCode))
	if err != nil {
		return nil, err
	}

	return tableFormattedCode, nil
}

func (t *pgTable) generateViewCode(packageName string, emitJsonTags bool) ([]byte, error) {
	viewReplacer := strings.NewReplacer(
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

	viewRawCode := viewReplacer.Replace(_viewTemplate)
	viewFormattedCode, err := format.Source([]byte(viewRawCode))
	if err != nil {
		return nil, err
	}

	return viewFormattedCode, nil
}

func (t *pgTable) entityName() string {
	return strcase.ToCamel(t.Name)
}

func (t *pgTable) FileName() string {
	return strcase.ToSnake(t.Name)
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

		views, err := pcg.getPgViews(schemaName)
		if err != nil {
			return err
		}

		for _, view := range views {
			tables = append(tables, view)
		}

		err = pcg.generateCodeForTables(
			tables,
			schema.GO.Dest,
			schema.GO.Package,
			schema.GO.EmitJsonTags)
		if err != nil {
			return err
		}
	}

	return nil
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

	var tables []pgTable
	for rows.Next() {
		var table pgTable
		var columnsJson []byte

		if err := rows.Scan(&table.Name, &columnsJson); err != nil {
			return nil, err
		}

		if err := json.Unmarshal(columnsJson, &table.Columns); err != nil {
			return nil, err
		}

		table.Kind = _table
		tables = append(tables, table)
	}

	return tables, nil
}

func (pcg *PgCodeGenerator) generateCommonCode(rootDirectory string, packageName string) error {
	replacer := strings.NewReplacer("{goPackageName}", packageName)

	rawCode := replacer.Replace(_commonTemplate)
	formattedCode, err := format.Source([]byte(rawCode))
	if err != nil {
		return fmt.Errorf("failed to generate commoon code for package [%s], got error [%s]", packageName, err.Error())
	}

	commonFilepath := fmt.Sprintf("%s/%s.go", rootDirectory, packageName)
	if err := pcg.writeToFile(commonFilepath, formattedCode); err != nil {
		return fmt.Errorf("failed to write file [%s], got error [%s]", commonFilepath, err.Error())
	}

	return nil
}

func (pcg *PgCodeGenerator) generateDAO(
	table pgTable,
	rootDirectory string,
	packageName string,
	emitJsonTags bool,
) error {
	if table.Kind != _table {
		return nil
	}

	code, err := table.generateDAOCode(packageName, emitJsonTags)
	if err != nil {
		return fmt.Errorf("failed to generate code for table [%s], got error [%s]", table.Name, err.Error())
	}

	filepath := fmt.Sprintf("%s/%s_dao.go", rootDirectory, table.FileName())
	if err := pcg.writeToFile(filepath, code); err != nil {
		return fmt.Errorf("failed to write file [%s], got error [%s]", filepath, err.Error())
	}

	log.Printf("- Generated [%s] for table [%s]\n", filepath, table.Name)

	return nil
}

func (pcg *PgCodeGenerator) generateView(
	table pgTable,
	rootDirectory string,
	packageName string,
	emitJsonTags bool,
) error {
	if table.Kind != _view {
		return nil
	}

	code, err := table.generateViewCode(packageName, emitJsonTags)
	if err != nil {
		return fmt.Errorf("failed to generate code for view [%s], got error [%s]", table.Name, err.Error())
	}

	filepath := fmt.Sprintf("%s/%s_view.go", rootDirectory, table.FileName())
	if err := pcg.writeToFile(filepath, code); err != nil {
		return fmt.Errorf("failed to write file [%s], got error [%s]", filepath, err.Error())
	}

	log.Printf("- Generated [%s] for view [%s]\n", filepath, table.Name)

	return nil
}

func (pcg *PgCodeGenerator) generateCodeForTables(
	tables []pgTable,
	rootDirectory string,
	packageName string,
	emitJsonTags bool,
) error {
	log.Printf("Generating code for tables and views")

	if err := os.MkdirAll(rootDirectory, 0777); err != nil {
		return fmt.Errorf("failed to create root directory [%s], got error [%s]", rootDirectory, err.Error())
	}

	if err := pcg.generateCommonCode(rootDirectory, packageName); err != nil {
		return err
	}

	for _, table := range tables {
		var err error
		switch table.Kind {
		case _table:
			err = pcg.generateDAO(table, rootDirectory, packageName, emitJsonTags)

		case _view:
			err = pcg.generateView(table, rootDirectory, packageName, emitJsonTags)
		}

		if err != nil {
			return err
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

	var views []pgTable
	for rows.Next() {
		var view pgTable
		var columnsJson []byte

		if err := rows.Scan(&view.Name, &columnsJson); err != nil {
			return nil, err
		}

		if err := json.Unmarshal(columnsJson, &view.Columns); err != nil {
			return nil, err
		}

		view.Kind = _view
		views = append(views, view)
	}

	return views, nil
}
