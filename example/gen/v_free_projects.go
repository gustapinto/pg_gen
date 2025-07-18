// Code generated by pg_gen, DO NOT EDIT.
package gen

import (
	"context"
	"database/sql"

	"github.com/google/uuid"
)

type VFreeProjects struct {
	Id   uuid.UUID `json:"id"`
	Name string    `json:"name"`
	Desc string    `json:"desc"`
}

func (self *VFreeProjects) Count(ctx context.Context, db *sql.DB, opts *SelectOptions) (uint, error) {
	query := `SELECT count(*) FROM "v_free_projects"`

	var values []any
	if opts != nil {
		filterPart, v := filtersToQueryPart(opts.Where)
		if filterPart != "" {
			query += filterPart
		}

		if v != nil {
			values = v
		}
	}

	row := db.QueryRowContext(ctx, query, values...)
	if row.Err() != nil {
		return 0, row.Err()
	}

	var count uint
	if err := row.Scan(&count); err != nil {
		return 0, err
	}

	return count, nil
}

func (self *VFreeProjects) Select(ctx context.Context, db *sql.DB, opts *SelectOptions) (*SelectResult[VFreeProjects], error) {
	query := `SELECT "id", "name", "desc" FROM "v_free_projects"`

	var values []any
	if opts != nil {
		filterPart, v := filtersToQueryPart(opts.Where)
		if filterPart != "" {
			query += filterPart
		}

		if v != nil {
			values = v
		}

		if orderByPart := opts.toOrderByPart(); orderByPart != "" {
			query += orderByPart
		}

		if limitPart := opts.toLimitOffsetPart(); limitPart != "" {
			query += limitPart
		}
	}

	total, err := self.Count(ctx, db, opts)
	if err != nil {
		return nil, err
	}

	rows, err := db.QueryContext(ctx, query, values...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	result := &SelectResult[VFreeProjects]{
		Total:    total,
		Selected: 0,
		Rows:     []VFreeProjects{},
	}

	for rows.Next() {
		var entity VFreeProjects
		if err := rows.Scan(&entity.Id, &entity.Name, &entity.Desc); err != nil {
			return nil, err
		}

		result.Rows = append(result.Rows, entity)
		result.Selected++
	}

	return result, nil
}
