package sql

import (
	"fmt"

	"database/sql"
	"reflect"
	"strings"

	"github.com/ioswarm/golik"
)

var baseFilterQuery = `
select %s from (
  select * from (
	select 
	  row_number() over (order by a.%s) as line_num, 
	  a.* 
	from (
      %s
	) a
  ) x
  where x.line_num between %d and %d
) y
`

func defaultHandlerCreation(db *sql.DB, itype reflect.Type, indexField string, schema string, table string, behavior interface{}) golik.HandlerCreation {
	return func(ctx golik.CloveContext) (golik.Handler, error) {
		return NewSqlHandler(db, itype, indexField, schema, table, behavior)
	}
}

func NewSqlHandler(db *sql.DB, itype reflect.Type, indexField string, schema string, table string, behavior interface{}) (golik.Handler, error) {
	if db == nil {
		return nil, golik.Errorln("Database connection is nil")
	}
	if itype.Kind() != reflect.Struct {
		return nil, golik.Errorln("Given type must be a struct")
	}

	fld := indexField
	if fld == "" {
		if itype.NumField() == 0 {
			return nil, golik.Errorf("Given type has no fields")
		}
		ftype := itype.Field(0)
		fld = golik.CamelCase(ftype.Name)
	}

	return &sqlHandler{
		database:   db,
		itype:      itype,
		indexField: fld,
		behavior:   behavior,
		schema:     schema,
		table:      table,
		builder:    NewEntityBuilder(itype),
	}, nil
}

type sqlHandler struct {
	database   *sql.DB
	itype      reflect.Type
	indexField string
	schema     string
	table      string
	builder    EntityBuilder
	behavior   interface{}
}

func (h *sqlHandler) count(ctx golik.CloveContext, where string) int {
	qry := "SELECT count(*) as cnt from " + h.tablePath() + " " + where
	rows, err := h.database.Query(qry)
	if err != nil {
		ctx.Warn("Could not query count: %v", err)
		return 0
	}
	defer rows.Close()

	if rows.Next() {
		var result int
		if err := rows.Scan(&result); err != nil {
			ctx.Warn("Could not get count value: %v", err)
			return 0
		}
		return result
	}
	return 0
}

func (h *sqlHandler) Filter(ctx golik.CloveContext, flt *golik.Filter) (*golik.Result, error) {
	cond, err := flt.Condition()
	if err != nil {
		return nil, err
	}

	where, _ := NewFilter(cond)
	count := h.count(ctx, where)
	size := flt.Size
	if size == 0 {
		size = 10
	}
	to := flt.From + size
	filterQry := fmt.Sprintln(h.buildSelectAll(), where)
	qry := fmt.Sprintf(baseFilterQuery, h.builder.ColumnQueryStr(), h.indexField, filterQry, flt.From+1, to)
	ctx.Debug("Execute query: %v", qry)

	rows, err := h.database.Query(qry)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	result := make([]interface{}, 0)

	vals := h.builder.ScanList()
	for rows.Next() {
		if err := rows.Scan(vals...); err != nil {
			return nil, err
		}
		ptrvale := reflect.New(h.itype)
		res := ptrvale.Interface()

		if err := h.builder.Read(vals, res); err != nil {
			return nil, err
		}

		result = append(result, res)
	}

	return &golik.Result{
		From:   flt.From,
		Size:   len(result),
		Count:  count,
		Result: result,
	}, nil
}

func (h *sqlHandler) tablePath() string {
	if h.schema == "" {
		return h.table
	}
	return fmt.Sprintf("%v.%v", h.schema, h.table)
}

func (h *sqlHandler) buildInsert() string {
	fields := h.builder.SqlNames()
	result := make([]string, len(fields))
	for i := range fields {
		result[i] = "?"
	}

	return fmt.Sprintf("INSERT INTO %v (%v) VALUES (%v)", h.tablePath(), strings.Join(fields, ", "), strings.Join(result, ", "))
}

func (h *sqlHandler) Create(ctx golik.CloveContext, cmd *golik.CreateCommand) error {
	tx, err := h.database.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	ddl := h.buildInsert()
	stmt, err := tx.Prepare(ddl)
	ctx.Debug("PrepareStatement: %v", ddl)
	if err != nil {
		return err
	}
	defer stmt.Close()

	_, err = stmt.Exec(h.builder.Values(cmd.Entity)...)
	if err != nil {
		return err
	}

	if err := tx.Commit(); err != nil {
		return err
	}

	return nil
}

func (h *sqlHandler) buildSelectAll() string {
	return fmt.Sprintf("SELECT %v FROM %v", h.builder.ColumnQueryStr(), h.tablePath())
}

func (h *sqlHandler) Read(ctx golik.CloveContext, cmd *golik.GetCommand) (interface{}, error) {
	qry := fmt.Sprintf("%v WHERE %v = %v", h.buildSelectAll(), h.indexField, toSqlValue(cmd.Id))
	ctx.Debug("Execute query: '%v'", qry)
	rows, err := h.database.Query(qry)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	vals := h.builder.ScanList()
	if rows.Next() {
		if err := rows.Scan(vals...); err != nil {
			return nil, err
		}
		ptrvale := reflect.New(h.itype)
		result := ptrvale.Interface()

		if err := h.builder.Read(vals, result); err != nil {
			return nil, err
		}

		return result, nil
	}

	return nil, fmt.Errorf("Could not find entity with id %v", cmd.Id) // TODO define default errors
}

func (h *sqlHandler) buildUpdate() string {
	fields := h.builder.SqlNames(h.indexField)
	result := make([]string, len(fields))
	for i, f := range fields {
		result[i] = f + " = ?"
	}

	return fmt.Sprintf("UPDATE %v SET %v WHERE %v = ?", h.tablePath(), strings.Join(result, ", "), h.indexField)
}

func (h *sqlHandler) Update(ctx golik.CloveContext, cmd *golik.UpdateCommand) error {
	if _, err := h.Read(ctx, golik.Get(cmd.Id)); err != nil {
		return err
	}

	tx, err := h.database.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	ddl := h.buildUpdate()
	stmt, err := tx.Prepare(ddl)
	ctx.Debug("PrepareStatement: %v", ddl)
	if err != nil {
		return err
	}
	defer stmt.Close()

	vals := append(h.builder.Values(cmd.Entity, h.indexField), cmd.Id)
	_, err = stmt.Exec(vals...)
	if err != nil {
		return err
	}

	if err := tx.Commit(); err != nil {
		return err
	}

	return nil
}

func (h *sqlHandler) buildDelete() string {
	return fmt.Sprintf("DELETE FROM %v WHERE %v = ?", h.tablePath(), h.indexField)
}

func (h *sqlHandler) Delete(ctx golik.CloveContext, cmd *golik.DeleteCommand) (interface{}, error) {
	entity, err := h.Read(ctx, golik.Get(cmd.Id))
	if err != nil {
		return nil, err
	}

	tx, err := h.database.Begin()
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	ddl := h.buildDelete()
	stmt, err := tx.Prepare(ddl)
	ctx.Debug("PrepareStatement: %v", ddl)
	if err != nil {
		return nil, err
	}
	defer stmt.Close()

	_, err = stmt.Exec(cmd.Id)
	if err != nil {
		return nil, err
	}

	if err := tx.Commit(); err != nil {
		return nil, err
	}

	return entity, nil
}

func (h *sqlHandler) OrElse(ctx golik.CloveContext, msg golik.Message) {
	if h.behavior != nil {
		ctx.AddOption("sql.database", h.database)
		ctx.AddOption("sql.schema", h.schema)
		ctx.AddOption("sql.table", h.table)
		golik.CallBehavior(ctx, msg, h.behavior)
	}
}
