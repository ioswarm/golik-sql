package sql

import (
	"fmt"

	"database/sql"
	"reflect"

	"github.com/ioswarm/golik"
)

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
		database: db,
		itype: itype,
		indexField: fld,
		behavior: behavior,
		schema: schema,
		table: table,
		builder: NewEntityBuilder(itype),
	}, nil
}


type sqlHandler struct {
	database *sql.DB
	itype      reflect.Type
	indexField string
	schema string
	table string
	builder EntityBuilder 
	behavior   interface{}
}

func (h *sqlHandler) Filter(ctx golik.CloveContext, flt *golik.Filter) (*golik.Result, error) {
	return nil, nil
}

func (h *sqlHandler) Create(ctx golik.CloveContext, cmd *golik.CreateCommand) error {
	return nil
}

func (h *sqlHandler) tablePath() string {
	if h.schema == "" {
		return h.table
	}
	return fmt.Sprintf("%v.%v", h.schema, h.table)
}

func (h *sqlHandler) buildSelectAll() string {
	return fmt.Sprintf("SELECT %v FROM %v", h.builder.ColumnQueryStr(), h.tablePath())
}

func (h *sqlHandler) Read(ctx golik.CloveContext, cmd *golik.GetCommand) (interface{}, error) {
	qry := fmt.Sprintf("%v WHERE %v = %v", h.buildSelectAll(), h.indexField, toSqlValue(cmd.Id))
	ctx.Info("Execute query: '%v'", qry)
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

func (h *sqlHandler) Update(ctx golik.CloveContext, cmd *golik.UpdateCommand) error {
	return nil
}

func (h *sqlHandler) Delete(ctx golik.CloveContext, cmd *golik.DeleteCommand) (interface{}, error) {
	return nil, nil
}

func (h *sqlHandler) OrElse(ctx golik.CloveContext, msg golik.Message) {
	if h.behavior != nil {
		ctx.AddOption("sql.database", h.database)
		ctx.AddOption("sql.schema", h.schema)
		ctx.AddOption("sql.table", h.table)
		golik.CallBehavior(ctx, msg, h.behavior)
	}
}