package sql

import (
	"database/sql"
	"fmt"
	"reflect"
	"strings"
	"sync"

	"github.com/ioswarm/golik"
)

func Sql(name string, system golik.Golik) (*SqlService, error) {
	return NewSqlService(name, newSettings(name), system)
}

func NewSql(name string, schema string, driver string, connection string, system golik.Golik) (*SqlService, error) {
	settings := newSettings(name)
	settings.Driver = driver
	settings.Connection = connection
	settings.Schema = schema
	return NewSqlService(name, settings, system)
}

func NewSqlService(name string, settings *Settings, system golik.Golik) (*SqlService, error) {
	sqls := &SqlService{
		name:     name,
		system:   system,
		settings: settings,
	}

	hdl, err := system.ExecuteService(sqls)
	if err != nil {
		return nil, err
	}

	sqls.mutex.Lock()
	defer sqls.mutex.Unlock()
	sqls.handler = hdl

	return sqls, nil
}

type SqlService struct {
	name     string
	system   golik.Golik
	handler  golik.CloveHandler
	settings *Settings
	database *sql.DB

	mutex sync.Mutex
}

func (sqls *SqlService) CreateServiceInstance(system golik.Golik) *golik.Clove {
	return &golik.Clove{
		Name: sqls.name,
		Behavior: func(ctx golik.CloveContext, msg golik.Message) {
			msg.Reply(golik.Done())
		},
		PreStart: sqls.connect,
		PostStop: sqls.close,
	}
}

func (sqls *SqlService) connect(ctx golik.CloveContext) error {
	ctx.Info("Connect to sql-database %v via %v", sqls.Connection(), sqls.Driver())
	con, err := sql.Open(sqls.Driver(), sqls.Connection())
	if err != nil {
		ctx.Error("Could not create sql-connection via %v to %v: %v", sqls.Driver(), sqls.Connection(), err)
		return golik.Errorf("Could not create sql-connection via %v to %v: %v", sqls.Driver(), sqls.Connection(), err)
	}

	if err := con.Ping(); err != nil {
		ctx.Error("Could not connect via %v to %v: %v", sqls.Driver(), sqls.Connection(), err)
		return golik.Errorf("Could not connect via %v to %v: %v", sqls.Driver(), sqls.Connection(), err)
	}

	con.SetMaxOpenConns(sqls.settings.MaxOpenConnections)
	con.SetMaxIdleConns(sqls.settings.MaxIdleConnections)
	con.SetConnMaxLifetime(sqls.settings.ConnectionLifeTime)

	sqls.mutex.Lock()
	defer sqls.mutex.Unlock()
	sqls.database = con

	return nil
}

func (sqls *SqlService) close(ctx golik.CloveContext) error {
	if sqls.database == nil {
		return nil
	}
	if err := sqls.database.Close(); err != nil {
		return golik.Errorf("Could not disconnect via %v to %v: %v", sqls.Driver(), sqls.Connection(), err)
	}
	ctx.Info("Disconnected from %v via %v", sqls.Connection(), sqls.Driver())
	return nil
}

func (sqls *SqlService) Name() string {
	return sqls.name
}

func (sqls *SqlService) Driver() string {
	return sqls.settings.Driver
}

func (sqls *SqlService) Connection() string {
	return sqls.settings.Connection
}

func (sqls *SqlService) Database() *sql.DB {
	return sqls.database
}

func (sqls *SqlService) Schema() string {
	return sqls.settings.Schema
}

func (sqls *SqlService) CreateConnectionPool(settings *golik.ConnectionPoolSettings) (golik.CloveRef, error) {
	if settings.Type.Kind() != reflect.Struct {
		return nil, golik.Errorln("Given type must be a struct")
	}

	if settings.Options == nil {
		settings.Options = make(map[string]interface{})
	}
	if _, ok := settings.Options["sql.driver"]; !ok {
		settings.Options["sql.driver"] = sqls.Driver()
	}
	if _, ok := settings.Options["sql.connection"]; !ok {
		settings.Options["sql.connection"] = sqls.Connection()
	}
	if _, ok := settings.Options["sql.database"]; !ok {
		settings.Options["sql.database"] = sqls.Database()
	}
	if _, ok := settings.Options["sql.schema"]; !ok {
		settings.Options["sql.schema"] = sqls.Schema()
	}

	tbl := strings.ToUpper(settings.Type.Name())
	if v, ok := settings.Options["sql.table"]; ok {
		tbl = fmt.Sprintln(v)
	}

	sch := ""
	if v, ok := settings.Options["sql.schema"]; ok {
		sch = fmt.Sprintln(v)
	}

	if settings.CreateHandler == nil {
		settings.CreateHandler = defaultHandlerCreation(sqls.Database(), settings.Type, settings.IndexField, sch, tbl, settings.Behavior)
	}

	clove := golik.NewConnectionPool(settings)
	return sqls.handler.Execute(clove)
}
