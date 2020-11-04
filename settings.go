package sql

import (
	"fmt"

	"github.com/spf13/viper"
)

type Settings struct {
	Poolsize   int
	Connection string
	Driver string
	Schema string
}

func newBaseSettings() *Settings {
	return &Settings{
		Poolsize: viper.GetInt("sql.poolsize"),
		Connection: viper.GetString("sql.connection"),
		Driver: viper.GetString("sql.driver"),
		Schema: viper.GetString("sql.schema"),
	}
}

func newSettings(name string) *Settings {
	bs := newBaseSettings()

	getPath := func(segment string) string {
		return fmt.Sprintf("sql.%v.%v", name, segment)
	}

	path := getPath("poolsize")
	if viper.IsSet(path) {
		bs.Poolsize = viper.GetInt(path)
	}

	path = getPath("connection")
	if viper.IsSet(path) {
		bs.Connection = viper.GetString(path)
	}

	path = getPath("driver")
	if viper.IsSet(path) {
		bs.Driver = viper.GetString(path)
	}

	path = getPath("schema")
	if viper.IsSet(path) {
		bs.Schema = viper.GetString(path)
	}

	return bs
}

func init() {
	viper.SetDefault("sql.poolsize", 10)
}
