package sql

import (
	"errors"
	"fmt"
	"reflect"
	"strings"
)

type FieldConversionRule interface {
	ValuePointer() interface{}
	ConvertValue(interface{}) (reflect.Value, error)
	CanConvert(ftype reflect.Type) bool
}

type Field interface {
	Name() string
	SQLName() string
	Field() reflect.StructField
	ConversionRule() FieldConversionRule
}

func NewField(field reflect.StructField, rule FieldConversionRule) Field {
	return &fieldDef{
		field: field,
		rule:  rule,
	}
}

type fieldDef struct {
	field reflect.StructField
	rule  FieldConversionRule
}

func (f *fieldDef) Name() string {
	return f.field.Name
}

func (f *fieldDef) SQLName() string {
	// TODO interpret tags
	return strings.ToUpper(f.Name())
}

func (f *fieldDef) Field() reflect.StructField {
	return f.field
}

func (f *fieldDef) ConversionRule() FieldConversionRule {
	return f.rule
}

type EntityBuilder interface {
	Rules() []FieldConversionRule
	AddRule(...FieldConversionRule) EntityBuilder
	Fields(...string) []Field
	ScanList() []interface{}

	Read([]interface{}, interface{}) error

	ValueOf(interface{}, string) (interface{}, error)
	Values(interface{}, ...string) []interface{}

	SqlNames(...string) []string
	ColumnQueryStr() string
}

func NewEntityBuilder(etype reflect.Type) EntityBuilder {
	return &entityBuilder{
		etype: etype,
		rules: []FieldConversionRule{
			NewStringRule(),
			NewTimeRule(),
			NewIntRule(),
			NewInt8Rule(),
			NewInt16Rule(),
			NewInt32Rule(),
			NewInt64Rule(),
			NewUintRule(),
			NewUint8Rule(),
			NewUint16Rule(),
			NewUint32Rule(),
			NewUint64Rule(),
			NewFloat32Rule(),
			NewFloat64Rule(),
			NewBoolRule(),
			NewBinaryRule(),
		},
	}
}

type entityBuilder struct {
	etype reflect.Type
	rules []FieldConversionRule
}

func (eb *entityBuilder) Rules() []FieldConversionRule {
	return eb.rules
}

func (eb *entityBuilder) AddRule(rule ...FieldConversionRule) EntityBuilder {
	eb.rules = append(rule, eb.rules...)
	return eb
}

func (eb *entityBuilder) findRuleMatch(ftype reflect.Type) (FieldConversionRule, bool) {
	for _, rule := range eb.rules {
		if rule.CanConvert(ftype) {
			return rule, true
		}
	}
	return nil, false
}

func (eb *entityBuilder) Fields(expects ...string) []Field {
	result := make([]Field, 0)
	contains := func(name string) bool {
		for _, e := range expects {
			if strings.ToUpper(e) == strings.ToUpper(name) {
				return true
			}
		}
		return false
	}

	for i := 0; i < eb.etype.NumField(); i++ {
		field := eb.etype.Field(i)
		name := []rune(field.Name)
		if name[0] >= 65 && name[0] <= 90 {
			if rule, ok := eb.findRuleMatch(field.Type); ok {
				f := NewField(field, rule)
				if !contains(f.SQLName()) {
					result = append(result, f)
				}
			}
		}
	}

	return result
}

func (eb *entityBuilder) ScanList() []interface{} {
	fields := eb.Fields()
	result := make([]interface{}, len(fields))

	for i, fld := range fields {
		result[i] = fld.ConversionRule().ValuePointer()
	}

	return result
}

func (eb *entityBuilder) Read(values []interface{}, i interface{}) error {
	fields := eb.Fields()
	if len(values) != len(fields) {
		return errors.New("Values differs from fields")
	}

	itype := reflect.TypeOf(i)
	if itype.Kind() != reflect.Ptr || itype.Elem().Kind() != reflect.Struct {
		return errors.New("Given value parameter must be pointer of struct")
	}
	ivalue := reflect.ValueOf(i)
	elemvalue := ivalue.Elem()

	for i, fld := range fields {
		fldvalue := elemvalue.FieldByName(fld.Name())
		val := values[i]
		vvalue, err := fld.ConversionRule().ConvertValue(val)
		if err != nil {
			return err
		}
		fldvalue.Set(vvalue)
	}

	return nil
}

func (eb *entityBuilder) ValueOf(i interface{}, name string) (interface{}, error) {
	fields := eb.Fields()
	ivalue := reflect.ValueOf(i)
	elemvalue := ivalue.Elem()

	for _, fld := range fields {
		if strings.ToUpper(fld.Name()) == strings.ToUpper(name) {
			fldvalue := elemvalue.FieldByName(fld.Name())
			return fldvalue.Interface(), nil
		}
	}
	return nil, fmt.Errorf("Unknown field %v", name)
}

func (eb *entityBuilder) Values(i interface{}, expects ...string) []interface{} {
	fields := eb.Fields(expects...)
	result := make([]interface{}, len(fields))

	ivalue := reflect.ValueOf(i)
	elemvalue := ivalue.Elem()

	for i, fld := range fields {
		fldvalue := elemvalue.FieldByName(fld.Name())
		result[i] = fldvalue.Interface()
	}

	return result
}

func (eb *entityBuilder) SqlNames(expects ...string) []string {
	fields := eb.Fields(expects...)
	names := make([]string, len(fields))
	for i, fld := range fields {
		names[i] = fld.SQLName()
	}

	return names
}

func (eb *entityBuilder) ColumnQueryStr() string {
	return strings.Join(eb.SqlNames(), ", ")
}
