package sql

import (
	"errors"
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
	Fields() []Field
	ScanList() []interface{}

	Read([]interface{}, interface{}) error

	ColumnQueryStr()string
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
	etype  reflect.Type
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

func (eb *entityBuilder) Fields() []Field {
	result := make([]Field, 0)

	for i := 0; i < eb.etype.NumField(); i++ {
		field := eb.etype.Field(i)
		name := []rune(field.Name)
		if name[0] >= 65 && name[0] <= 90 {
			if rule, ok := eb.findRuleMatch(field.Type); ok {
				f := NewField(field, rule)
				result = append(result, f)
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

func (eb *entityBuilder) ColumnQueryStr() string {
	fields := eb.Fields()
	names := make([]string, len(fields))
	for i, fld := range fields {
		names[i] = fld.SQLName()
	}

	return strings.Join(names, ", ")
}