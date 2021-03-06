package sql

import (
	"fmt"
	"reflect"
	"time"

	"database/sql"
)

var (
	timetype = reflect.TypeOf(time.Time{})
)

func NewStringRule() FieldConversionRule {
	return &stringRule{}
}

type stringRule struct{}

func (r *stringRule) ValuePointer() interface{} {
	return new(sql.NullString)
}

func (r *stringRule) ConvertValue(v interface{}) (reflect.Value, error) {
	if v == nil {
		return reflect.ValueOf(""), nil
	}
	if val, ok := v.(*sql.NullString); ok {
		if val.Valid {
			return reflect.ValueOf(val.String), nil
		}
		return reflect.ValueOf(""), nil
	}

	return reflect.ValueOf(nil), fmt.Errorf("Could not convert %T as string", v)
}

func (r *stringRule) CanConvert(ftype reflect.Type) bool {
	return ftype.Kind() == reflect.String
}

func NewTimeRule() FieldConversionRule {
	return &timeRule{}
}

type timeRule struct {
	pointer bool
}

func (r *timeRule) ValuePointer() interface{} {
	return new(sql.NullTime)
}

func (r *timeRule) ConvertValue(v interface{}) (reflect.Value, error) {
	if v == nil {
		if r.pointer {
			return reflect.New(timetype), nil
		}
		return reflect.ValueOf(time.Time{}), nil
	}
	if val, ok := v.(*sql.NullTime); ok {
		if val.Valid {
			if r.pointer {
				return reflect.ValueOf(&val.Time), nil
			}
			return reflect.ValueOf(val.Time), nil
		}

		if r.pointer {
			return reflect.New(timetype), nil
		}
		return reflect.ValueOf(time.Time{}), nil
	}
	return reflect.ValueOf(nil), fmt.Errorf("Could not convert %T as int", v)
}

func (r *timeRule) CanConvert(ftype reflect.Type) bool {
	if ftype.Kind() == reflect.Ptr && ftype.Elem() == timetype {
		r.pointer = true
		return true
	}
	return ftype == timetype
}

func NewIntRule() FieldConversionRule {
	return &intRule{}
}

type intRule struct{}

func (r *intRule) ValuePointer() interface{} {
	return new(sql.NullInt32)
}

func (r *intRule) ConvertValue(v interface{}) (reflect.Value, error) {
	if v == nil {
		return reflect.ValueOf(0), nil
	}
	if val, ok := v.(*sql.NullInt32); ok {
		if val.Valid {
			return reflect.ValueOf(int(val.Int32)), nil
		}
		return reflect.ValueOf(0), nil
	}
	return reflect.ValueOf(nil), fmt.Errorf("Could not convert %T as int", v)
}

func (r *intRule) CanConvert(ftype reflect.Type) bool {
	return ftype.Kind() == reflect.Int
}

func NewInt8Rule() FieldConversionRule {
	return &int8Rule{}
}

type int8Rule struct{}

func (r *int8Rule) ValuePointer() interface{} {
	return new(sql.NullInt32)
}

func (r *int8Rule) ConvertValue(v interface{}) (reflect.Value, error) {
	if v == nil {
		return reflect.ValueOf(int8(0)), nil
	}
	if val, ok := v.(*sql.NullInt32); ok {
		if val.Valid {
			return reflect.ValueOf(int8(val.Int32)), nil
		}
		return reflect.ValueOf(int8(0)), nil
	}
	return reflect.ValueOf(nil), fmt.Errorf("Could not convert %T as int", v)
}

func (r *int8Rule) CanConvert(ftype reflect.Type) bool {
	return ftype.Kind() == reflect.Int8
}

func NewInt16Rule() FieldConversionRule {
	return &int16Rule{}
}

type int16Rule struct{}

func (r *int16Rule) ValuePointer() interface{} {
	return new(sql.NullInt32)
}

func (r *int16Rule) ConvertValue(v interface{}) (reflect.Value, error) {
	if v == nil {
		return reflect.ValueOf(int16(0)), nil
	}
	if val, ok := v.(*sql.NullInt32); ok {
		if val.Valid {
			return reflect.ValueOf(int16(val.Int32)), nil
		}
		return reflect.ValueOf(int16(0)), nil
	}
	return reflect.ValueOf(nil), fmt.Errorf("Could not convert %T as int", v)
}

func (r *int16Rule) CanConvert(ftype reflect.Type) bool {
	return ftype.Kind() == reflect.Int16
}

func NewInt32Rule() FieldConversionRule {
	return &int32Rule{}
}

type int32Rule struct{}

func (r *int32Rule) ValuePointer() interface{} {
	return new(sql.NullInt32)
}

func (r *int32Rule) ConvertValue(v interface{}) (reflect.Value, error) {
	if v == nil {
		return reflect.ValueOf(int32(0)), nil
	}
	if val, ok := v.(*sql.NullInt32); ok {
		if val.Valid {
			return reflect.ValueOf(val.Int32), nil
		}
		return reflect.ValueOf(int32(0)), nil
	}
	return reflect.ValueOf(nil), fmt.Errorf("Could not convert %T as int", v)
}

func (r *int32Rule) CanConvert(ftype reflect.Type) bool {
	return ftype.Kind() == reflect.Int32
}

func NewInt64Rule() FieldConversionRule {
	return &int64Rule{}
}

type int64Rule struct{}

func (r *int64Rule) ValuePointer() interface{} {
	return new(sql.NullInt64)
}

func (r *int64Rule) ConvertValue(v interface{}) (reflect.Value, error) {
	if v == nil {
		return reflect.ValueOf(int64(0)), nil
	}
	if val, ok := v.(*sql.NullInt64); ok {
		if val.Valid {
			return reflect.ValueOf(val.Int64), nil
		}
		return reflect.ValueOf(int64(0)), nil
	}
	return reflect.ValueOf(nil), fmt.Errorf("Could not convert %T as int", v)
}

func (r *int64Rule) CanConvert(ftype reflect.Type) bool {
	return ftype.Kind() == reflect.Int64
}

func NewUintRule() FieldConversionRule {
	return &uintRule{}
}

type uintRule struct{}

func (r *uintRule) ValuePointer() interface{} {
	return new(sql.NullInt32)
}

func (r *uintRule) ConvertValue(v interface{}) (reflect.Value, error) {
	if v == nil {
		return reflect.ValueOf(uint(0)), nil
	}
	if val, ok := v.(*sql.NullInt32); ok {
		if val.Valid {
			return reflect.ValueOf(uint(val.Int32)), nil
		}
		return reflect.ValueOf(uint(0)), nil
	}
	return reflect.ValueOf(nil), fmt.Errorf("Could not convert %T as int", v)
}

func (r *uintRule) CanConvert(ftype reflect.Type) bool {
	return ftype.Kind() == reflect.Uint
}

func NewUint8Rule() FieldConversionRule {
	return &uint8Rule{}
}

type uint8Rule struct{}

func (r *uint8Rule) ValuePointer() interface{} {
	return new(sql.NullInt32)
}

func (r *uint8Rule) ConvertValue(v interface{}) (reflect.Value, error) {
	if v == nil {
		return reflect.ValueOf(uint8(0)), nil
	}
	if val, ok := v.(*sql.NullInt32); ok {
		if val.Valid {
			return reflect.ValueOf(uint8(val.Int32)), nil
		}
		return reflect.ValueOf(uint8(0)), nil
	}
	return reflect.ValueOf(nil), fmt.Errorf("Could not convert %T as int", v)
}

func (r *uint8Rule) CanConvert(ftype reflect.Type) bool {
	return ftype.Kind() == reflect.Uint8
}

func NewUint16Rule() FieldConversionRule {
	return &uint16Rule{}
}

type uint16Rule struct{}

func (r *uint16Rule) ValuePointer() interface{} {
	return new(sql.NullInt32)
}

func (r *uint16Rule) ConvertValue(v interface{}) (reflect.Value, error) {
	if v == nil {
		return reflect.ValueOf(uint16(0)), nil
	}
	if val, ok := v.(*sql.NullInt32); ok {
		if val.Valid {
			return reflect.ValueOf(uint16(val.Int32)), nil
		}
		return reflect.ValueOf(uint16(0)), nil
	}
	return reflect.ValueOf(nil), fmt.Errorf("Could not convert %T as int", v)
}

func (r *uint16Rule) CanConvert(ftype reflect.Type) bool {
	return ftype.Kind() == reflect.Uint16
}

func NewUint32Rule() FieldConversionRule {
	return &uint32Rule{}
}

type uint32Rule struct{}

func (r *uint32Rule) ValuePointer() interface{} {
	return new(sql.NullInt32)
}

func (r *uint32Rule) ConvertValue(v interface{}) (reflect.Value, error) {
	if v == nil {
		return reflect.ValueOf(uint32(0)), nil
	}
	if val, ok := v.(*sql.NullInt32); ok {
		if val.Valid {
			return reflect.ValueOf(uint32(val.Int32)), nil
		}
		return reflect.ValueOf(uint32(0)), nil
	}
	return reflect.ValueOf(nil), fmt.Errorf("Could not convert %T as int", v)
}

func (r *uint32Rule) CanConvert(ftype reflect.Type) bool {
	return ftype.Kind() == reflect.Uint32
}

func NewUint64Rule() FieldConversionRule {
	return &uint64Rule{}
}

type uint64Rule struct{}

func (r *uint64Rule) ValuePointer() interface{} {
	return new(sql.NullInt64)
}

func (r *uint64Rule) ConvertValue(v interface{}) (reflect.Value, error) {
	if v == nil {
		return reflect.ValueOf(uint64(0)), nil
	}
	if val, ok := v.(*sql.NullInt64); ok {
		if val.Valid {
			return reflect.ValueOf(uint64(val.Int64)), nil
		}
		return reflect.ValueOf(uint64(0)), nil
	}
	return reflect.ValueOf(nil), fmt.Errorf("Could not convert %T as int", v)
}

func (r *uint64Rule) CanConvert(ftype reflect.Type) bool {
	return ftype.Kind() == reflect.Uint64
}

func NewFloat32Rule() FieldConversionRule {
	return &float32Rule{}
}

type float32Rule struct{}

func (r *float32Rule) ValuePointer() interface{} {
	return new(sql.NullFloat64)
}

func (r *float32Rule) ConvertValue(v interface{}) (reflect.Value, error) {
	if v == nil {
		return reflect.ValueOf(float32(0.0)), nil
	}
	if val, ok := v.(*sql.NullFloat64); ok {
		if val.Valid {
			return reflect.ValueOf(float32(val.Float64)), nil
		}
		return reflect.ValueOf(float32(0.0)), nil
	}
	return reflect.ValueOf(nil), fmt.Errorf("Could not convert %T as int", v)
}

func (r *float32Rule) CanConvert(ftype reflect.Type) bool {
	return ftype.Kind() == reflect.Float32
}

func NewFloat64Rule() FieldConversionRule {
	return &float64Rule{}
}

type float64Rule struct{}

func (r *float64Rule) ValuePointer() interface{} {
	return new(sql.NullFloat64)
}

func (r *float64Rule) ConvertValue(v interface{}) (reflect.Value, error) {
	if v == nil {
		return reflect.ValueOf(float64(0.0)), nil
	}
	if val, ok := v.(*sql.NullFloat64); ok {
		if val.Valid {
			return reflect.ValueOf(val.Float64), nil
		}
		return reflect.ValueOf(float64(0)), nil
	}
	return reflect.ValueOf(nil), fmt.Errorf("Could not convert %T as int", v)
}

func (r *float64Rule) CanConvert(ftype reflect.Type) bool {
	return ftype.Kind() == reflect.Float64
}

func NewBoolRule() FieldConversionRule {
	return &boolRule{}
}

type boolRule struct{}

func (r *boolRule) ValuePointer() interface{} {
	return new(sql.NullBool)
}

func (r *boolRule) ConvertValue(v interface{}) (reflect.Value, error) {
	if v == nil {
		return reflect.ValueOf(false), nil
	}
	if val, ok := v.(*sql.NullBool); ok {
		if val.Valid {
			return reflect.ValueOf(val.Bool), nil
		}
		return reflect.ValueOf(false), nil
	}
	return reflect.ValueOf(nil), fmt.Errorf("Could not convert %T as int", v)
}

func (r *boolRule) CanConvert(ftype reflect.Type) bool {
	return ftype.Kind() == reflect.Bool
}

func NewBinaryRule() FieldConversionRule {
	return &binaryRule{}
}

type binaryRule struct{}

func (r *binaryRule) ValuePointer() interface{} {
	return new([]byte)
}

func (r *binaryRule) ConvertValue(v interface{}) (reflect.Value, error) {
	if v == nil {
		return reflect.ValueOf([]byte{}), nil
	}
	if val, ok := v.(*[]byte); ok {
		return reflect.ValueOf(*val), nil
	}
	return reflect.ValueOf(nil), fmt.Errorf("Could not convert %T as int", v)
}

func (r *binaryRule) CanConvert(ftype reflect.Type) bool {
	return ftype.Kind() == reflect.Slice && ftype.Elem().Kind() == reflect.Uint8
}
