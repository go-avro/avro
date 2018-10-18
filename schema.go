package avro

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"math"
	"reflect"
	"strings"
)

// ***********************
// NOTICE this file was changed beginning in November 2016 by the team maintaining
// https://github.com/go-avro/avro. This notice is required to be here due to the
// terms of the Apache license, see LICENSE for details.
// ***********************

const (
	// Record schema type constant
	Record int = iota

	// Enum schema type constant
	Enum

	// Array schema type constant
	Array

	// Map schema type constant
	Map

	// Union schema type constant
	Union

	// Fixed schema type constant
	Fixed

	// String schema type constant
	String

	// Bytes schema type constant
	Bytes

	// Int schema type constant
	Int

	// Long schema type constant
	Long

	// Float schema type constant
	Float

	// Double schema type constant
	Double

	// Boolean schema type constant
	Boolean

	// Null schema type constant
	Null

	// Recursive schema type constant. Recursive is an artificial type that means a Record schema without its definition
	// that should be looked up in some registry.
	Recursive
)

const (
	typeRecord  = "record"
	typeUnion   = "union"
	typeEnum    = "enum"
	typeArray   = "array"
	typeMap     = "map"
	typeFixed   = "fixed"
	typeString  = "string"
	typeBytes   = "bytes"
	typeInt     = "int"
	typeLong    = "long"
	typeFloat   = "float"
	typeDouble  = "double"
	typeBoolean = "boolean"
	typeNull    = "null"
)

const (
	schemaAliasesField   = "aliases"
	schemaDefaultField   = "default"
	schemaDocField       = "doc"
	schemaFieldsField    = "fields"
	schemaItemsField     = "items"
	schemaNameField      = "name"
	schemaNamespaceField = "namespace"
	schemaSizeField      = "size"
	schemaSymbolsField   = "symbols"
	schemaTypeField      = "type"
	schemaValuesField    = "values"
)

// Schema is an interface representing a single Avro schema (both primitive and complex).
type Schema interface {
	// Returns an integer constant representing this schema type.
	Type() int

	// If this is a record, enum or fixed, returns its name, otherwise the name of the primitive type.
	GetName() string

	// Gets a custom non-reserved property from this schema and a bool representing if it exists.
	Prop(key string) (interface{}, bool)

	// Converts this schema to its JSON representation.
	String() string

	// Checks whether the given value is writeable to this schema.
	Validate(v reflect.Value) bool

	// Project this (writer) schema onto a different reader schema using avro schema resolution rules
	Project(value interface{}, into Schema) (interface{}, error)
}

// StringSchema implements Schema and represents Avro string type.
type StringSchema struct{}

// Returns a JSON representation of StringSchema.
func (*StringSchema) String() string {
	return `{"type": "string"}`
}

// Type returns a type constant for this StringSchema.
func (*StringSchema) Type() int {
	return String
}

// GetName returns a type name for this StringSchema.
func (*StringSchema) GetName() string {
	return typeString
}

// Prop doesn't return anything valuable for StringSchema.
func (*StringSchema) Prop(key string) (interface{}, bool) {
	return nil, false
}

// Validate checks whether the given value is writeable to this schema.
func (*StringSchema) Validate(v reflect.Value) bool {
	_, isString := dereference(v).Interface().(string)
	_, isBytes := dereference(v).Interface().([]byte)
	return isString || isBytes
}

// MarshalJSON serializes the given schema as JSON. Never returns an error.
func (*StringSchema) MarshalJSON() ([]byte, error) {
	return []byte(`"string"`), nil
}

// Project this (writer) schema onto a different reader schema using avro schema resolution rules
func (*StringSchema) Project(value interface{}, into Schema) (interface{}, error) {
	switch into.Type() {
	case String:
		return value, nil
	case Bytes:
		return []byte(value.(string)), nil
	default:
		return nil, errors.New("Could not project String value into reader schema: " + into.String())
	}
}

// BytesSchema implements Schema and represents Avro bytes type.
type BytesSchema struct{}

// String returns a JSON representation of BytesSchema.
func (*BytesSchema) String() string {
	return `{"type": "bytes"}`
}

// Type returns a type constant for this BytesSchema.
func (*BytesSchema) Type() int {
	return Bytes
}

// GetName returns a type name for this BytesSchema.
func (*BytesSchema) GetName() string {
	return typeBytes
}

// Prop doesn't return anything valuable for BytesSchema.
func (*BytesSchema) Prop(key string) (interface{}, bool) {
	return nil, false
}

// Validate checks whether the given value is writeable to this schema.
func (*BytesSchema) Validate(v reflect.Value) bool {
	v = dereference(v)

	return v.Kind() == reflect.Slice && v.Type().Elem().Kind() == reflect.Uint8
}

// MarshalJSON serializes the given schema as JSON. Never returns an error.
func (*BytesSchema) MarshalJSON() ([]byte, error) {
	return []byte(`"bytes"`), nil
}

// Project this (writer) schema onto a different reader schema using avro schema resolution rules
func (*BytesSchema) Project(value interface{}, into Schema) (interface{}, error) {
	switch into.Type() {
	case Bytes:
		return value, nil
	case String:
		return string(value.([]byte)), nil
	default:
		return nil, errors.New("Could not project Bytes value into reader schema: " + into.String())
	}
}

// IntSchema implements Schema and represents Avro int type.
type IntSchema struct{}

// String returns a JSON representation of IntSchema.
func (*IntSchema) String() string {
	return `{"type": "int"}`
}

// Type returns a type constant for this IntSchema.
func (*IntSchema) Type() int {
	return Int
}

// GetName returns a type name for this IntSchema.
func (*IntSchema) GetName() string {
	return typeInt
}

// Prop doesn't return anything valuable for IntSchema.
func (*IntSchema) Prop(key string) (interface{}, bool) {
	return nil, false
}

// Validate checks whether the given value is writeable to this schema.
func (*IntSchema) Validate(v reflect.Value) bool {
	return reflect.TypeOf(dereference(v).Interface()).Kind() == reflect.Int32
}

// MarshalJSON serializes the given schema as JSON. Never returns an error.
func (*IntSchema) MarshalJSON() ([]byte, error) {
	return []byte(`"int"`), nil
}

// Project this (writer) schema onto a different reader schema using avro schema resolution rules
func (*IntSchema) Project(value interface{}, into Schema) (interface{}, error) {
	switch into.Type() {
	case Int:
		return value, nil
	case Long:
		return int64(value.(int32)), nil
	case Float:
		return float32(value.(int32)), nil
	case Double:
		return float64(value.(int32)), nil
	default:
		return nil, errors.New("Could not project Int value into reader schema: " + into.String())
	}
}

// LongSchema implements Schema and represents Avro long type.
type LongSchema struct{}

// Returns a JSON representation of LongSchema.
func (*LongSchema) String() string {
	return `{"type": "long"}`
}

// Type returns a type constant for this LongSchema.
func (*LongSchema) Type() int {
	return Long
}

// GetName returns a type name for this LongSchema.
func (*LongSchema) GetName() string {
	return typeLong
}

// Prop doesn't return anything valuable for LongSchema.
func (*LongSchema) Prop(key string) (interface{}, bool) {
	return nil, false
}

// Validate checks whether the given value is writeable to this schema.
func (*LongSchema) Validate(v reflect.Value) bool {
	return reflect.TypeOf(dereference(v).Interface()).Kind() == reflect.Int64
}

// MarshalJSON serializes the given schema as JSON. Never returns an error.
func (*LongSchema) MarshalJSON() ([]byte, error) {
	return []byte(`"long"`), nil
}

// Project this (writer) schema onto a different reader schema using avro schema resolution rules
func (*LongSchema) Project(value interface{}, into Schema) (interface{}, error) {
	switch into.Type() {
	case Long:
		return value, nil
	case Float:
		return float32(value.(int64)), nil
	case Double:
		return float64(value.(int64)), nil
	default:
		return nil, errors.New("Could not project Long value into reader schema: " + into.String())
	}
}

// FloatSchema implements Schema and represents Avro float type.
type FloatSchema struct{}

// String returns a JSON representation of FloatSchema.
func (*FloatSchema) String() string {
	return `{"type": "float"}`
}

// Type returns a type constant for this FloatSchema.
func (*FloatSchema) Type() int {
	return Float
}

// GetName returns a type name for this FloatSchema.
func (*FloatSchema) GetName() string {
	return typeFloat
}

// Prop doesn't return anything valuable for FloatSchema.
func (*FloatSchema) Prop(key string) (interface{}, bool) {
	return nil, false
}

// Validate checks whether the given value is writeable to this schema.
func (*FloatSchema) Validate(v reflect.Value) bool {
	return reflect.TypeOf(dereference(v).Interface()).Kind() == reflect.Float32
}

// MarshalJSON serializes the given schema as JSON. Never returns an error.
func (*FloatSchema) MarshalJSON() ([]byte, error) {
	return []byte(`"float"`), nil
}

// Project this (writer) schema onto a different reader schema using avro schema resolution rules
func (*FloatSchema) Project(value interface{}, into Schema) (interface{}, error) {
	switch into.Type() {
	case Float:
		return value, nil
	case Double:
		return float64(value.(float32)), nil
	default:
		return nil, errors.New("Could not project Float value into reader schema: " + into.String())
	}
}

// DoubleSchema implements Schema and represents Avro double type.
type DoubleSchema struct{}

// Returns a JSON representation of DoubleSchema.
func (*DoubleSchema) String() string {
	return `{"type": "double"}`
}

// Type returns a type constant for this DoubleSchema.
func (*DoubleSchema) Type() int {
	return Double
}

// GetName returns a type name for this DoubleSchema.
func (*DoubleSchema) GetName() string {
	return typeDouble
}

// Prop doesn't return anything valuable for DoubleSchema.
func (*DoubleSchema) Prop(key string) (interface{}, bool) {
	return nil, false
}

// Validate checks whether the given value is writeable to this schema.
func (*DoubleSchema) Validate(v reflect.Value) bool {
	return reflect.TypeOf(dereference(v).Interface()).Kind() == reflect.Float64
}

// MarshalJSON serializes the given schema as JSON. Never returns an error.
func (*DoubleSchema) MarshalJSON() ([]byte, error) {
	return []byte(`"double"`), nil
}

// Project this (writer) schema onto a different reader schema using avro schema resolution rules
func (*DoubleSchema) Project(value interface{}, into Schema) (interface{}, error) {
	return value, nil
}

// BooleanSchema implements Schema and represents Avro boolean type.
type BooleanSchema struct{}

// String returns a JSON representation of BooleanSchema.
func (*BooleanSchema) String() string {
	return `{"type": "boolean"}`
}

// Type returns a type constant for this BooleanSchema.
func (*BooleanSchema) Type() int {
	return Boolean
}

// GetName returns a type name for this BooleanSchema.
func (*BooleanSchema) GetName() string {
	return typeBoolean
}

// Prop doesn't return anything valuable for BooleanSchema.
func (*BooleanSchema) Prop(key string) (interface{}, bool) {
	return nil, false
}

// Validate checks whether the given value is writeable to this schema.
func (*BooleanSchema) Validate(v reflect.Value) bool {
	return reflect.TypeOf(dereference(v).Interface()).Kind() == reflect.Bool
}

// MarshalJSON serializes the given schema as JSON. Never returns an error.
func (*BooleanSchema) MarshalJSON() ([]byte, error) {
	return []byte(`"boolean"`), nil
}

// Project this (writer) schema onto a different reader schema using avro schema resolution rules
func (*BooleanSchema) Project(value interface{}, into Schema) (interface{}, error) {
	return value, nil
}

// NullSchema implements Schema and represents Avro null type.
type NullSchema struct{}

// String returns a JSON representation of NullSchema.
func (*NullSchema) String() string {
	return `{"type": "null"}`
}

// Type returns a type constant for this NullSchema.
func (*NullSchema) Type() int {
	return Null
}

// GetName returns a type name for this NullSchema.
func (*NullSchema) GetName() string {
	return typeNull
}

// Prop doesn't return anything valuable for NullSchema.
func (*NullSchema) Prop(key string) (interface{}, bool) {
	return nil, false
}

// Validate checks whether the given value is writeable to this schema.
func (*NullSchema) Validate(v reflect.Value) bool {
	// Check if the value is something that can be null
	switch v.Kind() {
	case reflect.Interface:
		return v.IsNil()
	case reflect.Array:
		return v.Cap() == 0
	case reflect.Slice:
		return v.IsNil() || v.Cap() == 0
	case reflect.Map:
		return len(v.MapKeys()) == 0
	case reflect.String:
		return len(v.String()) == 0
	case reflect.Float32:
		// Should NaN floats be treated as null?
		return math.IsNaN(v.Float())
	case reflect.Float64:
		// Should NaN floats be treated as null?
		return math.IsNaN(v.Float())
	case reflect.Ptr:
		return v.IsNil()
	case reflect.Invalid:
		return true
	}

	// Nothing else in particular, so this should not validate?
	return false
}

// Project this (writer) schema onto a different reader schema using avro schema resolution rules
func (*NullSchema) Project(value interface{}, into Schema) (interface{}, error) {
	return value, nil
}

// MarshalJSON serializes the given schema as JSON. Never returns an error.
func (*NullSchema) MarshalJSON() ([]byte, error) {
	return []byte(`"null"`), nil
}

// RecordSchema implements Schema and represents Avro record type.
type RecordSchema struct {
	Name       string   `json:"name,omitempty"`
	Namespace  string   `json:"namespace,omitempty"`
	Doc        string   `json:"doc,omitempty"`
	Aliases    []string `json:"aliases,omitempty"`
	Properties map[string]interface{}
	Fields     []*SchemaField `json:"fields"`
	AliasIndex map[string]int
}

// String returns a JSON representation of RecordSchema.
func (s *RecordSchema) String() string {
	bytes, err := json.MarshalIndent(s, "", "    ")
	if err != nil {
		panic(err)
	}

	return string(bytes)
}

// MarshalJSON serializes the given schema as JSON.
func (s *RecordSchema) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		Type      string         `json:"type,omitempty"`
		Namespace string         `json:"namespace,omitempty"`
		Name      string         `json:"name,omitempty"`
		Doc       string         `json:"doc,omitempty"`
		Aliases   []string       `json:"aliases,omitempty"`
		Fields    []*SchemaField `json:"fields"`
	}{
		Type:      "record",
		Namespace: s.Namespace,
		Name:      s.Name,
		Doc:       s.Doc,
		Aliases:   s.Aliases,
		Fields:    s.Fields,
	})
}

// Type returns a type constant for this RecordSchema.
func (*RecordSchema) Type() int {
	return Record
}

// GetName returns a record name for this RecordSchema.
func (s *RecordSchema) GetName() string {
	return s.Name
}

// Prop gets a custom non-reserved property from this schema and a bool representing if it exists.
func (s *RecordSchema) Prop(key string) (interface{}, bool) {
	if s.Properties != nil {
		if prop, ok := s.Properties[key]; ok {
			return prop, true
		}
	}

	return nil, false
}

// Validate checks whether the given value is writeable to this schema.
func (s *RecordSchema) Validate(v reflect.Value) bool {
	v = dereference(v)
	if v.Kind() != reflect.Struct || !v.CanAddr() || !v.CanInterface() {
		return false
	}
	rec, ok := v.Interface().(GenericRecord)
	if !ok {
		// This is not a generic record and is likely a specific record. Hence
		// use the basic check.
		return v.Kind() == reflect.Struct
	}

	fieldCount := 0
	for key, val := range rec.fields {
		for idx := range s.Fields {
			// key.Name must have rs.Fields[idx].Name as a suffix
			if len(s.Fields[idx].Name) <= len(key) {
				lhs := key[len(key)-len(s.Fields[idx].Name):]
				if lhs == s.Fields[idx].Name {
					if !s.Fields[idx].Type.Validate(reflect.ValueOf(val)) {
						return false
					}
					fieldCount++
					break
				}
			}
		}
	}

	// All of the fields set must be accounted for in the union.
	if fieldCount < len(rec.fields) {
		return false
	}

	return true
}

// Project this (writer) schema onto a different reader schema using avro schema resolution rules
func (s *RecordSchema) Project(value interface{}, into Schema) (interface{}, error) {
	switch into.Type() {
	case Record:
		intoRecord := into.(*RecordSchema)
		if intoRecord.GetName() != s.GetName() {
			//TODO Record alias check with namespaces
			return nil, errors.New("TODO Record alias check")
		}
		switch v := value.(type) {
		case *GenericRecord:
			result := NewGenericRecord(into)
			NextReaderField:
			for _, intoField := range intoRecord.Fields {
				checkField := func(name string) (bool, error) {
					if w, ok := s.AliasIndex[name]; ok {
						writerField := s.Fields[w]
						//TODO instead of v.Get(string) there should also be v.GetByIndex(int) as in IndexedRecord
						//TODO instead of v.Set(string) there should also be v.SetByIndex(int,...) as in IndexedRecord
						if projectedValue, err := writerField.Type.Project(v.Get(writerField.Name), intoField.Type); err != nil {
							return false, err
						} else {
							result.Set(intoField.Name, projectedValue)
							return true, nil
						}

					}
					return false, nil
				}
				if match, err := checkField(intoField.Name); err != nil {
					return nil, err
				} else if match {
					continue NextReaderField
				}
				for _, intoFieldAlias := range intoField.Aliases {
					if match, err := checkField(intoFieldAlias); err != nil {
						return nil, err
					} else if match {
						continue NextReaderField
					}
				}
				if intoField.Default == nil {
					return nil, errors.New("Schema field doesn't have any default value: " + intoField.Name)
				} else {
					result.Set(intoField.Name, intoField.Default)
				}
			}
			return result, nil
		default:
			panic(reflect.TypeOf(v))
		}

		/*
if the reader's record schema has a field with no default value, and writer's schema does not have a field with the same name, an error is signalled.
		 */
		panic(intoRecord)
	default:
		return nil, errors.New("Record can only be projected into another Record")
	}
}

// RecursiveSchema implements Schema and represents Avro record type without a definition (e.g. that should be looked up).
type RecursiveSchema struct {
	Actual *RecordSchema
}

func newRecursiveSchema(parent *RecordSchema) *RecursiveSchema {
	return &RecursiveSchema{
		Actual: parent,
	}
}

// String returns a JSON representation of RecursiveSchema.
func (s *RecursiveSchema) String() string {
	return fmt.Sprintf(`{"type": "%s"}`, s.Actual.GetName())
}

// Type returns a type constant for this RecursiveSchema.
func (*RecursiveSchema) Type() int {
	return Recursive
}

// GetName returns a record name for enclosed RecordSchema.
func (s *RecursiveSchema) GetName() string {
	return s.Actual.GetName()
}

// Prop doesn't return anything valuable for RecursiveSchema.
func (*RecursiveSchema) Prop(key string) (interface{}, bool) {
	return nil, false
}

// Validate checks whether the given value is writeable to this schema.
func (s *RecursiveSchema) Validate(v reflect.Value) bool {
	return s.Actual.Validate(v)
}

// MarshalJSON serializes the given schema as JSON. Never returns an error.
func (s *RecursiveSchema) MarshalJSON() ([]byte, error) {
	return []byte(fmt.Sprintf(`"%s"`, s.Actual.GetName())), nil
}

// Project this (writer) schema onto a different reader schema using avro schema resolution rules
func (s *RecursiveSchema) Project(value interface{}, into Schema) (interface{}, error) {
	//TODO todo probably delegate to RecordSchema.Project(..)
	return value, nil
}

// SchemaField represents a schema field for Avro record.
type SchemaField struct {
	Name       string      `json:"name,omitempty"`
	Doc        string      `json:"doc,omitempty"`
	Default    interface{} `json:"default"`
	Type       Schema      `json:"type,omitempty"`
	Aliases    []string    `json:"aliases,omitempty"`
	Properties map[string]interface{}
}

// Gets a custom non-reserved property from this schemafield and a bool representing if it exists.
func (s *SchemaField) Prop(key string) (interface{}, bool) {
	if s.Properties != nil {
		if prop, ok := s.Properties[key]; ok {
			return prop, true
		}
	}
	return nil, false
}

// MarshalJSON serializes the given schema field as JSON.
func (s *SchemaField) MarshalJSON() ([]byte, error) {
	if s.Type.Type() == Null || (s.Type.Type() == Union && s.Type.(*UnionSchema).Types[0].Type() == Null) {
		return json.Marshal(struct {
			Name    string      `json:"name,omitempty"`
			Doc     string      `json:"doc,omitempty"`
			Default interface{} `json:"default"`
			Aliases []string    `json:"aliases,omitempty"`
			Type    Schema      `json:"type,omitempty"`
		}{
			Name:    s.Name,
			Doc:     s.Doc,
			Default: s.Default,
			Aliases: s.Aliases,
			Type:    s.Type,
		})
	}

	return json.Marshal(struct {
		Name    string      `json:"name,omitempty"`
		Doc     string      `json:"doc,omitempty"`
		Default interface{} `json:"default,omitempty"`
		Aliases []string    `json:"aliases,omitempty"`
		Type    Schema      `json:"type,omitempty"`
	}{
		Name:    s.Name,
		Doc:     s.Doc,
		Default: s.Default,
		Aliases: s.Aliases,
		Type:    s.Type,
	})
}

// String returns a JSON representation of SchemaField.
func (s *SchemaField) String() string {
	return fmt.Sprintf("[SchemaField: Name: %s, Doc: %s, Default: %v, Type: %s]", s.Name, s.Doc, s.Default, s.Type)
}

// EnumSchema implements Schema and represents Avro enum type.
type EnumSchema struct {
	Name       string
	Namespace  string
	Aliases    []string
	Doc        string
	Symbols    []string
	Properties map[string]interface{}
}

// String returns a JSON representation of EnumSchema.
func (s *EnumSchema) String() string {
	bytes, err := json.MarshalIndent(s, "", "    ")
	if err != nil {
		panic(err)
	}

	return string(bytes)
}

// Type returns a type constant for this EnumSchema.
func (*EnumSchema) Type() int {
	return Enum
}

// GetName returns an enum name for this EnumSchema.
func (s *EnumSchema) GetName() string {
	return s.Name
}

// Prop gets a custom non-reserved property from this schema and a bool representing if it exists.
func (s *EnumSchema) Prop(key string) (interface{}, bool) {
	if s.Properties != nil {
		if prop, ok := s.Properties[key]; ok {
			return prop, true
		}
	}

	return nil, false
}

// Validate checks whether the given value is writeable to this schema.
func (*EnumSchema) Validate(v reflect.Value) bool {
	//TODO implement
	return true
}

// MarshalJSON serializes the given schema as JSON.
func (s *EnumSchema) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		Type      string   `json:"type,omitempty"`
		Namespace string   `json:"namespace,omitempty"`
		Name      string   `json:"name,omitempty"`
		Doc       string   `json:"doc,omitempty"`
		Symbols   []string `json:"symbols,omitempty"`
	}{
		Type:      "enum",
		Namespace: s.Namespace,
		Name:      s.Name,
		Doc:       s.Doc,
		Symbols:   s.Symbols,
	})
}

// Project this (writer) schema onto a different reader schema using avro schema resolution rules
func (s *EnumSchema) Project(value interface{}, into Schema) (interface{}, error) {
	/*TODO
	if both are enums:
	if the writer's symbol is not present in the reader's enum, then an error is signalled.
	*/
	panic("TODO")
}

// ArraySchema implements Schema and represents Avro array type.
type ArraySchema struct {
	Items      Schema
	Properties map[string]interface{}
}

// String returns a JSON representation of ArraySchema.
func (s *ArraySchema) String() string {
	bytes, err := json.MarshalIndent(s, "", "    ")
	if err != nil {
		panic(err)
	}

	return string(bytes)
}

// Type returns a type constant for this ArraySchema.
func (*ArraySchema) Type() int {
	return Array
}

// GetName returns a type name for this ArraySchema.
func (*ArraySchema) GetName() string {
	return typeArray
}

// Prop gets a custom non-reserved property from this schema and a bool representing if it exists.
func (s *ArraySchema) Prop(key string) (interface{}, bool) {
	if s.Properties != nil {
		if prop, ok := s.Properties[key]; ok {
			return prop, true
		}
	}

	return nil, false
}

// Validate checks whether the given value is writeable to this schema.
func (s *ArraySchema) Validate(v reflect.Value) bool {
	v = dereference(v)

	// This needs to be a slice
	return v.Kind() == reflect.Slice || v.Kind() == reflect.Array
}

// MarshalJSON serializes the given schema as JSON.
func (s *ArraySchema) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		Type  string `json:"type,omitempty"`
		Items Schema `json:"items,omitempty"`
	}{
		Type:  "array",
		Items: s.Items,
	})
}

// Project this (writer) schema onto a different reader schema using avro schema resolution rules
func (s *ArraySchema) Project(value interface{}, into Schema) (interface{}, error) {
	/*TODO
	if both are arrays:
	This resolution algorithm is applied recursively to the reader's and writer's array item schemas.
	*/
	panic("TODO")
}

// MapSchema implements Schema and represents Avro map type.
type MapSchema struct {
	Values     Schema
	Properties map[string]interface{}
}

// String returns a JSON representation of MapSchema.
func (s *MapSchema) String() string {
	bytes, err := json.MarshalIndent(s, "", "    ")
	if err != nil {
		panic(err)
	}

	return string(bytes)
}

// Type returns a type constant for this MapSchema.
func (*MapSchema) Type() int {
	return Map
}

// GetName returns a type name for this MapSchema.
func (*MapSchema) GetName() string {
	return typeMap
}

// Prop gets a custom non-reserved property from this schema and a bool representing if it exists.
func (s *MapSchema) Prop(key string) (interface{}, bool) {
	if s.Properties != nil {
		if prop, ok := s.Properties[key]; ok {
			return prop, true
		}
	}
	return nil, false
}

// Validate checks whether the given value is writeable to this schema.
func (s *MapSchema) Validate(v reflect.Value) bool {
	v = dereference(v)

	return v.Kind() == reflect.Map && v.Type().Key().Kind() == reflect.String
}

// MarshalJSON serializes the given schema as JSON.
func (s *MapSchema) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		Type   string `json:"type,omitempty"`
		Values Schema `json:"values,omitempty"`
	}{
		Type:   "map",
		Values: s.Values,
	})
}

// Project this (writer) schema onto a different reader schema using avro schema resolution rules
func (s *MapSchema) Project(value interface{}, into Schema) (interface{}, error) {
	/*TODO
	if both are maps:
	This resolution algorithm is applied recursively to the reader's and writer's value schemas.
	*/
	panic("TODO")
}

// UnionSchema implements Schema and represents Avro union type.
type UnionSchema struct {
	Types []Schema
}

// String returns a JSON representation of UnionSchema.
func (s *UnionSchema) String() string {
	bytes, err := json.MarshalIndent(s, "", "    ")
	if err != nil {
		panic(err)
	}

	return fmt.Sprintf(`{"type": %s}`, string(bytes))
}

// Type returns a type constant for this UnionSchema.
func (*UnionSchema) Type() int {
	return Union
}

// GetName returns a type name for this UnionSchema.
func (*UnionSchema) GetName() string {
	return typeUnion
}

// Prop doesn't return anything valuable for UnionSchema.
func (*UnionSchema) Prop(key string) (interface{}, bool) {
	return nil, false
}

// GetType gets the index of actual union type for a given value.
func (s *UnionSchema) GetType(v reflect.Value) int {
	if s.Types != nil {
		for i := range s.Types {
			if t := s.Types[i]; t.Validate(v) {
				return i
			}
		}
	}

	return -1
}

// Validate checks whether the given value is writeable to this schema.
func (s *UnionSchema) Validate(v reflect.Value) bool {
	v = dereference(v)
	for i := range s.Types {
		if t := s.Types[i]; t.Validate(v) {
			return true
		}
	}

	return false
}

// MarshalJSON serializes the given schema as JSON.
func (s *UnionSchema) MarshalJSON() ([]byte, error) {
	return json.Marshal(s.Types)
}

// Project this (writer) schema onto a different reader schema using avro schema resolution rules
func (s *UnionSchema) Project(value interface{}, into Schema) (interface{}, error) {
	/*TODO
	if both are unions:
	The first schema in the reader's union that matches the selected writer's union schema is recursively resolved against it. if none match, an error is signalled.

	if reader's is a union, but writer's is not
	The first schema in the reader's union that matches the writer's schema is recursively resolved against it. If none match, an error is signalled.

	if writer's is a union, but reader's is not
	If the reader's schema matches the selected writer's schema, it is recursively resolved against it. If they do not match, an error is signalled.
	*/
	panic("TODO")
}

// FixedSchema implements Schema and represents Avro fixed type.
type FixedSchema struct {
	Namespace  string
	Name       string
	Size       int
	Properties map[string]interface{}
}

// String returns a JSON representation of FixedSchema.
func (s *FixedSchema) String() string {
	bytes, err := json.MarshalIndent(s, "", "    ")
	if err != nil {
		panic(err)
	}

	return string(bytes)
}

// Type returns a type constant for this FixedSchema.
func (*FixedSchema) Type() int {
	return Fixed
}

// GetName returns a fixed name for this FixedSchema.
func (s *FixedSchema) GetName() string {
	return s.Name
}

// Prop gets a custom non-reserved property from this schema and a bool representing if it exists.
func (s *FixedSchema) Prop(key string) (interface{}, bool) {
	if s.Properties != nil {
		if prop, ok := s.Properties[key]; ok {
			return prop, true
		}
	}
	return nil, false
}

// Validate checks whether the given value is writeable to this schema.
func (s *FixedSchema) Validate(v reflect.Value) bool {
	v = dereference(v)

	return (v.Kind() == reflect.Array || v.Kind() == reflect.Slice) && v.Type().Elem().Kind() == reflect.Uint8 && v.Len() == s.Size
}

// MarshalJSON serializes the given schema as JSON.
func (s *FixedSchema) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		Type string `json:"type,omitempty"`
		Size int    `json:"size,omitempty"`
		Name string `json:"name,omitempty"`
	}{
		Type: "fixed",
		Size: s.Size,
		Name: s.Name,
	})
}

// Project this (writer) schema onto a different reader schema using avro schema resolution rules
func (s *FixedSchema) Project(value interface{}, into Schema) (interface{}, error) {
	return value, nil
}

// GetFullName returns a fully-qualified name for a schema if possible. The format is namespace.name.
func GetFullName(schema Schema) string {
	switch sch := schema.(type) {
	case *RecordSchema:
		return getFullName(sch.GetName(), sch.Namespace)
	case *EnumSchema:
		return getFullName(sch.GetName(), sch.Namespace)
	case *FixedSchema:
		return getFullName(sch.GetName(), sch.Namespace)
	default:
		return schema.GetName()
	}
}

// ParseSchemaFile parses a given file.
// May return an error if schema is not parsable or file does not exist.
func ParseSchemaFile(file string) (Schema, error) {
	fileContents, err := ioutil.ReadFile(file)
	if err != nil {
		return nil, err
	}

	return ParseSchema(string(fileContents))
}

// ParseSchema parses a given schema without provided schemas to reuse.
// Equivalent to call ParseSchemaWithResistry(rawSchema, make(map[string]Schema))
// May return an error if schema is not parsable or has insufficient information about any type.
func ParseSchema(rawSchema string) (Schema, error) {
	return ParseSchemaWithRegistry(rawSchema, make(map[string]Schema))
}

// ParseSchemaWithRegistry parses a given schema using the provided registry for type lookup.
// Registry will be filled up during parsing.
// May return an error if schema is not parsable or has insufficient information about any type.
func ParseSchemaWithRegistry(rawSchema string, schemas map[string]Schema) (Schema, error) {
	var schema interface{}
	if err := json.Unmarshal([]byte(rawSchema), &schema); err != nil {
		schema = rawSchema
	}

	return schemaByType(schema, schemas, "")
}

// MustParseSchema is like ParseSchema, but panics if the given schema cannot be parsed.
func MustParseSchema(rawSchema string) Schema {
	s, err := ParseSchema(rawSchema)
	if err != nil {
		panic(err)
	}
	return s
}

func schemaByType(i interface{}, registry map[string]Schema, namespace string) (Schema, error) {
	switch v := i.(type) {
	case nil:
		return new(NullSchema), nil
	case string:
		switch v {
		case typeNull:
			return new(NullSchema), nil
		case typeBoolean:
			return new(BooleanSchema), nil
		case typeInt:
			return new(IntSchema), nil
		case typeLong:
			return new(LongSchema), nil
		case typeFloat:
			return new(FloatSchema), nil
		case typeDouble:
			return new(DoubleSchema), nil
		case typeBytes:
			return new(BytesSchema), nil
		case typeString:
			return new(StringSchema), nil
		default:
			// If a name reference contains a dot, we consider it a full name reference.
			// Otherwise, use the getFullName helper to look up the name.
			// See https://avro.apache.org/docs/1.7.7/spec.html#Names
			fullName := v
			if !strings.ContainsRune(fullName, '.') {
				fullName = getFullName(v, namespace)
			}
			schema, ok := registry[fullName]
			if !ok {
				return nil, fmt.Errorf("Unknown type name: %s", v)
			}

			return schema, nil
		}
	case map[string][]interface{}:
		return parseUnionSchema(v[schemaTypeField], registry, namespace)
	case map[string]interface{}:
		switch v[schemaTypeField] {
		case typeNull:
			return new(NullSchema), nil
		case typeBoolean:
			return new(BooleanSchema), nil
		case typeInt:
			return new(IntSchema), nil
		case typeLong:
			return new(LongSchema), nil
		case typeFloat:
			return new(FloatSchema), nil
		case typeDouble:
			return new(DoubleSchema), nil
		case typeBytes:
			return new(BytesSchema), nil
		case typeString:
			return new(StringSchema), nil
		case typeArray:
			items, err := schemaByType(v[schemaItemsField], registry, namespace)
			if err != nil {
				return nil, err
			}
			return &ArraySchema{Items: items, Properties: getProperties(v)}, nil
		case typeMap:
			values, err := schemaByType(v[schemaValuesField], registry, namespace)
			if err != nil {
				return nil, err
			}
			return &MapSchema{Values: values, Properties: getProperties(v)}, nil
		case typeEnum:
			return parseEnumSchema(v, registry, namespace)
		case typeFixed:
			return parseFixedSchema(v, registry, namespace)
		case typeRecord:
			return parseRecordSchema(v, registry, namespace)
		default:
			// Type references can also be done as {"type": "otherType"}.
			// Just call back in so we can handle this scenario in the string matcher above.
			return schemaByType(v[schemaTypeField], registry, namespace)
		}
	case []interface{}:
		return parseUnionSchema(v, registry, namespace)
	}

	return nil, ErrInvalidSchema
}

func parseEnumSchema(v map[string]interface{}, registry map[string]Schema, namespace string) (Schema, error) {
	symbols := make([]string, len(v[schemaSymbolsField].([]interface{})))
	for i, symbol := range v[schemaSymbolsField].([]interface{}) {
		symbols[i] = symbol.(string)
	}

	schema := &EnumSchema{Name: v[schemaNameField].(string), Symbols: symbols}
	setOptionalField(&schema.Namespace, v, schemaNamespaceField)
	setOptionalField(&schema.Doc, v, schemaDocField)
	schema.Properties = getProperties(v)

	return addSchema(getFullName(v[schemaNameField].(string), namespace), schema, registry), nil
}

func parseFixedSchema(v map[string]interface{}, registry map[string]Schema, namespace string) (Schema, error) {
	size, ok := v[schemaSizeField].(float64)
	if !ok {
		return nil, ErrInvalidFixedSize
	}

	schema := &FixedSchema{Name: v[schemaNameField].(string), Size: int(size), Properties: getProperties(v)}
	setOptionalField(&schema.Namespace, v, schemaNamespaceField)
	return addSchema(getFullName(v[schemaNameField].(string), namespace), schema, registry), nil
}

func parseUnionSchema(v []interface{}, registry map[string]Schema, namespace string) (Schema, error) {
	types := make([]Schema, len(v))
	var err error
	for i := range v {
		types[i], err = schemaByType(v[i], registry, namespace)
		if err != nil {
			return nil, err
		}
	}
	return &UnionSchema{Types: types}, nil
}

func parseRecordSchema(v map[string]interface{}, registry map[string]Schema, namespace string) (Schema, error) {
	schema := &RecordSchema{Name: v[schemaNameField].(string)}
	setOptionalField(&schema.Namespace, v, schemaNamespaceField)
	setOptionalField(&namespace, v, schemaNamespaceField)
	setOptionalField(&schema.Doc, v, schemaDocField)
	addSchema(getFullName(v[schemaNameField].(string), namespace), newRecursiveSchema(schema), registry)
	schema.AliasIndex = make(map[string]int)
	fields := make([]*SchemaField, len(v[schemaFieldsField].([]interface{})))
	for i := range fields {
		field, err := parseSchemaField(v[schemaFieldsField].([]interface{})[i], registry, namespace)
		if err != nil {
			return nil, err
		}
		fields[i] = field
		schema.AliasIndex[field.Name] = i
		for _, alias := range field.Aliases {
			schema.AliasIndex[alias] = i
		}
	}
	schema.Fields = fields
	schema.Properties = getProperties(v)

	return schema, nil
}

func parseSchemaField(i interface{}, registry map[string]Schema, namespace string) (*SchemaField, error) {
	switch v := i.(type) {
	case map[string]interface{}:
		name, ok := v[schemaNameField].(string)
		if !ok {
			return nil, fmt.Errorf("Schema field name missing")
		}
		schemaField := &SchemaField{Name: name, Properties: getProperties(v)}
		setOptionalField(&schemaField.Doc, v, schemaDocField)
		if aliases, ok := v[schemaAliasesField]; ok {
			for _, a := range aliases.([]interface{}) {
				schemaField.Aliases = append(schemaField.Aliases, a.(string))
			}
		}
		fieldType, err := schemaByType(v[schemaTypeField], registry, namespace)
		if err != nil {
			return nil, err
		}
		schemaField.Type = fieldType

		if def, exists := v[schemaDefaultField]; exists {
			switch def.(type) {
			case float64:
				// JSON treats all numbers as float64 by default
				switch schemaField.Type.Type() {
				case Int:
					var converted = int32(def.(float64))
					schemaField.Default = converted
				case Long:
					var converted = int64(def.(float64))
					schemaField.Default = converted
				case Float:
					var converted = float32(def.(float64))
					schemaField.Default = converted

				default:
					schemaField.Default = def
				}
			default:
				schemaField.Default = def
			}
		}
		return schemaField, nil
	}

	return nil, ErrInvalidSchema
}

func setOptionalField(where *string, v map[string]interface{}, fieldName string) {
	if field, exists := v[fieldName]; exists {
		*where = field.(string)
	}
}

func addSchema(name string, schema Schema, schemas map[string]Schema) Schema {
	if schemas != nil {
		if sch, ok := schemas[name]; ok {
			return sch
		}

		schemas[name] = schema
	}

	return schema
}

func getFullName(name string, namespace string) string {
	if len(namespace) > 0 && !strings.ContainsRune(name, '.') {
		return namespace + "." + name
	}

	return name
}

// gets custom string properties from a given schema
func getProperties(v map[string]interface{}) map[string]interface{} {
	props := make(map[string]interface{})
	for name, value := range v {
		if !isReserved(name) {
			props[name] = value
		}
	}
	return props
}

func isReserved(name string) bool {
	switch name {
	case schemaAliasesField, schemaDocField, schemaFieldsField, schemaItemsField, schemaNameField,
		schemaNamespaceField, schemaSizeField, schemaSymbolsField, schemaTypeField, schemaValuesField:
		return true
	}

	return false
}

func dereference(v reflect.Value) reflect.Value {
	if v.Kind() == reflect.Ptr {
		return v.Elem()
	}

	return v
}
