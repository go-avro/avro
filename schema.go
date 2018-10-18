package avro

import (
	"bytes"
	"encoding/json"
	"fmt"
	"hash/crc64"
	"io/ioutil"
	"math"
	"reflect"
	"strings"
	"time"
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

	// logical types
	logicalTypeDate            = "date"
	logicalTypeDecimal         = "decimal"
	logicalTypeDuration        = "duration"
	logicalTypeTimeOfDayMillis = "time-millis"
	logicalTypeTimeOfDayMicros = "time-micros"
	logicalTypeTimeMillis      = "timestamp-millis"
	logicalTypeTimeMicros      = "timestamp-micros"
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

	// logical types - see https://avro.apache.org/docs/1.8.2/spec.html#Logical+Types
	schemaLogicalTypeField = "logicalType"
	schemaScaleField       = "scale"
	schemaPrecisionField   = "precision"
)

// Schema is an interface representing a single Avro schema (both primitive and complex).
type Schema interface {
	// Canonical returns the encoded schema JSON after
	// https://avro.apache.org/docs/1.8.2/spec.html#Transforming+into+Parsing+Canonical+Form
	Canonical() ([]byte, error)

	// Fingerprint returns a CRC64 of the canonical form and is cached in the schema.
	Fingerprint() uint64

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
}

type hashable struct {
	hash      uint64
	canonical func() []byte
	valid     bool
}

// use the polynomial from the avro spec
var polynomialTable = crc64.MakeTable(0xc15d213aa4d7a795)

// Fingerprint helps types that embed hashable to implement
func (hashable *hashable) getFingerprint(schema Schema) uint64 {
	if hashable.valid {
		return hashable.hash
	}
	data, err := schema.Canonical()
	if err != nil {
		panic(fmt.Sprintf("failed to get canonical schema for %s: %v", GetFullName(schema), err))
	}
	hash64 := crc64.New(polynomialTable)
	n, err := hash64.Write(data)
	if n != len(data) {
		panic(fmt.Sprintf("crc64 refused to accept our data? short read %d < %d", n, len(data)))
	}
	if err != nil {
		panic(fmt.Sprintf("crc64 failed: %v", err))
	}
	hashable.hash = hash64.Sum64()
	hashable.valid = true
	return hashable.hash
}

// StringSchema implements Schema and represents Avro string type.
type StringSchema struct {
	hashable
}

// Canonical implements Schema and returns the 'Canonical Form' for matching, etc
func (ss *StringSchema) Canonical() ([]byte, error) { return ss.MarshalJSON() }

// Fingerprint implement Schema is a way to uniquely identify avro schemas by their content.
func (ss *StringSchema) Fingerprint() uint64 { return ss.getFingerprint(ss) }

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
	_, ok := dereference(v).Interface().(string)
	return ok
}

// MarshalJSON serializes the given schema as JSON. Never returns an error.
func (*StringSchema) MarshalJSON() ([]byte, error) {
	return []byte(`"string"`), nil
}

// BytesSchema implements Schema and represents Avro bytes type.
type BytesSchema struct {
	hashable
	LogicalType string `json:"logicalType,omitempty"`
	Scale       int    `json:"scale,omitempty"`
	Precision   int    `json:"precision,omitempty"`
}

// Canonical implements Schema and returns the 'Canonical Form' for matching, etc
func (bs *BytesSchema) Canonical() ([]byte, error) {
	return []byte(`"bytes"`), nil
}

// Fingerprint implement Schema is a way to uniquely identify avro schemas by their content.
func (bs *BytesSchema) Fingerprint() uint64 { return bs.getFingerprint(bs) }

// String returns a JSON representation of BytesSchema.
func (bs *BytesSchema) String() string {
	if bs.LogicalType == "" {
		return `{"type": "bytes"}`
	}
	jb, _ := bs.MarshalJSON()
	return string(jb)
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
func (bs *BytesSchema) MarshalJSON() ([]byte, error) {
	if bs.LogicalType == "" {
		return []byte(`"bytes"`), nil
	}
	if bs.LogicalType == logicalTypeDecimal {
		// the only logical type on bytes is decimal
		return json.Marshal(struct {
			Type        string `json:"type"`
			LogicalType string `json:"logicalType"`
			Scale       int    `json:"scale"`
			Precision   int    `json:"precision"`
		}{
			Type:        "bytes",
			LogicalType: bs.LogicalType,
			Scale:       bs.Scale,
			Precision:   bs.Precision,
		})
	} else {
		// otherwise who knows?
		// the only logical type on bytes is decimal
		return json.Marshal(struct {
			Type        string `json:"type"`
			LogicalType string `json:"logicalType"`
		}{
			Type:        "bytes",
			LogicalType: bs.LogicalType,
		})
	}
}

// IntSchema implements Schema and represents Avro int type.
type IntSchema struct{ hashable }

// Canonical implements Schema and returns the 'Canonical Form' for matching, etc
func (is IntSchema) Canonical() ([]byte, error) { return is.MarshalJSON() }

// Fingerprint implement Schema is a way to uniquely identify avro schemas by their content.
func (is *IntSchema) Fingerprint() uint64 { return is.getFingerprint(is) }

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

// LongSchema implements Schema and represents Avro long type.
type LongSchema struct {
	hashable
	LogicalType string
}

// Canonical implements Schema and returns the 'Canonical Form' for matching, etc
func (ls LongSchema) Canonical() ([]byte, error) { return []byte(`"long"`), nil }

// Fingerprint implement Schema is a way to uniquely identify avro schemas by their content.
func (ls *LongSchema) Fingerprint() uint64 { return ls.getFingerprint(ls) }

// Returns a JSON representation of LongSchema.
func (ls *LongSchema) String() string {
	if ls.LogicalType == "" {
		return `{"type": "long"}`
	}
	jb, _ := ls.MarshalJSON()
	return string(jb)
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
func (ls *LongSchema) Validate(v reflect.Value) bool {
	switch ls.LogicalType {
	case logicalTypeTimeOfDayMicros:
		if _, ok := dereference(v).Interface().(time.Time); ok {
			return ok
		}
		if _, ok := dereference(v).Interface().(time.Duration); ok {
			return ok
		}
		return false
	case logicalTypeTimeMillis, logicalTypeTimeMicros:
		if _, ok := dereference(v).Interface().(time.Time); ok {
			return ok
		}
		return false
	default:
		return reflect.TypeOf(dereference(v).Interface()).Kind() == reflect.Int64
	}
}

// GetInt64 gets the signed 64-bit value from the given value.
func (ls *LongSchema) GetInt64(v reflect.Value) int64 {
	switch ls.LogicalType {
	case logicalTypeTimeOfDayMicros:
		if timeVal, ok := dereference(v).Interface().(time.Time); ok {
			h, m, s := timeVal.Clock()
			duration := time.Duration(h)*time.Hour + time.Duration(s)*time.Second
			duration += time.Duration(m)*time.Minute + time.Duration(timeVal.Nanosecond())
			return int64(duration / time.Microsecond)
		}
		if durationVal, ok := dereference(v).Interface().(time.Duration); ok {
			return int64(durationVal / time.Microsecond)
		}
	case logicalTypeTimeMillis:
		if timeVal, ok := dereference(v).Interface().(time.Time); ok {
			millis := int64(time.Duration(timeVal.Nanosecond()) / time.Millisecond)
			millis += timeVal.Unix() * 1000
			return millis
		}
	case logicalTypeTimeMicros:
		if timeVal, ok := dereference(v).Interface().(time.Time); ok {
			micros := int64(time.Duration(timeVal.Nanosecond()) / time.Microsecond)
			micros += timeVal.Unix() * 1e6
			return micros
		}
	}
	return dereference(v).Interface().(int64)
}

// MarshalJSON serializes the given schema as JSON. Never returns an error.
func (ls *LongSchema) MarshalJSON() ([]byte, error) {
	if ls.LogicalType == "" {
		return []byte(`"long"`), nil
	}
	return json.Marshal(struct {
		Type        string `json:"type"`
		LogicalType string `json:"logicalType,omitempty"`
	}{
		Type:        "long",
		LogicalType: ls.LogicalType,
	})
}

// FloatSchema implements Schema and represents Avro float type.
type FloatSchema struct{ hashable }

// Canonical implements Schema and returns the 'Canonical Form' for matching, etc
func (fs FloatSchema) Canonical() ([]byte, error) { return fs.MarshalJSON() }

// Fingerprint implement Schema is a way to uniquely identify avro schemas by their content.
func (fs *FloatSchema) Fingerprint() uint64 { return fs.getFingerprint(fs) }

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

// DoubleSchema implements Schema and represents Avro double type.
type DoubleSchema struct{ hashable }

// Canonical implements Schema and returns the 'Canonical Form' for matching, etc
func (ds DoubleSchema) Canonical() ([]byte, error) { return ds.MarshalJSON() }

// Fingerprint implement Schema is a way to uniquely identify avro schemas by their content.
func (ds *DoubleSchema) Fingerprint() uint64 { return ds.getFingerprint(ds) }

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

// BooleanSchema implements Schema and represents Avro boolean type.
type BooleanSchema struct {
	hashable
}

// Canonical implements Schema and returns the 'Canonical Form' for matching, etc
func (bs BooleanSchema) Canonical() ([]byte, error) { return bs.MarshalJSON() }

// Fingerprint implement Schema is a way to uniquely identify avro schemas by their content.
func (bs *BooleanSchema) Fingerprint() uint64 { return bs.getFingerprint(bs) }

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

// NullSchema implements Schema and represents Avro null type.
type NullSchema struct{ hashable }

// Canonical implements Schema and returns the 'Canonical Form' for matching, etc
func (ns NullSchema) Canonical() ([]byte, error) { return ns.MarshalJSON() }

// Fingerprint implement Schema is a way to uniquely identify avro schemas by their content.
func (ns *NullSchema) Fingerprint() uint64 { return ns.getFingerprint(ns) }

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

// MarshalJSON serializes the given schema as JSON. Never returns an error.
func (*NullSchema) MarshalJSON() ([]byte, error) {
	return []byte(`"null"`), nil
}

// RecordSchema implements Schema and represents Avro record type.
type RecordSchema struct {
	hashable
	Name       string   `json:"name,omitempty"`
	Namespace  string   `json:"namespace,omitempty"`
	Doc        string   `json:"doc,omitempty"`
	Aliases    []string `json:"aliases,omitempty"`
	Properties map[string]interface{}
	Fields     []*SchemaField `json:"fields"`
}

// Canonical implements Schema and returns the 'Canonical Form' for matching, etc
func (rs RecordSchema) Canonical() ([]byte, error) {
	var buf bytes.Buffer
	buf.WriteRune('{')
	// [STRIP] Keep only attributes that are relevant to parsing data, which are: name, fields
	// [ORDER] Order the appearance of fields of JSON objects as follows: name, fields
	writeString(&buf, "name", getFullName(rs.Name, rs.Namespace), false) // name is required
	// fields, also required
	writeFieldName(&buf, "fields", true)
	buf.WriteRune('[')
	for fieldIdx, field := range rs.Fields {
		if fieldIdx > 0 {
			buf.WriteRune(',')
		}
		fieldCanon, err := field.Canonical()
		if err != nil {
			return nil, fmt.Errorf("failed to convert field '%s' of %s to canonical: %v",
				field.Name, getFullName(rs.Name, rs.Namespace), err)
		}
		buf.Write(fieldCanon)
	}
	buf.WriteRune(']')
	buf.WriteRune('}')
	return buf.Bytes(), nil
}

// Fingerprint implement Schema is a way to uniquely identify avro schemas by their content.
func (rs *RecordSchema) Fingerprint() uint64 { return rs.getFingerprint(rs) }

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

// RecursiveSchema implements Schema and represents Avro record type without a definition (e.g. that should be looked up).
type RecursiveSchema struct {
	hashable
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

// String returns a JSON representation of RecursiveSchema.
func (s *RecursiveSchema) Canonical() ([]byte, error) {
	return s.MarshalJSON()
}

// Fingerprint implement Schema is a way to uniquely identify avro schemas by their content.
func (s *RecursiveSchema) Fingerprint() uint64 { return s.getFingerprint(s) }

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

// SchemaField represents a schema field for Avro record.
type SchemaField struct {
	Name       string      `json:"name,omitempty"`
	Doc        string      `json:"doc,omitempty"`
	Default    interface{} `json:"default"`
	Aliases    []string    `json:"aliases,omitempty"`
	Type       Schema      `json:"type,omitempty"`
	Properties map[string]interface{}
}

// Canonical implements Schema and returns the 'Canonical Form' for matching, etc
func (s *SchemaField) Canonical() ([]byte, error) {
	// [STRIP] Keep only attributes that are relevant to parsing data, which are:
	// type, name.  Strip all others (e.g., doc and aliases).
	var buf bytes.Buffer
	buf.WriteRune('{')
	// [ORDER] Order the appearance of fields of JSON objects as follows: name, type
	writeString(&buf, "name", s.Name, false) // name is required
	// fields, also required
	writeFieldName(&buf, "type", true)
	fieldTypeCanonical, err := s.Type.Canonical()
	if err != nil {
		return nil, fmt.Errorf("failed to convert type '%s' in field '%s' to canonical: %v",
			GetFullName(s.Type), s.Name, err)
	}
	buf.Write(fieldTypeCanonical)
	buf.WriteRune('}')
	return buf.Bytes(), nil
}

// Gets a custom non-reserved property from this schemafield and a bool representing if it exists.
func (this *SchemaField) Prop(key string) (interface{}, bool) {
	if this.Properties != nil {
		if prop, ok := this.Properties[key]; ok {
			return prop, true
		}
	}
	return nil, false
}

// MarshalJSON serializes the given schema field as JSON.
func (s *SchemaField) MarshalJSON() ([]byte, error) {
	if s.Type.Type() == Null || (s.Type.Type() == Union && s.Type.(*UnionSchema).Types[0].Type() == Null) {
		return json.Marshal(struct {
			Name    string      `json:"name"`
			Doc     string      `json:"doc,omitempty"`
			Default interface{} `json:"default"`
			Type    Schema      `json:"type"`
		}{
			Name:    s.Name,
			Doc:     s.Doc,
			Default: s.Default,
			Type:    s.Type,
		})
	}

	return json.Marshal(struct {
		Name    string      `json:"name"`
		Doc     string      `json:"doc,omitempty"`
		Default interface{} `json:"default,omitempty"`
		Type    Schema      `json:"type"`
	}{
		Name:    s.Name,
		Doc:     s.Doc,
		Default: s.Default,
		Type:    s.Type,
	})
}

// String returns a JSON representation of SchemaField.
func (s *SchemaField) String() string {
	return fmt.Sprintf("[SchemaField: Name: %s, Doc: %s, Default: %v, Type: %s]", s.Name, s.Doc, s.Default, s.Type)
}

// EnumSchema implements Schema and represents Avro enum type.
type EnumSchema struct {
	hashable
	Name       string
	Namespace  string
	Aliases    []string
	Doc        string
	Symbols    []string
	Properties map[string]interface{}
}

// Fingerprint implement Schema is a way to uniquely identify avro schemas by their content.
func (s *EnumSchema) Fingerprint() uint64 { return s.getFingerprint(s) }

// Canonical implements Schema and returns the 'Canonical Form' for matching, etc
func (s *EnumSchema) Canonical() ([]byte, error) {
	// [STRIP] Keep only attributes that are relevant to parsing data, which are: name, symbols
	var buf bytes.Buffer
	buf.WriteRune('{')
	// [ORDER] Order the appearance of fields of JSON objects as follows: name, symbols
	writeString(&buf, "name", s.Name, false) // name is required
	// fields, also required
	writeFieldName(&buf, "symbols", true)
	buf.WriteRune('[')
	for symIdx, sym := range s.Symbols {
		if symIdx > 0 {
			buf.WriteRune(',')
		}
		buf.WriteRune('"')
		buf.WriteString(sym)
		buf.WriteRune('"')
	}
	buf.WriteRune(']')
	buf.WriteRune('}')
	return buf.Bytes(), nil
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

// ArraySchema implements Schema and represents Avro array type.
type ArraySchema struct {
	hashable
	Items      Schema
	Properties map[string]interface{}
}

// Fingerprint implement Schema is a way to uniquely identify avro schemas by their content.
func (s *ArraySchema) Fingerprint() uint64 { return s.getFingerprint(s) }

// Canonical implements Schema and returns the 'Canonical Form' for matching, etc
func (s *ArraySchema) Canonical() ([]byte, error) {
	var buf bytes.Buffer
	buf.WriteRune('{')
	// [ORDER] Order the appearance of fields of JSON objects as follows: type, items
	writeString(&buf, "type", "array", false) // name is required
	// fields, also required
	writeFieldName(&buf, "items", true)
	itemsCanonical, err := s.Items.Canonical()
	if err != nil {
		return nil, fmt.Errorf("failed to convert array item type '%s' to canonical: %v",
			GetFullName(s.Items), err)
	}
	buf.Write(itemsCanonical)
	buf.WriteRune('}')
	return buf.Bytes(), nil
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

// MapSchema implements Schema and represents Avro map type.
type MapSchema struct {
	hashable
	Values     Schema
	Properties map[string]interface{}
}

// Fingerprint implement Schema is a way to uniquely identify avro schemas by their content.
func (s *MapSchema) Fingerprint() uint64 { return s.getFingerprint(s) }

// Canonical implements Schema and returns the 'Canonical Form' for matching, etc
func (s *MapSchema) Canonical() ([]byte, error) {
	var buf bytes.Buffer
	buf.WriteRune('{')
	// [ORDER] Order the appearance of fields of JSON objects as follows: type, values
	writeString(&buf, "type", "map", false) // name is required
	// values, also required
	writeFieldName(&buf, "values", true)
	valuesCanonical, err := s.Values.Canonical()
	if err != nil {
		return nil, fmt.Errorf("failed to convert map value type '%s' to canonical: %v",
			GetFullName(s.Values), err)
	}
	buf.Write(valuesCanonical)
	buf.WriteRune('}')
	return buf.Bytes(), nil
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

// UnionSchema implements Schema and represents Avro union type.
type UnionSchema struct {
	hashable
	Types []Schema
}

// Fingerprint implement Schema is a way to uniquely identify avro schemas by their content.
func (s *UnionSchema) Fingerprint() uint64 { return s.getFingerprint(s) }

// Canonical implements Schema and returns the 'Canonical Form' for matching, etc
func (s UnionSchema) Canonical() ([]byte, error) {
	var buf bytes.Buffer
	buf.WriteRune('[')
	for typeIdx, typeSchema := range s.Types {
		if typeIdx > 0 {
			buf.WriteRune(',')
		}
		typeCanon, err := typeSchema.Canonical()
		if err != nil {
			return nil, fmt.Errorf("failed to convert union value at idx #%d (%s) to canonical: %v",
				typeIdx, GetFullName(typeSchema), err)
		}
		buf.Write(typeCanon)
	}
	buf.WriteRune(']')
	return buf.Bytes(), nil
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

// FixedSchema implements Schema and represents Avro fixed type.
type FixedSchema struct {
	hashable
	Namespace   string
	Name        string
	Size        int
	LogicalType string
	Scale       int
	Precision   int
	Properties  map[string]interface{}
}

// Fingerprint implement Schema is a way to uniquely identify avro schemas by their content.
func (s *FixedSchema) Fingerprint() uint64 { return s.getFingerprint(s) }

// Canonical implements Schema and returns the 'Canonical Form' for matching, etc
func (s FixedSchema) Canonical() ([]byte, error) {
	var buf bytes.Buffer
	// [ORDER] Order the appearance of fields of JSON objects as follows: name, type, size.
	buf.WriteRune('{')
	writeString(&buf, "name", getFullName(s.Name, s.Namespace), false)
	writeString(&buf, "type", "fixed", true)
	writeInt(&buf, "size", s.Size, true)
	buf.WriteRune('}')
	return buf.Bytes(), nil
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
		Type        string `json:"type,omitempty"`
		Size        int    `json:"size,omitempty"`
		Name        string `json:"name,omitempty"`
		LogicalType string `json:"logicalType,omitempty"`
		Scale       int    `json:"scale,omitempty"`
		Precision   int    `json:"precision,omitempty"`
	}{
		Type:        "fixed",
		Size:        s.Size,
		Name:        s.Name,
		LogicalType: s.LogicalType,
		Scale:       s.Scale,
		Precision:   s.Precision,
	})
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
			if logicalType, ok := v[schemaLogicalTypeField].(string); ok {
				return &LongSchema{LogicalType: logicalType}, nil
			}
			return new(LongSchema), nil
		case typeFloat:
			return new(FloatSchema), nil
		case typeDouble:
			return new(DoubleSchema), nil
		case typeBytes:
			return parseBytesSchema(v, registry, namespace)
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
	logicalType, scale, precision, err := parseLogicalType(v)
	if err != nil {
		return nil, err
	}
	schema := &FixedSchema{
		Name:        v[schemaNameField].(string),
		Size:        int(size),
		LogicalType: logicalType,
		Scale:       scale,
		Precision:   precision,
		Properties:  getProperties(v),
	}
	setOptionalField(&schema.Namespace, v, schemaNamespaceField)
	return addSchema(getFullName(v[schemaNameField].(string), namespace), schema, registry), nil
}

func parseBytesSchema(v map[string]interface{}, registry map[string]Schema, namespace string) (Schema, error) {
	logicalType, scale, precision, err := parseLogicalType(v)
	if err != nil {
		return nil, err
	}
	schema := &BytesSchema{
		LogicalType: logicalType,
		Scale:       scale,
		Precision:   precision,
	}
	return schema, nil
}

func parseLogicalType(v map[string]interface{}) (logicalType string, scale, precision int, err error) {
	logicalType, _ = v[schemaLogicalTypeField].(string)
	if logicalType == logicalTypeDecimal {
		var ok bool
		var tmpFloat float64
		if tmpFloat, ok = v[schemaScaleField].(float64); ok {
			scale = int(tmpFloat)
		} else {
			scale, _ = v[schemaScaleField].(int)
		}
		if tmpFloat, ok = v[schemaPrecisionField].(float64); ok {
			precision = int(tmpFloat)
		} else if precision, ok = v[schemaPrecisionField].(int); !ok {
			return "", -1, -1, ErrPrecisionRequired
		}
	}
	return
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
	setOptionalStringListField(&schema.Aliases, v, schemaAliasesField)
	addSchema(getFullName(v[schemaNameField].(string), namespace), newRecursiveSchema(schema), registry)
	fields := make([]*SchemaField, len(v[schemaFieldsField].([]interface{})))
	for i := range fields {
		field, err := parseSchemaField(v[schemaFieldsField].([]interface{})[i], registry, namespace)
		if err != nil {
			return nil, err
		}
		fields[i] = field
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
		fieldType, err := schemaByType(v[schemaTypeField], registry, namespace)
		if err != nil {
			return nil, err
		}
		err = setOptionalStringListField(&schemaField.Aliases, v, schemaAliasesField)
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

func setOptionalStringListField(where *[]string, v map[string]interface{}, fieldName string) error {
	if field, exists := v[fieldName]; exists {
		if boxedList, ok := field.([]interface{}); ok {
			var stringList = make([]string, len(boxedList))
			for i := range boxedList {
				if stringList[i], ok = boxedList[i].(string); !ok {
					return fmt.Errorf("Bad '%s' entry %#v", fieldName, boxedList[i])
				}
			}
			field = stringList
		}
		if stringList, ok := field.([]string); ok {
			*where = stringList
		}
	}
	return nil
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
		schemaLogicalTypeField, schemaPrecisionField, schemaScaleField,
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
