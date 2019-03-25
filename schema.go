package avro

import (
	"crypto/sha256"
	"encoding/json"
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

type Fingerprint [32]byte

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

	// Canonical Schema
	Canonical() (*CanonicalSchema, error)

	// Returns a pre-computed or cached fingerprint
	Fingerprint() (*Fingerprint, error)
}

// StringSchema implements Schema and represents Avro string type.
type StringSchema struct{}

// Returns a pre-computed or cached fingerprint
func (*StringSchema) Fingerprint() (*Fingerprint, error) {
	return &Fingerprint{
		233, 229, 193, 201, 228, 246, 39, 115, 57, 209, 188, 222, 7, 51, 165, 155,
		212, 47, 135, 49, 244, 73, 218, 109, 193, 48, 16, 169, 22, 147, 13, 72,
	}, nil
}

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

// Canonical Schema
func (s *StringSchema) Canonical() (*CanonicalSchema, error) {
	return &CanonicalSchema{Type: "string"}, nil
}

// Standard JSON representation
func (*StringSchema) MarshalJSON() ([]byte, error) {
	return []byte(`"string"`), nil
}

// BytesSchema implements Schema and represents Avro bytes type.
type BytesSchema struct{}

// Returns a pre-computed or cached fingerprint
func (*BytesSchema) Fingerprint() (*Fingerprint, error) {
	return &Fingerprint{
		154, 229, 7, 169, 221, 57, 238, 91, 124, 126, 40, 93, 162, 192, 132, 101,
		33, 200, 174, 141, 128, 254, 234, 229, 80, 78, 12, 152, 29, 83, 245, 250,
	}, nil
}

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

// Canonical Schema
func (s *BytesSchema) Canonical() (*CanonicalSchema, error) {
	return &CanonicalSchema{Type: "bytes"}, nil
}

// Standard JSON representation
func (*BytesSchema) MarshalJSON() ([]byte, error) {
	return []byte(`"bytes"`), nil
}

// IntSchema implements Schema and represents Avro int type.
type IntSchema struct{}

// Returns a pre-computed or cached fingerprint
func (*IntSchema) Fingerprint() (*Fingerprint, error) {
	return &Fingerprint{
		63, 43, 135, 169, 254, 124, 201, 177, 56, 53, 89, 140, 57, 129, 205, 69,
		227, 227, 85, 48, 158, 80, 144, 170, 9, 51, 215, 190, 203, 111, 186, 69,
	}, nil
}

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

// Canonical Schema
func (s *IntSchema) Canonical() (*CanonicalSchema, error) {
	return &CanonicalSchema{Type: "int"}, nil
}

// Standard JSON representation
func (*IntSchema) MarshalJSON() ([]byte, error) {
	return []byte(`"int"`), nil
}

// LongSchema implements Schema and represents Avro long type.
type LongSchema struct{}

// Returns a pre-computed or cached fingerprint
func (*LongSchema) Fingerprint() (*Fingerprint, error) {
	return &Fingerprint{
		195, 44, 73, 125, 246, 115, 12, 151, 250, 7, 54, 42, 165, 2, 63, 55,
		212, 154, 2, 126, 196, 82, 54, 7, 120, 17, 76, 244, 39, 150, 90, 221,
	}, nil
}

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

// Canonical Schema
func (s *LongSchema) Canonical() (*CanonicalSchema, error) {
	return &CanonicalSchema{Type: "long"}, nil
}

// Standard JSON representation
func (*LongSchema) MarshalJSON() ([]byte, error) {
	return []byte(`"long"`), nil
}

// FloatSchema implements Schema and represents Avro float type.
type FloatSchema struct{}

// Returns a pre-computed or cached fingerprint
func (*FloatSchema) Fingerprint() (*Fingerprint, error) {
	return &Fingerprint{
		30, 113, 249, 236, 5, 29, 102, 63, 86, 176, 216, 225, 252, 132, 215, 26,
		165, 108, 207, 233, 250, 147, 170, 32, 209, 5, 71, 167, 171, 235, 92, 192,
	}, nil
}

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

// Canonical Schema
func (s *FloatSchema) Canonical() (*CanonicalSchema, error) {
	return &CanonicalSchema{Type: "float"}, nil
}

// Standard JSON representation
func (*FloatSchema) MarshalJSON() ([]byte, error) {
	return []byte(`"float"`), nil
}

// DoubleSchema implements Schema and represents Avro double type.
type DoubleSchema struct{}

// Returns a pre-computed or cached fingerprint
func (*DoubleSchema) Fingerprint() (*Fingerprint, error) {
	return &Fingerprint{
		115, 10, 154, 140, 97, 22, 129, 215, 238, 244, 66, 224, 60, 22, 199, 13,
		19, 188, 163, 235, 139, 151, 123, 180, 3, 234, 255, 82, 23, 106, 242, 84,
	}, nil
}

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

// Canonical Schema
func (s *DoubleSchema) Canonical() (*CanonicalSchema, error) {
	return &CanonicalSchema{Type: "double"}, nil
}

// Standard JSON representation
func (*DoubleSchema) MarshalJSON() ([]byte, error) {
	return []byte(`"double"`), nil
}

// BooleanSchema implements Schema and represents Avro boolean type.
type BooleanSchema struct{}

// Returns a pre-computed or cached fingerprint
func (*BooleanSchema) Fingerprint() (*Fingerprint, error) {
	return &Fingerprint{
		165, 176, 49, 171, 98, 188, 65, 109, 114, 12, 4, 16, 216, 2, 234, 70,
		185, 16, 196, 251, 232, 92, 80, 169, 70, 204, 198, 88, 183, 78, 103, 126,
	}, nil
}

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

// Canonical Schema
func (s *BooleanSchema) Canonical() (*CanonicalSchema, error) {
	return &CanonicalSchema{Type: "boolean"}, nil
}

// Standard JSON representation
func (*BooleanSchema) MarshalJSON() ([]byte, error) {
	return []byte(`"boolean"`), nil
}

// NullSchema implements Schema and represents Avro null type.
type NullSchema struct{}

// Returns a pre-computed or cached fingerprint
func (*NullSchema) Fingerprint() (*Fingerprint, error) {
	return &Fingerprint{
		240, 114, 203, 236, 59, 248, 132, 24, 113, 212, 40, 66, 48, 197, 233, 131,
		220, 33, 26, 86, 131, 122, 237, 134, 36, 135, 20, 143, 148, 125, 26, 31,
	}, nil
}

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

// Canonical Schema
func (s *NullSchema) Canonical() (*CanonicalSchema, error) {
	return &CanonicalSchema{Type: "null"}, nil
}

// Standard JSON representation
func (*NullSchema) MarshalJSON() ([]byte, error) {
	return []byte(`"null"`), nil
}

// RecordSchema implements Schema and represents Avro record type.
type RecordSchema struct {
	Name        string   `json:"name,omitempty"`
	Namespace   string   `json:"namespace,omitempty"`
	Doc         string   `json:"doc,omitempty"`
	Aliases     []string `json:"aliases,omitempty"`
	Properties  map[string]interface{}
	Fields      []*SchemaField `json:"fields"`
	fingerprint *Fingerprint
}

// Returns a pre-computed or cached fingerprint
func (s *RecordSchema) Fingerprint() (*Fingerprint, error) {
	if s.fingerprint == nil {
		if f, err := calculateSchemaFingerprint(s); err != nil {
			return nil, err
		} else {
			s.fingerprint = f
		}
	}
	return s.fingerprint, nil
}

// String returns a JSON representation of RecordSchema.
func (s *RecordSchema) String() string {
	bytes, err := json.MarshalIndent(s, "", "    ")
	if err != nil {
		panic(err)
	}

	return string(bytes)
}

// Canonical Schema
func (s *RecordSchema) Canonical() (*CanonicalSchema, error) {
	fields := make([]*CanonicalSchemaField, len(s.Fields))
	for i, f := range s.Fields {
		if fc, err := f.Type.Canonical(); err != nil {
			return nil, err
		} else {
			fields[i] = &CanonicalSchemaField{
				Name: f.Name,
				Type: fc,
			}
		}
	}
	return &CanonicalSchema{Type: "record", Name: GetFullName(s), Fields: fields}, nil
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
	Actual *RecordSchema
}

// Returns a pre-computed or cached fingerprint
func (s *RecursiveSchema) Fingerprint() (*Fingerprint, error) {
	return s.Actual.Fingerprint()
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

// Canonical JSON representation
func (s *RecursiveSchema) Canonical() (*CanonicalSchema, error) {
	return s.Actual.Canonical()
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
	Type       Schema      `json:"type,omitempty"`
	Properties map[string]interface{}
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
			Name    string      `json:"name,omitempty"`
			Doc     string      `json:"doc,omitempty"`
			Default interface{} `json:"default"`
			Type    Schema      `json:"type,omitempty"`
		}{
			Name:    s.Name,
			Doc:     s.Doc,
			Default: s.Default,
			Type:    s.Type,
		})
	}

	return json.Marshal(struct {
		Name    string      `json:"name,omitempty"`
		Doc     string      `json:"doc,omitempty"`
		Default interface{} `json:"default,omitempty"`
		Type    Schema      `json:"type,omitempty"`
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
	Name        string
	Namespace   string
	Aliases     []string
	Doc         string
	Symbols     []string
	Properties  map[string]interface{}
	fingerprint *Fingerprint
}

// Returns a pre-computed or cached fingerprint
func (s *EnumSchema) Fingerprint() (*Fingerprint, error) {
	if s.fingerprint == nil {
		if f, err := calculateSchemaFingerprint(s); err != nil {
			return nil, err
		} else {
			s.fingerprint = f
		}
	}
	return s.fingerprint, nil
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

// Canonical representation
func (s *EnumSchema) Canonical() (*CanonicalSchema, error) {
	return &CanonicalSchema{Name: GetFullName(s), Type: "enum", Symbols: s.Symbols}, nil
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
	Items       Schema
	Properties  map[string]interface{}
	fingerprint *Fingerprint
}

// Returns a pre-computed or cached fingerprint
func (s *ArraySchema) Fingerprint() (*Fingerprint, error) {
	if s.fingerprint == nil {
		if f, err := calculateSchemaFingerprint(s); err != nil {
			return nil, err
		} else {
			s.fingerprint = f
		}
	}
	return s.fingerprint, nil
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

// Canonical representation
func (s *ArraySchema) Canonical() (*CanonicalSchema, error) {
	if ic, err := s.Items.Canonical(); err != nil {
		return nil, err
	} else {
		return &CanonicalSchema{Type: "array", Items: ic}, nil
	}
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
	Values      Schema
	Properties  map[string]interface{}
	fingerprint *Fingerprint
}

// Returns a pre-computed or cached fingerprint
func (s *MapSchema) Fingerprint() (*Fingerprint, error) {
	if s.fingerprint == nil {
		if f, err := calculateSchemaFingerprint(s); err != nil {
			return nil, err
		} else {
			s.fingerprint = f
		}
	}
	return s.fingerprint, nil
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

// Canonical representation
func (s *MapSchema) Canonical() (*CanonicalSchema, error) {
	if vc, err := s.Values.Canonical(); err != nil {
		return nil, err
	} else {
		return &CanonicalSchema{Type: "array", Values: vc}, nil
	}
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
	Types       []Schema
	fingerprint *Fingerprint
}

// Returns a pre-computed or cached fingerprint
func (s *UnionSchema) Fingerprint() (*Fingerprint, error) {
	if s.fingerprint == nil {
		if f, err := calculateSchemaFingerprint(s); err != nil {
			return nil, err
		} else {
			s.fingerprint = f
		}
	}
	return s.fingerprint, nil
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

// Canonical representation
func (s *UnionSchema) Canonical() (*CanonicalSchema, error) {
	ct := make([]*CanonicalSchema, len(s.Types))
	for i, t := range s.Types {
		if c, err := t.Canonical(); err != nil {
			return nil, err
		} else {
			ct[i] = c
		}
	}
	return &CanonicalSchema{Type: "union", Types: ct}, nil
}

// MarshalJSON serializes the given schema as JSON.
func (s *UnionSchema) MarshalJSON() ([]byte, error) {
	return json.Marshal(s.Types)
}

// FixedSchema implements Schema and represents Avro fixed type.
type FixedSchema struct {
	Namespace   string
	Name        string
	Size        int
	Properties  map[string]interface{}
	fingerprint *Fingerprint
}

// Returns a pre-computed or cached fingerprint
func (s *FixedSchema) Fingerprint() (*Fingerprint, error) {
	if s.fingerprint == nil {
		if f, err := calculateSchemaFingerprint(s); err != nil {
			return nil, err
		} else {
			s.fingerprint = f
		}
	}
	return s.fingerprint, nil
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

// Canonical representation
func (s *FixedSchema) Canonical() (*CanonicalSchema, error) {
	return &CanonicalSchema{
		Type: "fixed",
		Name: GetFullName(s),
		Size: s.Size,
	}, nil
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

func calculateSchemaFingerprint(s Schema) (*Fingerprint, error) {
	if canonical, err := s.Canonical(); err != nil {
		return nil, err
	} else if bytes, err := canonical.MarshalJSON(); err != nil {
		return nil, err
	} else {
		f := Fingerprint(sha256.Sum256(bytes))
		return &f, nil
	}
}

type CanonicalSchema struct {
	Name    string                  `json:"name,omitempty"`
	Type    string                  `json:"type,omitempty"`
	Fields  []*CanonicalSchemaField `json:"fields,omitempty"`
	Symbols []string                `json:"symbols,omitempty"`
	Items   *CanonicalSchema        `json:"items,omitempty"`
	Values  *CanonicalSchema        `json:"values,omitempty"`
	Size    int                     `json:"size,omitempty"`
	Types   []*CanonicalSchema      `json:"size,omit"`
}

type CanonicalSchemaField struct {
	Name string           `json:"name,omitempty"`
	Type *CanonicalSchema `json:"type,omitempty"`
}

func (c *CanonicalSchema) MarshalJSON() ([]byte, error) {
	switch c.Type {
	case "string", "bytes", "int", "long", "float", "double", "boolean", "null":
		return []byte("\"" + c.Type + "\""), nil
	case "union":
		return json.Marshal(c.Types)
	default:
		return json.Marshal(struct{
			Name    string                  `json:"name,omitempty"`
			Type    string                  `json:"type,omitempty"`
			Fields  []*CanonicalSchemaField `json:"fields,omitempty"`
			Symbols []string                `json:"symbols,omitempty"`
			Items   *CanonicalSchema        `json:"items,omitempty"`
			Values  *CanonicalSchema        `json:"values,omitempty"`
			Size    int                     `json:"size,omitempty"`
		}{
			c.Name,
			c.Type,
			c.Fields,
			c.Symbols,
			c.Items,
			c.Values,
			c.Size,
		})
	}

}
