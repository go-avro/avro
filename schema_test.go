package avro

import (
	"encoding/json"
	"fmt"
	"testing"
)

func TestPrimitiveSchema(t *testing.T) {
	primitiveSchemaAssert(t, "string", String, "STRING")
	primitiveSchemaAssert(t, "int", Int, "INT")
	primitiveSchemaAssert(t, "long", Long, "LONG")
	primitiveSchemaAssert(t, "boolean", Boolean, "BOOLEAN")
	primitiveSchemaAssert(t, "float", Float, "FLOAT")
	primitiveSchemaAssert(t, "double", Double, "DOUBLE")
	primitiveSchemaAssert(t, "bytes", Bytes, "BYTES")
	primitiveSchemaAssert(t, "null", Null, "NULL")
}

func primitiveSchemaAssert(t *testing.T, raw string, expected int, typeName string) {
	s, err := ParseSchema(raw)
	assert(t, err, nil)

	if s.Type() != expected {
		t.Errorf("\n%s \n===\n Should parse into Type() = %s", raw, typeName)
	}
}

func TestArraySchema(t *testing.T) {
	//array of strings
	raw := `{"type":"array", "items": "string"}`
	s, err := ParseSchema(raw)
	assert(t, err, nil)
	if s.Type() != Array {
		t.Errorf("\n%s \n===\n Should parse into Type() = %s", raw, "ARRAY")
	}
	if s.(*ArraySchema).Items.Type() != String {
		t.Errorf("\n%s \n===\n Array item type should be STRING", raw)
	}

	//array of longs
	raw = `{"type":"array", "items": "long"}`
	s, err = ParseSchema(raw)
	assert(t, err, nil)
	if s.Type() != Array {
		t.Errorf("\n%s \n===\n Should parse into Type() = %s", raw, "ARRAY")
	}
	if s.(*ArraySchema).Items.Type() != Long {
		t.Errorf("\n%s \n===\n Array item type should be LONG", raw)
	}

	//array of arrays of strings
	raw = `{"type":"array", "items": {"type":"array", "items": "string"}}`
	s, err = ParseSchema(raw)
	assert(t, err, nil)
	if s.Type() != Array {
		t.Errorf("\n%s \n===\n Should parse into Type() = %s", raw, "ARRAY")
	}
	if s.(*ArraySchema).Items.Type() != Array {
		t.Errorf("\n%s \n===\n Array item type should be ARRAY", raw)
	}
	if s.(*ArraySchema).Items.(*ArraySchema).Items.Type() != String {
		t.Errorf("\n%s \n===\n Array's nested item type should be STRING", raw)
	}

	raw = `{"type":"array", "items": {"type": "record", "name": "TestRecord", "fields": [
	{"name": "longRecordField", "type": "long"},
	{"name": "floatRecordField", "type": "float"}
	]}}`
	s, err = ParseSchema(raw)
	assert(t, err, nil)
	if s.Type() != Array {
		t.Errorf("\n%s \n===\n Should parse into Type() = %s", raw, "ARRAY")
	}
	if s.(*ArraySchema).Items.Type() != Record {
		t.Errorf("\n%s \n===\n Array item type should be RECORD", raw)
	}
	if s.(*ArraySchema).Items.(*RecordSchema).Fields[0].Type.Type() != Long {
		t.Errorf("\n%s \n===\n Array's nested record first field type should be LONG", raw)
	}
	if s.(*ArraySchema).Items.(*RecordSchema).Fields[1].Type.Type() != Float {
		t.Errorf("\n%s \n===\n Array's nested record first field type should be FLOAT", raw)
	}
}

func TestMapSchema(t *testing.T) {
	//map[string, int]
	raw := `{"type":"map", "values": "int"}`
	s, err := ParseSchema(raw)
	assert(t, err, nil)
	if s.Type() != Map {
		t.Errorf("\n%s \n===\n Should parse into MapSchema. Actual %#v", raw, s)
	}
	if s.(*MapSchema).Values.Type() != Int {
		t.Errorf("\n%s \n===\n Map value type should be Int. Actual %#v", raw, s.(*MapSchema).Values)
	}

	//map[string, []string]
	raw = `{"type":"map", "values": {"type":"array", "items": "string"}}`
	s, err = ParseSchema(raw)
	assert(t, err, nil)
	if s.Type() != Map {
		t.Errorf("\n%s \n===\n Should parse into MapSchema. Actual %#v", raw, s)
	}
	if s.(*MapSchema).Values.Type() != Array {
		t.Errorf("\n%s \n===\n Map value type should be Array. Actual %#v", raw, s.(*MapSchema).Values)
	}
	if s.(*MapSchema).Values.(*ArraySchema).Items.Type() != String {
		t.Errorf("\n%s \n===\n Map nested array item type should be String. Actual %#v", raw, s.(*MapSchema).Values.(*ArraySchema).Items)
	}

	//map[string, [int, string]]
	raw = `{"type":"map", "values": ["int", "string"]}`
	s, err = ParseSchema(raw)
	assert(t, err, nil)
	if s.Type() != Map {
		t.Errorf("\n%s \n===\n Should parse into MapSchema. Actual %#v", raw, s)
	}
	if s.(*MapSchema).Values.Type() != Union {
		t.Errorf("\n%s \n===\n Map value type should be Union. Actual %#v", raw, s.(*MapSchema).Values)
	}
	if s.(*MapSchema).Values.(*UnionSchema).Types[0].Type() != Int {
		t.Errorf("\n%s \n===\n Map nested union's first type should be Int. Actual %#v", raw, s.(*MapSchema).Values.(*UnionSchema).Types[0])
	}
	if s.(*MapSchema).Values.(*UnionSchema).Types[1].Type() != String {
		t.Errorf("\n%s \n===\n Map nested union's second type should be String. Actual %#v", raw, s.(*MapSchema).Values.(*UnionSchema).Types[1])
	}

	//map[string, record]
	raw = `{"type":"map", "values": {"type": "record", "name": "TestRecord2", "fields": [
	{"name": "doubleRecordField", "type": "double"},
	{"name": "fixedRecordField", "type": {"type": "fixed", "size": 4, "name": "bytez"}}
	]}}`
	s, err = ParseSchema(raw)
	assert(t, err, nil)
	if s.Type() != Map {
		t.Errorf("\n%s \n===\n Should parse into MapSchema. Actual %#v", raw, s)
	}
	if s.(*MapSchema).Values.Type() != Record {
		t.Errorf("\n%s \n===\n Map value type should be Record. Actual %#v", raw, s.(*MapSchema).Values)
	}
	if s.(*MapSchema).Values.(*RecordSchema).Fields[0].Type.Type() != Double {
		t.Errorf("\n%s \n===\n Map value's record first field should be Double. Actual %#v", raw, s.(*MapSchema).Values.(*RecordSchema).Fields[0].Type)
	}
	if s.(*MapSchema).Values.(*RecordSchema).Fields[1].Type.Type() != Fixed {
		t.Errorf("\n%s \n===\n Map value's record first field should be Fixed. Actual %#v", raw, s.(*MapSchema).Values.(*RecordSchema).Fields[1].Type)
	}
}

func TestRecordSchema(t *testing.T) {
	raw := `{"type": "record", "name": "TestRecord", "fields": [
     	{"name": "longRecordField", "type": "long"},
     	{"name": "stringRecordField", "type": "string"},
     	{"name": "intRecordField", "type": "int"},
     	{"name": "floatRecordField", "type": "float"}
     ]}`
	s, err := ParseSchema(raw)
	assert(t, err, nil)
	if s.Type() != Record {
		t.Errorf("\n%s \n===\n Should parse into RecordSchema. Actual %#v", raw, s)
	}
	if s.(*RecordSchema).Fields[0].Type.Type() != Long {
		t.Errorf("\n%s \n===\n Record's first field type should parse into LongSchema. Actual %#v", raw, s.(*RecordSchema).Fields[0].Type)
	}
	if s.(*RecordSchema).Fields[1].Type.Type() != String {
		t.Errorf("\n%s \n===\n Record's second field type should parse into StringSchema. Actual %#v", raw, s.(*RecordSchema).Fields[1].Type)
	}
	if s.(*RecordSchema).Fields[2].Type.Type() != Int {
		t.Errorf("\n%s \n===\n Record's third field type should parse into IntSchema. Actual %#v", raw, s.(*RecordSchema).Fields[2].Type)
	}
	if s.(*RecordSchema).Fields[3].Type.Type() != Float {
		t.Errorf("\n%s \n===\n Record's fourth field type should parse into FloatSchema. Actual %#v", raw, s.(*RecordSchema).Fields[3].Type)
	}

	raw = `{"namespace": "scalago",
	"type": "record",
	"name": "PingPong",
	"fields": [
	{"name": "counter", "type": "long"},
	{"name": "name", "type": "string"}
	]}`
	s, err = ParseSchema(raw)
	assert(t, err, nil)
	if s.Type() != Record {
		t.Errorf("\n%s \n===\n Should parse into RecordSchema. Actual %#v", raw, s)
	}
	if s.(*RecordSchema).Name != "PingPong" {
		t.Errorf("\n%s \n===\n Record's name should be PingPong. Actual %#v", raw, s.(*RecordSchema).Name)
	}
	f0 := s.(*RecordSchema).Fields[0]
	if f0.Name != "counter" {
		t.Errorf("\n%s \n===\n Record's first field name should be 'counter'. Actual %#v", raw, f0.Name)
	}
	if f0.Type.Type() != Long {
		t.Errorf("\n%s \n===\n Record's first field type should parse into LongSchema. Actual %#v", raw, f0.Type)
	}
	f1 := s.(*RecordSchema).Fields[1]
	if f1.Name != "name" {
		t.Errorf("\n%s \n===\n Record's first field name should be 'counter'. Actual %#v", raw, f0.Name)
	}
	if f1.Type.Type() != String {
		t.Errorf("\n%s \n===\n Record's second field type should parse into StringSchema. Actual %#v", raw, f1.Type)
	}
}

func TestEnumSchema(t *testing.T) {
	raw := `{"type":"enum", "name":"foo", "symbols":["A", "B", "C", "D"]}`
	s, err := ParseSchema(raw)
	assert(t, err, nil)
	if s.Type() != Enum {
		t.Errorf("\n%s \n===\n Should parse into EnumSchema. Actual %#v", raw, s)
	}
	if s.(*EnumSchema).Name != "foo" {
		t.Errorf("\n%s \n===\n Enum name should be 'foo'. Actual %#v", raw, s.(*EnumSchema).Name)
	}
	if !arrayEqual(s.(*EnumSchema).Symbols, []string{"A", "B", "C", "D"}) {
		t.Errorf("\n%s \n===\n Enum symbols should be [\"A\", \"B\", \"C\", \"D\"]. Actual %#v", raw, s.(*EnumSchema).Symbols)
	}
}

func TestUnionSchema(t *testing.T) {
	raw := `["null", "string"]`
	s, err := ParseSchema(raw)
	assert(t, err, nil)
	if s.Type() != Union {
		t.Errorf("\n%s \n===\n Should parse into UnionSchema. Actual %#v", raw, s)
	}
	if s.(*UnionSchema).Types[0].Type() != Null {
		t.Errorf("\n%s \n===\n Union's first type should be Null. Actual %#v", raw, s.(*UnionSchema).Types[0])
	}
	if s.(*UnionSchema).Types[1].Type() != String {
		t.Errorf("\n%s \n===\n Union's second type should be String. Actual %#v", raw, s.(*UnionSchema).Types[1])
	}

	raw = `["string", "null"]`
	s, err = ParseSchema(raw)
	assert(t, err, nil)
	if s.Type() != Union {
		t.Errorf("\n%s \n===\n Should parse into UnionSchema. Actual %#v", raw, s)
	}
	if s.(*UnionSchema).Types[0].Type() != String {
		t.Errorf("\n%s \n===\n Union's first type should be String. Actual %#v", raw, s.(*UnionSchema).Types[0])
	}
	if s.(*UnionSchema).Types[1].Type() != Null {
		t.Errorf("\n%s \n===\n Union's second type should be Null. Actual %#v", raw, s.(*UnionSchema).Types[1])
	}
}

func TestFixedSchema(t *testing.T) {
	raw := `{"type": "fixed", "size": 16, "name": "md5"}`
	s, err := ParseSchema(raw)
	assert(t, err, nil)
	if s.Type() != Fixed {
		t.Errorf("\n%s \n===\n Should parse into FixedSchema. Actual %#v", raw, s)
	}
	if s.(*FixedSchema).Size != 16 {
		t.Errorf("\n%s \n===\n Fixed size should be 16. Actual %#v", raw, s.(*FixedSchema).Size)
	}
	if s.(*FixedSchema).Name != "md5" {
		t.Errorf("\n%s \n===\n Fixed name should be md5. Actual %#v", raw, s.(*FixedSchema).Name)
	}
}

func TestSchemaRegistryMap(t *testing.T) {
	rawSchema1 := `{"type": "record", "name": "TestRecord", "namespace": "com.github.elodina", "fields": [
		{"name": "longRecordField", "type": "long"}
	]}`

	rawSchema2 := `{"type": "record", "name": "TestRecord2", "namespace": "com.github.elodina", "fields": [
		{"name": "record", "type": ["null", "TestRecord"]}
	]}`

	rawSchema3 := `{"type": "record", "name": "TestRecord3", "namespace": "com.github.other", "fields": [
		{"name": "record", "type": ["null", "com.github.elodina.TestRecord2"]}
	]}`

	rawSchema4 := `{"type": "record", "name": "TestRecord3", "namespace": "com.github.elodina", "fields": [
		{"name": "record", "type": ["null", {"type": "TestRecord2"}, "com.github.other.TestRecord3"]}
	]}`

	registry := make(map[string]Schema)

	s1, err := ParseSchemaWithRegistry(rawSchema1, registry)
	assert(t, err, nil)
	assert(t, s1.Type(), Record)
	assert(t, len(registry), 1)

	s2, err := ParseSchemaWithRegistry(rawSchema2, registry)
	assert(t, err, nil)
	assert(t, s2.Type(), Record)
	assert(t, len(registry), 2)

	s3, err := ParseSchemaWithRegistry(rawSchema3, registry)
	assert(t, err, nil)
	assert(t, s3.Type(), Record)
	assert(t, len(registry), 3)

	s4, err := ParseSchemaWithRegistry(rawSchema4, registry)
	assert(t, err, nil)
	assert(t, s4.Type(), Record)
	assert(t, len(registry), 4)
}

func TestRecordCustomProps(t *testing.T) {
	raw := `{"type": "record", "name": "TestRecord", "hello": "world", "fields": [
     	{"name": "longRecordField", "type": "long"},
     	{"name": "stringRecordField", "type": "string"},
     	{"name": "intRecordField", "type": "int"},
     	{"name": "floatRecordField", "type": "float"}
     ]}`
	s, err := ParseSchema(raw)
	assert(t, err, nil)
	assert(t, len(s.(*RecordSchema).Properties), 1)

	value, exists := s.Prop("hello")
	assert(t, exists, true)
	assert(t, value, "world")
}

func TestLoadSchemas(t *testing.T) {
	schemas := LoadSchemas("test/schemas/")
	assert(t, len(schemas), 4)

	_, exists := schemas["example.avro.Complex"]
	assert(t, exists, true)
	_, exists = schemas["example.avro.foo"]
	assert(t, exists, true)
}

func TestSchemaEquality(t *testing.T) {

	s0, _ := ParseSchema(`{"type": "record", "name": "TestRecord", "namespace": "xyz", "hello": "world", "fields": [
		{"name": "field1", "type": "long"},
		{"name": "field2", "type": "string", "doc": "hello world"}
	]}`)

	s1, _ := ParseSchema(`{"type": "record", "name": "TestRecord", "namespace": "xyz", "hello": "world", "fields": [
		{"name": "field1", "type": "long"},
		{"name": "field2", "type": "string", "doc": "hello"}
	]}`)
	s2, _ := ParseSchema(`{"type": "record", "name": "TestRecord", "hello": "world", "fields": [
		{"name": "field1", "type": "long", "aliases": ["f1"] },
		{"name": "field2", "type": "string", "doc": "hello"}
	]}`)
	//s3, _ := ParseSchema(`{"type": "record", "name": "TestRecord", "hello": "world", "fields": [
	//	{"name": "field1", "type": "long", "aliases": ["f1"] },{"name": "field2", "doc": "hello", "type": "string"}
	//]}`)

	s_enum1, _ := ParseSchema(`{"type":"enum", "name":"foo", "symbols":["A", "B", "C", "D"], "doc": "hello"}`)
	s_enum2, _ := ParseSchema(`{"type":"enum", "name":"foo", "symbols":["D", "C", "B", "A"]}`)
	s_fixed1, _ := ParseSchema(`{"type": "fixed", "size": 16, "name": "md5"}`)
	s_fixed2, _ := ParseSchema(`{"type": "fixed", "size": 32, "name": "md5"}`)
	s_fixedSame, _ := ParseSchema(`{"type": "fixed", "size": 16, "name": "md5", "doc": "xyz"}`)
	f1, _ := s_fixed1.Fingerprint()
	f2, _ := s_fixedSame.Fingerprint()
	assert(t, f1, f2)
	s_array1, _ := ParseSchema(`{"type":"array", "items": "string"}`)
	s_array2, _ := ParseSchema(`{"type":"array", "items": "long"}`)
	s_map1, _ := ParseSchema(`{"type":"map", "values": "float"}`)
	s_map2, _ := ParseSchema(`{"type":"map", "values": "double"}`)
	s_union1, _ := ParseSchema(`["null", "string"]`)
	s_union2, _ := ParseSchema(`["string", "null"]`)
	s_union3, _ := ParseSchema(`["string", "int", "float"]`)

	f3, _ := s0.Fingerprint()
	f4, _ := s1.Fingerprint()
	assert(t, f3, f4)

	normal, _ := json.Marshal(s_enum1)
	assert(t, string(normal), `{"type":"enum","name":"foo","doc":"hello","symbols":["A","B","C","D"]}`)
	canonical, _ := s_enum1.Canonical()
	c, _ := canonical.MarshalJSON()
	//doc is stripped from canonical
	assert(t, string(c),`{"name":"foo","type":"enum","symbols":["A","B","C","D"]}` )


	schemas := []Schema{
		s1, s2,
		s_enum1, s_enum2,
		s_fixed1, s_fixed2,
		s_array1, s_array2,
		s_map1, s_map2,
		s_union1, s_union2, s_union3,
		new(StringSchema),
		new(BytesSchema),
		new(IntSchema),
		new(LongSchema),
		new(FloatSchema),
		new(DoubleSchema),
		new(BooleanSchema),
		new(NullSchema),
	}
	for i := range schemas {
		for y := range schemas {
			f1, _ := schemas[i].Fingerprint()
			f2, _ := schemas[y].Fingerprint()
			if y == i {
				assert(t, f1, f2)
			} else if f1 == f2 {
				panic(fmt.Errorf("different schemas have same fingerprint: %q and %q",
					schemas[i].GetName(), schemas[y].GetName()))
			}
		}
	}

	//assert(t, s1.Fingerprint(), newRecursiveSchema(s1.(*RecordSchema)).Fingerprint())
	//assert(t, s2.Fingerprint(), newRecursiveSchema(s2.(*RecordSchema)).Fingerprint())
	//assert(t, s3.Fingerprint(), newRecursiveSchema(s3.(*RecordSchema)).Fingerprint())

	////benchmarks
	//f1 := calculateSchemaFingerprint(s1)
	//f2 := calculateSchemaFingerprint(s2)
	//f3 := calculateSchemaFingerprint(s3)
	//fmt.Println("reflet.DeepEqual Benchmark", testing.Benchmark(func(b *testing.B) {
	//	for i := 0; i < b.N; i++ {
	//		reflect.DeepEqual(s1, s2)
	//		reflect.DeepEqual(s2, s3)
	//	}
	//}))
	//fmt.Println("Fingerprint Comparsion Benchmark", testing.Benchmark(func(b *testing.B) {
	//	for i := 0; i < b.N; i++ {
	//		if f1 == f2 {
	//			panic("1")
	//		}
	//		if f2 != f3 {
	//			panic("2")
	//		}
	//	}
	//}))

}

func arrayEqual(arr1 []string, arr2 []string) bool {
	if len(arr1) != len(arr2) {
		return false
	}

	for i := range arr1 {
		if arr1[i] != arr2[i] {
			return false
		}
	}
	return true
}
