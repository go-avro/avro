package avro

import (
	"bytes"
	"log"
	"reflect"
	"testing"
)

func testGeneric(record *GenericRecord) {
	var buf bytes.Buffer
	w := NewGenericDatumWriter().SetSchema(record.Schema())
	if err := w.Write(record, NewBinaryEncoder(&buf)); err != nil {
		panic(err)
	}

	r := NewGenericDatumReader().SetSchema(record.Schema())
	decodedRecord := NewGenericRecord(record.Schema())
	if err := r.Read(decodedRecord, NewBinaryDecoder(buf.Bytes())); err != nil {
		panic(err)
	}

	if record.String() != decodedRecord.String() {
		panic("record compare failed")
	}
}

func testSpecific(record interface{}, schema Schema) {
	var buf bytes.Buffer
	w := NewSpecificDatumWriter().SetSchema(schema)
	if err := w.Write(record, NewBinaryEncoder(&buf)); err != nil {
		panic(err)
	}

	r := NewSpecificDatumReader().SetSchema(schema)
	decodedRecord := reflect.New(reflect.TypeOf(record).Elem()).Interface()
	if err := r.Read(decodedRecord, NewBinaryDecoder(buf.Bytes())); err != nil {
		panic(err)
	}

	if !reflect.DeepEqual(record, decodedRecord) {
		panic("record compare failed")
	}
}

func TestUnionAsOption(t *testing.T) {
	nestedSchema := MustParseSchema(`{ 
					"name": "Nest", 
					"type": "record", 
					"fields": [ 
						{ "name": "id", "type": "int" } 
					] 
				}`)

	schema := MustParseSchema(`{
	    "type": "record",
	    "name": "Rec",
	    "fields": [
	        { "name": "opt_bool", "type": ["null", "boolean"] },
	        { "name": "opt_int", "type": ["null", "int"] },
	        { "name": "opt_long", "type": ["null", "long"] },
	        { "name": "opt_float", "type": ["null", "float"] },
			{ "name": "opt_double", "type": ["null", "double"] },
	        { "name": "opt_bytes", "type": ["null", "bytes"] },
	        { "name": "opt_string", "type": ["null", "string"] },
			{ "name": "opt_fixed", "type": ["null", { "name": "fixed6", "type": "fixed", "size": 5 } ] },
			{ "name": "opt_array", "type": ["null", { "type": "array", "items": "string"}] },
			{ "name": "opt_map", "type": ["null", { "type": "map", "values": "string"}] },
			{ "name": "opt_record", "type": [ "null", { 
					"name": "Nest", 
					"type": "record", 
					"fields": [ 
						{ "name": "id", "type": "int" } 
					] 
				}
			] }
	    ]
	}`)

	emptyGenericRecord := NewGenericRecord(schema)
	emptyGenericRecord.Set("opt_bool", nil)
	emptyGenericRecord.Set("opt_int", nil)
	emptyGenericRecord.Set("opt_long", nil)
	emptyGenericRecord.Set("opt_float", nil)
	emptyGenericRecord.Set("opt_double", nil)
	emptyGenericRecord.Set("opt_bytes", nil)
	emptyGenericRecord.Set("opt_string", nil)
	emptyGenericRecord.Set("opt_fixed", nil)
	emptyGenericRecord.Set("opt_array", nil)
	emptyGenericRecord.Set("opt_map", nil)
	emptyGenericRecord.Set("opt_record", nil)
	testGeneric(emptyGenericRecord)

	genericRecord := NewGenericRecord(schema)
	optBool := true
	genericRecord.Set("opt_bool", &optBool)
	optInt := int32(1)
	genericRecord.Set("opt_int", &optInt)
	optLong := int64(1)
	genericRecord.Set("opt_long", &optLong)
	optFloat := float32(1)
	genericRecord.Set("opt_float", &optFloat)
	optDouble := float64(1)
	genericRecord.Set("opt_double", &optDouble)
	optBytes := []byte("hello")
	genericRecord.Set("opt_bytes", &optBytes)
	optString := "hello"
	genericRecord.Set("opt_string", &optString)
	optFixed := []byte("12345")
	genericRecord.Set("opt_fixed", &optFixed)
	optArray := []string{"hello", "world"}
	genericRecord.Set("opt_array", &optArray)
	optMap := map[string]string{"hello": "world"}
	genericRecord.Set("opt_map", &optMap)
	optNested := NewGenericRecord(nestedSchema)
	optNested.Set("id", int32(1))
	genericRecord.Set("opt_record", optNested)

	testGeneric(genericRecord)

	type Nest struct {
		Id int32
	}

	type Rec struct {
		Opt_bool   *bool
		Opt_int    *int32
		Opt_long   *int64
		Opt_float  *float32
		Opt_double *float64
		Opt_bytes  *[]byte
		Opt_string *string
		Opt_fixed  *[]byte
		Opt_array  *[]string
		Opt_map    *map[string]string
		Opt_record *Nest
	}

	emptySpecificRecord := &Rec{nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil}
	testSpecific(emptySpecificRecord, schema)

	specificRecord := &Rec{
		&optBool,
		&optInt,
		&optLong,
		&optFloat,
		&optDouble,
		&optBytes,
		&optString,
		&optFixed,
		&optArray,
		&optMap,
		&Nest{Id: 1},
	}
	testSpecific(specificRecord, schema)

}

func TestProjections(t *testing.T) {
	schemaV1 := MustParseSchema(`{ 
					"name": "Rec", 
					"type": "record", 
					"fields": [ 
						{ "name": "deleted", "type": "int" }, 
						{ "name": "sum", "type": "int" },
						{ "name": "longToDouble", "type": "long" },
						{ "name": "id", "type": "bytes" }
					] 
				}`)

	//fields are ordered differently
	//id field is both renamed and promoted to string
	//list field is added with a default
	schemaV2 := MustParseSchema(`{
					"name": "Rec",
					"type": "record",
					"fields": [
						{ "name": "key", "type": "string", "aliases": ["id"] },
						{ "name": "sum", "type": "long" },
						{ "name": "longToDouble", "type": "double" },
						{ "name": "added", "type": { "type": "array", "items": "long" }, "default": [1,2,3] }
					]
				}`)

	genRecV1 := NewGenericRecord(schemaV1)
	genRecV1.Set("deleted", int32(5))
	genRecV1.Set("sum", int32(99))
	genRecV1.Set("id", []byte("key1"))
	genRecV1.Set("longToDouble", int64(12345))

	var buf bytes.Buffer
	w := NewGenericDatumWriter().SetSchema(genRecV1.Schema())
	if err := w.Write(genRecV1, NewBinaryEncoder(&buf)); err != nil {
		panic(err)
	}

	r := NewDatumProjector(schemaV2, schemaV1)
	decodedRecord := NewGenericRecord(schemaV2)
	if err := r.Read(decodedRecord, NewBinaryDecoder(buf.Bytes())); err != nil {
		panic(err)
	}

	log.Println(decodedRecord)
	if decodedRecord.Get("key").(string) != "key1" ||
		decodedRecord.Get("sum").(int64) != 99 ||
		len(decodedRecord.Get("added").([]interface{})) != 3 {
		panic("generic projection failed")
	}
	type RecV1 struct {
		Deleted      int32
		Id           []byte
		Sum          int32
		LongToDouble int64
	}
	type RecV2 struct {
		Key          string
		Sum          int64
		LongToDouble float64
		Added        []int64 `avro:default,[1,2,3]`
	}

	recV1 := &RecV1{500, []byte("key1"), 1000, 12345}
	var buf2 bytes.Buffer
	w2 := NewSpecificDatumWriter().SetSchema(schemaV1)
	if err := w2.Write(recV1, NewBinaryEncoder(&buf2)); err != nil {
		panic(err)
	}

	r2 := NewDatumProjector(schemaV2, schemaV1)
	recV2 := new(RecV2)
	if err := r2.Read(recV2, NewBinaryDecoder(buf2.Bytes())); err != nil {
		panic(err)
	}

	if recV2.Key != string(recV1.Id) ||
		recV2.Sum != int64(recV1.Sum) ||
		recV2.LongToDouble != float64(recV1.LongToDouble) ||
		len(recV2.Added) != 3 {
		panic("specific projection failed")
	}

}
