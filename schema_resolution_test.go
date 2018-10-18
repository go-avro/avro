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

	log.Println(record)
	log.Println(decodedRecord)
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
