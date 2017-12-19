package fuzzes

import avro "gopkg.in/avro.v0"

const ComplexSchemaRaw = `{
    "type": "record",
    "namespace": "example.avro",
    "name": "Complex",
    "fields": [
        {
            "name": "stringArray",
            "type": {
                "type": "array",
                "items": "string"
            }
        },
        {
            "name": "longArray",
            "type": {
                "type": "array",
                "items": "long"
            }
        },
        {
            "name": "enumField",
            "type": {
                "type": "enum",
                "name": "foo",
                "symbols": [
                    "A",
                    "B",
                    "C",
                    "D"
                ]
            }
        },
        {
            "name": "mapOfInts",
            "type": {
                "type": "map",
                "values": "int"
            }
        },
        {
            "name": "unionField",
            "type": [
                "null",
                "string",
                "boolean"
            ]
        },
        {
            "name": "fixedField",
            "type": {
                "type": "fixed",
                "size": 16,
                "name": "md5"
            }
        },
        {
            "name": "recordField",
            "type": ["null", {
                "type": "record",
                "name": "TestRecord",
                "fields": [
                    {
                        "name": "longRecordField",
                        "type": "long"
                    },
                    {
                        "name": "stringRecordField",
                        "type": "string"
                    },
                    {
                        "name": "intRecordField",
                        "type": "int"
                    },
                    {
                        "name": "floatRecordField",
                        "type": "float"
                    }
                ]
            }]
        },
        {
            "name":"mapOfRecord",
            "type":{
               "type": "map",
               "values": "TestRecord"
            }
        }
    ]
}`

var ComplexSchema = avro.MustParseSchema(ComplexSchemaRaw)

var ComplexEnumSymbols = []string{"A", "B", "C", "D"}

func NewComplexEnumField() *avro.GenericEnum {
	return avro.NewGenericEnum(ComplexEnumSymbols)
}

//complex
type Complex struct {
	StringArray []string
	LongArray   []int64
	EnumField   *avro.GenericEnum
	MapOfInts   map[string]int32
	UnionField  interface{}
	FixedField  []byte
	RecordField *TestRecord
	MapOfRecord map[string]*TestRecord
}

type TestRecord struct {
	LongRecordField   int64
	StringRecordField string
	IntRecordField    int32
	FloatRecordField  float32
}

const PrimitiveSchemaRaw = `{"type":"record","name":"Primitive","namespace":"example.avro","fields":[{"name":"booleanField","type":"boolean"},{"name":"intField","type":"int"},{"name":"longField","type":"long"},{"name":"floatField","type":"float"},{"name":"doubleField","type":"double"},{"name":"bytesField","type":"bytes"},{"name":"stringField","type":"string"},{"name":"nullField","type":"null"}]}`

type Primitive struct {
	BooleanField bool
	IntField     int32
	LongField    int64
	FloatField   float32
	DoubleField  float64
	BytesField   []byte
	StringField  string
	NullField    interface{}
}

var CombinedSchemaRaw = `{
    "type": "record",
    "namespace": "example.avro",
    "name": "CombinedEverything",
    "fields": [
        {
            "name": "complex",
            "type": ["null", ` + ComplexSchemaRaw + `]
        },
        {
            "name": "primitive",
            "type": ["null", ` + PrimitiveSchemaRaw + `]
        }
    ]
}`

var CombinedSchema = avro.MustParseSchema(CombinedSchemaRaw)

type Combined struct {
	Complex   *Complex
	Primitive *Primitive
}
