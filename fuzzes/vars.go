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
