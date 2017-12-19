// +build gofuzz

package specificreadercomplex

import (
	"bytes"

	avro "gopkg.in/avro.v0"
	"gopkg.in/avro.v0/fuzzes"
)

var buf bytes.Buffer
var reader = avro.NewSpecificDatumReader().SetSchema(fuzzes.ComplexSchema)
var prepared = avro.NewSpecificDatumReader().SetSchema(avro.Prepare(fuzzes.ComplexSchema))

func Fuzz(input []byte) int {
	var dest fuzzes.Complex
	// First run on un-prepared schema
	err := reader.Read(&dest, avro.NewBinaryDecoder(input))
	if err != nil {
		return 0
	}

	// Second run on prepared schema
	dest = fuzzes.Complex{}
	err = prepared.Read(&dest, avro.NewBinaryDecoder(input))
	if err != nil {
		return 0
	}
	return 1
}
