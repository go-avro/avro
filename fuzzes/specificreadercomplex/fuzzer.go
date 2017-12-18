// +build gofuzz

package specificreadercomplex

import (
	"bytes"

	avro "gopkg.in/avro.v0"
	"gopkg.in/avro.v0/fuzzes"
)

var buf bytes.Buffer
var reader = avro.NewSpecificDatumReader().SetSchema(fuzzes.ComplexSchema)

func Fuzz(input []byte) int {
	var dest fuzzes.Complex
	err := reader.Read(&dest, avro.NewBinaryDecoder(input))
	if err != nil {
		return 0
	}
	return 1
}
