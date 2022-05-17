/* Licensed to the Apache Software Foundation (ASF) under one or more
contributor license agreements.  See the NOTICE file distributed with
this work for additional information regarding copyright ownership.
The ASF licenses this file to You under the Apache License, Version 2.0
(the "License"); you may not use this file except in compliance with
the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License. */

package main

import (
	"fmt"

	"gopkg.in/avro.v0"
)

// Fields to map should be exported, field names specified
type SomeComplexType struct {
	StringArray []string          `avro:"stringArray"`
	LongArray   []int64           `avro:"longArray"`
	EnumField   *avro.GenericEnum `avro:"enumField"`
	MapOfInts   map[string]int32  `avro:"mapOfInts"`
	UnionField  string            `avro:"unionField"`
	FixedField  []byte            `avro:"fixedField"`
	RecordField *SomeAnotherType  `avro:"recordField"`
}

// Fields to map should be exported here as well, matched by similarity
type SomeAnotherType struct {
	LongRecordField   int64
	StringRecordField string
	IntRecordField    int32
	FloatRecordField  float32
}

func main() {
	schema, err := avro.ParseSchemaFile("schema.json")
	if err != nil {
		// Should not actually happen
		panic(err)
	}

	// Create a specific DatumReader and set schema
	datum := avro.NewSpecificDatumReader()
	datum.SetSchema(schema)

	// Provide a filename to read and a DatumReader to manage the reading itself
	specificReader, err := avro.NewDataFileReader("complex.avro", datum)
	if err != nil {
		// Should not actually happen
		panic(err)
	}

	for {
		// Note: should ALWAYS pass in a pointer, e.g. specificReader.Next(SomeComplexType{}) will NOT work
		obj := new(SomeComplexType)
		ok, err := specificReader.Next(obj)
		if !ok {
			fmt.Println("END")
			break
		}
		if err != nil {
			panic(err)
			break
		} else {
			fmt.Printf("obj: %#v\n", *obj)
			fmt.Printf("obj.RecordField: %#v\n", *obj.RecordField)
		}
	}
}
