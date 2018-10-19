package avro

import (
	"errors"
	"reflect"
)

func NewDatumProjector(readerSchema, writerSchema Schema) * DatumProjector {
	//TODO check cache for the same combination of schemas
	return &DatumProjector{
		projection: newProjection(readerSchema, writerSchema),
	}
}

type DatumProjector struct {
	projection *Projection
}

func (reader *DatumProjector) Read(into interface{}, dec Decoder) error {
	rv := reflect.ValueOf(into)
	if rv.Kind() != reflect.Ptr || rv.IsNil() {
		return errors.New("Not applicable for non-pointer types or nil")
	}
	return reader.projection.Project(rv, dec)
}

type Projection struct {
	Project func(into reflect.Value, dec Decoder) error
}

func newProjection(reader, writer Schema) *Projection {
	result := &Projection{}
	switch reader.Type() {
	case Null:
	case Int:
	case Long:
	case Float:
	case Double:
	case Bytes:
	case String:
	case Fixed:
	case Enum:
	case Array:
	case Map:
	case Record:
		readerRecordSchema := reader.(*RecordSchema)
		writerRecordSchema := writer.(*RecordSchema)
		projectIndexMap := make(map[int]*Projection, len(readerRecordSchema.Fields))
	NextReaderField:
		for r, readerField := range readerRecordSchema.Fields {
			checkField := func(name string) bool {
				if _, ok := writerRecordSchema.AliasIndex[name]; ok {
					//writerField := writerRecordSchema.Fields[w]
					projectIndexMap[r] = nil //TODO newProjection(readerField, writerField)
					return true
				}
				return false
			}
			if checkField(readerField.Name) {
				continue NextReaderField
			}
			for _, intoFieldAlias := range readerField.Aliases {
				if checkField(intoFieldAlias) {
					continue NextReaderField
				}
			}
			if readerField.Default == nil {
				panic(errors.New("Schema field doesn't have any default value: " + readerField.Name))
			} else {
				projectIndexMap[r] = nil //TODO (readerField.Name, readerField.Default)
			}
		}
	case Union:
		//TODO case Recursive:
	}
	return result
}
