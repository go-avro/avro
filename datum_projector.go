package avro

import (
	"errors"
	"fmt"
	"reflect"
	"strings"
)

func NewDatumProjector(readerSchema, writerSchema Schema) *DatumProjector {
	//TODO check cache for the same combination of schemas
	return &DatumProjector{
		projection: newProjection(readerSchema, writerSchema),
	}
}

type DatumProjector struct {
	projection *Projection
}

func (reader *DatumProjector) Read(target interface{}, dec Decoder) error {
	rv := reflect.ValueOf(target)
	if rv.Kind() != reflect.Ptr || rv.IsNil() {
		return errors.New("not applicable for non-pointer types or nil")
	}
	return reader.projection.Project(rv, dec)
}

type Projection struct {
	Project func(target reflect.Value, dec Decoder) error
	Unwrap func(dec Decoder) (interface{}, error)
}

func (p *Projection) Read(target reflect.Value, dec Decoder) error {
	if v, err := p.Unwrap(dec); err != nil {
		return err
	} else {
		target.Set(reflect.ValueOf(v))
	}
	return nil
}

func newProjection(readerSchema, writerSchema Schema) *Projection {

	if writerSchema.Type() == Union {
		if readerSchema.Type() == Union {
			writerUnionSchema := writerSchema.(*UnionSchema)
			variants := make([]*Projection, len(writerUnionSchema.Types))
			for i, t := range writerUnionSchema.Types {
				variants[i] = newProjection(t, t)
			}
			return unionProjection(readerSchema.(*UnionSchema), writerUnionSchema, variants)
		} else {
			for _, t := range writerSchema.(*UnionSchema).Types {
				if t.Type() == readerSchema.Type() && t.GetName() == readerSchema.GetName() {
					variants := make([]*Projection, len(writerSchema.(*UnionSchema).Types))
					for i, t := range writerSchema.(*UnionSchema).Types {
						if t.Type() == readerSchema.Type() {
							variants[i] = newProjection(readerSchema, t)
						}
					}
					return unionProjection(readerSchema, writerSchema.(*UnionSchema), variants)
				}
			}
			panic(fmt.Errorf("writer Union does not match reader schema: %v", readerSchema))
		}
	} else if readerSchema.Type() == Union {
		for _, t := range readerSchema.(*UnionSchema).Types {
			if t.Type() == writerSchema.Type() && t.GetName() == writerSchema.GetName() {
				return newProjection(t, writerSchema)
			}
		}
	}

	result := &Projection{}
	result.Project = func(target reflect.Value, dec Decoder) error {
		if target.Kind() == reflect.Ptr {
			target.Set(reflect.New(target.Type().Elem()))
			target = target.Elem()
		}
		return result.Read(target, dec)
	}

	switch readerSchema.Type() {
	case Null:
		switch writerSchema.Type() {
		case Null:
			result.Unwrap = func(dec Decoder) (interface{}, error) {
				return nil, nil
			}
		default:
			panic(fmt.Errorf("impossible projection from %q to %q", writerSchema, readerSchema))
		}
	case Boolean:
		switch writerSchema.Type() {
		case Boolean:
			result.Unwrap = func(dec Decoder) (interface{}, error) {
				return dec.ReadBoolean()
			}
		default:
			panic(fmt.Errorf("impossible projection from %q to %q", writerSchema, readerSchema))
		}

	case Int:
		switch writerSchema.Type() {
		case Int:
			result.Unwrap = func(dec Decoder) (interface{}, error) {
				return dec.ReadInt()
			}
		default:
			panic(fmt.Errorf("impossible projection from %q to %q", writerSchema, readerSchema))
		}

	case Long:
		switch writerSchema.Type() {
		case Long:
			result.Unwrap = func(dec Decoder) (interface{}, error) {
				return dec.ReadInt()
			}
		case Int:
			result.Unwrap = func(dec Decoder) (interface{}, error) {
				v, err := dec.ReadInt()
				return int64(v), err
			}
		default:
			panic(fmt.Errorf("impossible projection from %q to %q", writerSchema, readerSchema))
		}
	case Float:
		switch writerSchema.Type() {
		case Float:
			result.Unwrap = func(dec Decoder) (interface{}, error) {
				return dec.ReadFloat()
			}
		case Int:
			result.Unwrap = func(dec Decoder) (interface{}, error) {
				v, err := dec.ReadInt()
				return float32(v), err
			}
		case Long:
			result.Unwrap = func(dec Decoder) (interface{}, error) {
				v, err := dec.ReadLong()
				return float32(v), err
			}
		default:
			panic(fmt.Errorf("impossible projection from %q to %q", writerSchema, readerSchema))
		}

	case Double:
		switch writerSchema.Type() {
		case Double:
			result.Unwrap = func(dec Decoder) (interface{}, error) {
				return dec.ReadDouble()
			}
		case Int:
			result.Unwrap = func(dec Decoder) (interface{}, error) {
				v, err := dec.ReadInt()
				return float64(v), err
			}
		case Long:
			result.Unwrap = func(dec Decoder) (interface{}, error) {
				v, err := dec.ReadLong()
				return float64(v), err
			}
		case Float:
			result.Unwrap = func(dec Decoder) (interface{}, error) {
				v, err := dec.ReadFloat()
				return float64(v), err
			}
		default:
			panic(fmt.Errorf("impossible projection from %q to %q", writerSchema, readerSchema))
		}
	case Bytes:
		switch writerSchema.Type() {
		case Bytes:
			result.Unwrap = func(dec Decoder) (interface{}, error) {
				return dec.ReadBytes()
			}
		case String:
			result.Unwrap = func(dec Decoder) (interface{}, error) {
				v, err := dec.ReadString()
				return []byte(v), err
			}
		default:
			panic(fmt.Errorf("impossible projection from %q to %q", writerSchema, readerSchema))
		}

	case String:
		switch writerSchema.Type() {
		case String:
			result.Unwrap = func(dec Decoder) (interface{}, error) {
				return dec.ReadString()
			}
		case Bytes:
			result.Unwrap = func(dec Decoder) (interface{}, error) {
				v, err := dec.ReadBytes()
				return string(v), err
			}
		default:
			panic(fmt.Errorf("impossible projection from %q to %q", writerSchema, readerSchema))
		}
		//TODO case Fixed:
		//TODO case Enum:
		//TODO case Array:
		//TODO case Map:
	case Record:
		readerRecordSchema := readerSchema.(*RecordSchema)
		writerRecordSchema := writerSchema.(*RecordSchema)
		defaultUnwrapperMap := make(map[string]interface{}, 0)
		defaultIndexMap := make(map[string]reflect.Value, 0)
		projectNameMap := make([]string, len(writerRecordSchema.Fields))
		projectIndexMap := make([]*Projection, len(writerRecordSchema.Fields))
		type NoDefault struct{}
	NextReaderField:
		for w, writerField := range writerRecordSchema.Fields {
			//match by name
			for _, readerField := range readerRecordSchema.Fields {
				if writerField.Name == readerField.Name {
					defaultIndexMap[readerField.Name] = reflect.ValueOf(nil)
					projectNameMap[w] = readerField.Name
					projectIndexMap[w] = newProjection(readerField.Type, writerField.Type)
					continue NextReaderField
				}
			}
			//match by alias
			for _, readerField := range readerRecordSchema.Fields {
				for _, intoFieldAlias := range readerField.Aliases {
					if writerField.Name == intoFieldAlias {
						defaultIndexMap[readerField.Name] = reflect.ValueOf(nil)
						projectNameMap[w] = readerField.Name
						projectIndexMap[w] = newProjection(readerField.Type, writerField.Type)
						continue NextReaderField
					}
				}
			}
			//removed fields
			projectIndexMap[w] = newProjection(writerField.Type, writerField.Type)
		}
		for _, readerField := range readerRecordSchema.Fields {
			if _, ok := defaultIndexMap[readerField.Name]; !ok {
				//TODO this functionality should be part of Schema type
				defaultUnwrapperMap[readerField.Name] = readerField.Default
				var defaultValue reflect.Value
				switch readerField.Type.Type() {
				case Array:
					a := readerField.Default.([]interface{})
					if len(a) > 0 {
						switch readerField.Type.(*ArraySchema).Items.Type() {
						case Long:
							defaultValue = reflect.MakeSlice(reflect.SliceOf(reflect.TypeOf(int64(0))), len(a), len(a))
							switch reflect.TypeOf(a[0]).Kind() {
							case reflect.Float64:
								for i, x := range a {
									defaultValue.Index(i).Set(reflect.ValueOf(int64(x.(float64))))
								}
							default:
								panic(fmt.Errorf("not impelemented %q", reflect.TypeOf(a[0])))
							}
						default:
							panic(fmt.Errorf("not impelemented %q", readerField.Type.(*ArraySchema).Items))
						}

					}
				default:
					defaultValue = reflect.ValueOf(readerField.Default)
				}
				defaultIndexMap[readerField.Name] = defaultValue
			} else {
				delete(defaultIndexMap, readerField.Name)
			}
		}

		result.Unwrap = func(dec Decoder) (interface{}, error) {
			record := NewGenericRecord(readerRecordSchema)
			for f := range projectIndexMap {
				field := writerRecordSchema.Fields[f]
				if projectIndexMap[f].Unwrap == nil {
					return nil, fmt.Errorf("unwrap not implemented for %q", field.Type)
				}
				if writerValue , err := projectIndexMap[f].Unwrap(dec); err != nil {
					return nil, err
				} else if writerValue != nil {
					if projectNameMap[f] != "" { //deleted fields don't have a mapped name
						record.Set(projectNameMap[f], writerValue)
					}
				}
			}
			if len(defaultIndexMap) > 0 {
				for d := range defaultUnwrapperMap {
					record.Set(d, defaultUnwrapperMap[d])
				}
			}
			return record, nil
		}

		result.Project = func(target reflect.Value, dec Decoder) error {
			if target.Kind() == reflect.Ptr && !target.Elem().IsValid() {
				target.Set(reflect.New(target.Type().Elem()))
			}
			target = dereference(target)
			switch target.Interface().(type) {
			case GenericRecord:

				record := target.Interface().(GenericRecord)
				for f := range projectIndexMap {
					field := writerRecordSchema.Fields[f]
					if projectIndexMap[f].Unwrap == nil {
						return fmt.Errorf("unwrap not implemented for %q", field.Type)
					}
					if writerValue , err := projectIndexMap[f].Unwrap(dec); err != nil {
						return err
					} else if writerValue != nil {
						if projectNameMap[f] != "" { //deleted fields don't have a mapped name
							record.Set(projectNameMap[f], writerValue)
						}
					}
				}
				if len(defaultIndexMap) > 0 {
					for d := range defaultUnwrapperMap {
						record.Set(d, defaultUnwrapperMap[d])
					}
				}
			default:
				for f := range projectIndexMap {
					//field := writerRecordSchema.Fields[f]
					structField := target.FieldByName(projectNameMap[f])
					if !structField.IsValid() {
						structField = target.FieldByName(strings.Title(projectNameMap[f]))
						if !structField.IsValid() {
							projectIndexMap[f].Unwrap(dec)//still have to read deleted fields from the writer value
							continue
						}
					}

					if err := projectIndexMap[f].Project(structField, dec); err != nil {
						return err
					}
				}
				if len(defaultIndexMap) > 0 {
					for d := range defaultIndexMap {
						if field := target.FieldByName(d); field.IsValid()  {
							field.Set(defaultIndexMap[d])
						} else {
							if field = target.FieldByName(strings.Title(d)); field.IsValid() {
								field.Set(defaultIndexMap[d])
							}
						}
					}
				}
			}
			return nil
		}
		//TODO case Recursive:
	default:
		panic(fmt.Errorf("not Implemented type: %v", readerSchema))
	}
	return result
}

func unionProjection(reader Schema, writer *UnionSchema, variants []*Projection) *Projection {
	result := &Projection{}
	result.Unwrap = func(dec Decoder) (interface{}, error) {
		unionIndex, err := dec.ReadInt()
		if err != nil {
			return reflect.ValueOf(unionIndex), err
		}

		if unionIndex < 0 || int(unionIndex) >= len(variants) {
			return reflect.Value{}, fmt.Errorf("invalid union index %d", unionIndex)
		}

		return variants[unionIndex].Unwrap(dec)

	}
	result.Project = func(target reflect.Value, dec Decoder) error {
		unionIndex, err := dec.ReadInt()
		if err != nil {
			return err
		}

		if unionIndex < 0 || int(unionIndex) >= len(variants) {
			return fmt.Errorf("invalid union index %d", unionIndex)
		}
		return variants[unionIndex].Project(target, dec)
	}
	return result
}
