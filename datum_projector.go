package avro

import (
	"errors"
	"fmt"
	"log"
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
}

func (p *Projection) Read(target reflect.Value, decode func() (interface{}, error)) error {
	//into = dereference(into)
	if v, err := decode(); err != nil {
		return err
	} else {
		target.Set(reflect.ValueOf(v))
	}
	return nil
}

func newProjection(readerSchema, writerSchema Schema) *Projection {
	result := &Projection{}
	switch readerSchema.Type() {
	//TODO case Null:
	case Int:
		switch writerSchema.Type() {
		case Int:
			result.Project = func(target reflect.Value, dec Decoder) error {
				return result.Read(target, func() (interface{}, error) { return dec.ReadInt() })
			}
		default:
			panic(fmt.Errorf("impossible projection from %q to %q", writerSchema, readerSchema))
		}

	case Long:
		switch writerSchema.Type() {
		case Int:
			result.Project = func(target reflect.Value, dec Decoder) error {
				return result.Read(target, func() (interface{}, error) { v, err := dec.ReadInt(); return int64(v), err })
			}
		case Long:
			result.Project = func(target reflect.Value, dec Decoder) error {
				return result.Read(target, func() (interface{}, error) { return dec.ReadInt() })
			}
		default:
			panic(fmt.Errorf("impossible projection from %q to %q", writerSchema, readerSchema))
		}
		//TODO case Float:
		//TODO case Double:
	case Bytes:
		switch writerSchema.Type() {
		case Bytes:
			result.Project = func(target reflect.Value, dec Decoder) error {
				return result.Read(target, func() (interface{}, error) { return dec.ReadBytes() })
			}
		case String:
			result.Project = func(target reflect.Value, dec Decoder) error {
				return result.Read(target, func() (interface{}, error) { v, err := dec.ReadString(); return []byte(v), err })
			}
		default:
			panic(fmt.Errorf("impossible projection from %q to %q", writerSchema, readerSchema))
		}

	case String:
		switch writerSchema.Type() {
		case String:
			result.Project = func(target reflect.Value, dec Decoder) error {
				return result.Read(target, func() (interface{}, error) { return dec.ReadString() })
			}
		case Bytes:
			result.Project = func(target reflect.Value, dec Decoder) error {
				return result.Read(target, func() (interface{}, error) { v, err := dec.ReadBytes(); return string(v), err })
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
		defaultIndexMap := make(map[string]reflect.Value, 0)
		projectNameMap := make([]string, len(writerRecordSchema.Fields))
		projectIndexMap := make([]*Projection, len(writerRecordSchema.Fields))
		type NoDefault struct{}
	NextReaderField:
		for w, writerField := range writerRecordSchema.Fields {
			//match by name
			for _, readerField := range readerRecordSchema.Fields {
				if writerField.Name == readerField.Name {
					readerStructName := strings.Title(readerField.Name)
					defaultIndexMap[readerStructName] = reflect.ValueOf(nil)
					projectNameMap[w] = readerStructName
					projectIndexMap[w] = newProjection(readerField.Type, writerField.Type)
					continue NextReaderField
				}
			}
			//match by alias
			for _, readerField := range readerRecordSchema.Fields {
				for _, intoFieldAlias := range readerField.Aliases {
					if writerField.Name == intoFieldAlias {
						readerStructName := strings.Title(readerField.Name)
						defaultIndexMap[readerStructName] = reflect.ValueOf(nil)
						projectNameMap[w] = readerStructName
						projectIndexMap[w] = newProjection(readerField.Type, writerField.Type)
						continue NextReaderField
					}
				}
			}
		}
		for _, readerField := range readerRecordSchema.Fields {
			readerStructName := strings.Title(readerField.Name)
			if _, ok := defaultIndexMap[readerStructName]; !ok {
				//TODO this functionality should be part of Schema type
				var defaultValue reflect.Value
				switch readerField.Type.Type() {
				case Array:
					//reflect.MakeSlice()
					a := readerField.Default.([]interface{})
					if len(a) > 0 {
						switch readerField.Type.(*ArraySchema).Items.Type() {
						case Long:
							defaultValue = reflect.MakeSlice(reflect.SliceOf(reflect.TypeOf(int64(0))), len(a),len(a))
							switch reflect.TypeOf(a[0]).Kind() {
								case reflect.Float64: for i, x := range a {
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
				defaultIndexMap[readerStructName] = defaultValue
			} else {
				delete(defaultIndexMap, readerStructName)
			}
		}

		result.Project = func(target reflect.Value, dec Decoder) error {
			target = dereference(target)
			switch target.Kind() {
			case reflect.Struct:
				for f := range projectIndexMap {
					field := writerRecordSchema.Fields[f]
					log.Println(f, target.Type(), strings.Title(field.Name))
					structField := target.FieldByName(projectNameMap[f])
					if !structField.IsValid() {
						return fmt.Errorf("no such field %q in %q", field.Name, target.Type().String())
					}
					if err := projectIndexMap[f].Project(structField, dec); err != nil {
						return err
					}
				}
				if len(defaultIndexMap) > 0 {
					for d := range defaultIndexMap {
						target.FieldByName(d).Set(defaultIndexMap[d])
					}
				}
			default:
				panic("!")
			}
			return nil
		}
		//TODO case Union:
		//TODO case Recursive:
	default:
		panic(fmt.Errorf("not Implemented type: %v", readerSchema))
	}
	return result
}
