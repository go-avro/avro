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
	Unwrap  func(dec Decoder) (interface{}, error)
}

func newProjection(readerSchema, writerSchema Schema) *Projection {

	if writerSchema.Type() == Union {
		if readerSchema.Type() == Union {
			writerUnionSchema := writerSchema.(*UnionSchema)
			variants := make(map[int32]*Projection)
			for i, t := range writerUnionSchema.Types {
				variants[int32(i)] = newProjection(t, t)
			}
			return unionProjection(variants)
		} else {
			for i, t := range writerSchema.(*UnionSchema).Types {
				if t.Type() == readerSchema.Type() && t.GetName() == readerSchema.GetName() {
					variants := make(map[int32]*Projection)
					variants[int32(i)] = newProjection(readerSchema, t)
					return unionProjection(variants)
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
	//default result.Project(..) relies on Unwrap function but for non-primitive schemas this is not used
	result.Project = func(target reflect.Value, dec Decoder) error {
		if target.Kind() == reflect.Ptr {
			target.Set(reflect.New(target.Type().Elem()))
			target = target.Elem()
		}
		if v, err := result.Unwrap(dec); err != nil {
			return err
		} else {
			target.Set(reflect.ValueOf(v))
		}
		return nil
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
	case Fixed:
		size := writerSchema.(*FixedSchema).Size
		switch {
		case writerSchema.Type() == Fixed && readerSchema.(*FixedSchema).Size == size:
			result.Unwrap = func(dec Decoder) (interface{}, error) {
				fixed := make([]byte, size)
				return fixed, dec.ReadFixed(fixed)
			}
		default:
			panic(fmt.Errorf("impossible projection from %q to %q", writerSchema, readerSchema))
		}
	case Array:
		readerArraySchema := readerSchema.(*ArraySchema)
		switch writerSchema.Type() {
		case Array:
			writerArraySchema := writerSchema.(*ArraySchema)
			itemProjection := newProjection(readerArraySchema.Items, writerArraySchema.Items)
			result.Unwrap = func(dec Decoder) (interface{}, error) {
				arrayLength, err := dec.ReadArrayStart()
				if err != nil {
					return nil, err
				}

				var array []interface{}
				for arrayLength > 0 {
					arrayPart := make([]interface{}, arrayLength, arrayLength)
					var i int64
					for ; i < arrayLength; i++ {
						val, err := itemProjection.Unwrap(dec)
						if err != nil {
							return nil, err
						}
						arrayPart[i] = val
					}
					concatArray := make([]interface{}, len(array)+int(arrayLength), cap(array)+int(arrayLength))
					copy(concatArray, array)
					copy(concatArray, arrayPart)
					array = concatArray
					arrayLength, err = dec.ArrayNext()
					if err != nil {
						return nil, err
					}
				}
				return array, nil
			}
			result.Project = func(target reflect.Value, dec Decoder) error {
				arrayLength, err := dec.ReadArrayStart()
				if err != nil {
					return err
				}
				indirectArrayType := target.Type()
				if indirectArrayType.Kind() == reflect.Ptr {
					indirectArrayType = indirectArrayType.Elem()
				}
				array := reflect.MakeSlice(indirectArrayType, 0, 0)
				for arrayLength > 0 {
					arrayPart := reflect.MakeSlice(indirectArrayType, int(arrayLength), int(arrayLength))
					var i int64
					for ; i < arrayLength; i++ {
						current := arrayPart.Index(int(i))
						if err := itemProjection.Project(current, dec); err != nil {
							return err
						}
					}
					if array.Len() == 0 {
						array = arrayPart
					} else {
						array = reflect.AppendSlice(array, arrayPart)
					}
					arrayLength, err = dec.ArrayNext()
					if err != nil {
						return err
					}
				}
				target.Set(array)
				return nil
			}
		default:
			panic(fmt.Errorf("impossible projection from %q to %q", writerSchema, readerSchema))
		}

	case Map:
		readerMapSchema := readerSchema.(*MapSchema)
		switch writerSchema.Type() {
		case Map:
			writerMapSchema := writerSchema.(*MapSchema)
			valueProjection := newProjection(readerMapSchema.Values, writerMapSchema.Values)
			keyProjection := newProjection(&StringSchema{}, &StringSchema{})
			result.Unwrap = func(dec Decoder) (interface{}, error) {
				mapLength, err := dec.ReadMapStart()
				if err != nil {
					return nil, err
				}
				resultMap := make(map[string]interface{}, mapLength)
				for mapLength > 0 {
					var i int64
					for ; i < mapLength; i++ {
						if key, err := keyProjection.Unwrap(dec); err != nil {
							return nil, err
						} else if val, err := valueProjection.Unwrap(dec); err != nil {
							return nil, err
						} else {
							resultMap[key.(string)] = val
						}
					}

					mapLength, err = dec.MapNext()

				}
				return resultMap, nil
			}

			result.Project = func(target reflect.Value, dec Decoder) error {
				mapLength, err := dec.ReadMapStart()
				if err != nil {
					return err
				}
				elemType := target.Type().Elem()
				elemIsPointer := elemType.Kind() == reflect.Ptr
				indirectMapType := target.Type()
				if indirectMapType.Kind() == reflect.Ptr {
					indirectMapType = indirectMapType.Elem()
				}
				resultMap := reflect.MakeMap(indirectMapType)
				for mapLength > 0 {
					var i int64
					for ; i < mapLength; i++ {
						k := ""
						key := reflect.ValueOf(&k).Elem()
						v := reflect.New(target.Type().Elem()).Interface()
						val := reflect.ValueOf(v).Elem()
						if err := keyProjection.Project(key, dec); err != nil {
							return err
						} else if err := valueProjection.Project(val, dec); err != nil {
							return err
						} else {
							if !elemIsPointer && val.Kind() == reflect.Ptr {
								resultMap.SetMapIndex(key, val.Elem())
							} else {
								resultMap.SetMapIndex(key, val)
							}
						}
					}

					mapLength, err = dec.MapNext()

				}
				target.Set(resultMap)
				return nil
			}
		default:
			panic(fmt.Errorf("impossible projection from %q to %q", writerSchema, readerSchema))
		}
	case Record:
		readerRecordSchema := readerSchema.(*RecordSchema)
		switch writerSchema.Type() {
		case Record:
			writerRecordSchema := writerSchema.(*RecordSchema)
			defaultUnwrapperMap := make(map[string]interface{}, 0)
			defaultIndexMap := make(map[string]reflect.Value, 0)
			projectNameMap := make([]string, len(writerRecordSchema.Fields))
			projectIndexMap := make([]*Projection, len(writerRecordSchema.Fields))
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
					//TODO converting default values to native types should be probably a method of Schema type
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
									panic(fmt.Errorf("TODO default converter from %q", reflect.TypeOf(a[0])))
								}
							default:
								panic(fmt.Errorf("TODO default converter to %q", readerField.Type.(*ArraySchema).Items))
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
						return nil, fmt.Errorf("TODO Unwrap for %q", field.Type)
					}
					if writerValue, err := projectIndexMap[f].Unwrap(dec); err != nil {
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
							return fmt.Errorf("TODO Unwrap for %q", field.Type)
						}
						if writerValue, err := projectIndexMap[f].Unwrap(dec); err != nil {
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
								projectIndexMap[f].Unwrap(dec) //still have to read deleted fields from the writer value
								continue
							}
						}

						if err := projectIndexMap[f].Project(structField, dec); err != nil {
							return err
						}
					}
					if len(defaultIndexMap) > 0 {
						for d := range defaultIndexMap {
							if field := target.FieldByName(d); field.IsValid() {
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
		default:
			panic(fmt.Errorf("impossible projection from %q to %q", writerSchema, readerSchema))
		}
	case Enum:
		//TODO implement Enum projection once Enum representation is finalized
		panic("enum projection not implemented yet")
	case Recursive:
		//TODO implement Recursive schema after clarifying how it's meant to be used
		panic("recurive schema projection not implemented yet")
	default:
		panic(fmt.Errorf("not Implemented type: %v", readerSchema))
	}
	return result
}

func unionProjection(variants map[int32]*Projection) *Projection {
	result := &Projection{}
	result.Unwrap = func(dec Decoder) (interface{}, error) {
		unionIndex, err := dec.ReadInt()
		if err != nil {
			return reflect.ValueOf(unionIndex), err
		}
		if p, ok := variants[unionIndex]; ! ok {
			return reflect.Value{}, fmt.Errorf("invalid union index %d", unionIndex)
		} else {
			return p.Unwrap(dec)
		}

	}
	result.Project = func(target reflect.Value, dec Decoder) error {
		unionIndex, err := dec.ReadInt()
		if err != nil {
			return err
		}

		if p, ok := variants[unionIndex]; ok {
			return p.Project(target, dec)
		} else {
			return fmt.Errorf("invalid union index %d", unionIndex)
		}

	}
	return result
}
