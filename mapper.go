// Package m provides a simple bidirectional type to database row mapper.
package m

import (
	"encoding/json"
	"fmt"
	"github.com/titanous/go-backports/database/sql"
	"reflect"
	"strings"
)

type Mapping struct {
	DB *sql.DB

	tables map[reflect.Type]string
}

func NewMapping() *Mapping {
	return &Mapping{tables: make(map[reflect.Type]string)}
}

// AddTable adds a table to type mapping to a Mapping.
//	M.AddTable("posts", Post{})
func (m *Mapping) AddTable(name string, thing interface{}) {
	typ := reflect.TypeOf(thing)
	m.tables[typ] = name
}

// Insert takes a struct and inserts it into the appropriate table.
// If a field is nil it will not be part of the INSERT statement.
func (m *Mapping) Insert(thing interface{}) error {
	typ, name := m.lookupTable(thing)
	columns, values := prepareSqlColumnsValues(thing, typ)
	_, err := m.DB.Exec(sqlInsertString(name, columns), values...)
	return err
}

// Select queries the database and returns a slice containing the returned rows scanned into the same type as thing.
func (m *Mapping) Select(thing interface{}, query string, bindings ...interface{}) ([]interface{}, error) {
	return m.doSelect(thing, query, bindings...)
}

// SelectOne is a convenience function that returns a single record or nil if no record is found.
func (m *Mapping) SelectOne(thing interface{}, query string, bindings ...interface{}) (interface{}, error) {
	res, err := m.doSelect(thing, query, bindings...)
	if err == nil && len(res) < 1 {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return res[0], nil
}

// Mostly taken from https://github.com/coopernurse/gorp by James Cooper
func (m *Mapping) doSelect(thing interface{}, query string, bindings ...interface{}) ([]interface{}, error) {
	rows, err := m.DB.Query(query, bindings...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		return nil, err
	}

	typ := reflect.TypeOf(thing)
	results := make([]interface{}, 0)

	for rows.Next() {
		instance := reflect.New(typ)
		values := make([]interface{}, len(columns))
		deserializeValues := make(map[int]interface{})

		for x := range columns {
			var fieldName string
			var serialize bool
			columnName := columns[x]
			fieldCount := typ.NumField()

			for i := 0; i < fieldCount; i++ {
				field := typ.Field(i)
				if field.Tag.Get("db") == columnName {
					fieldName = field.Name
					if field.Tag.Get("serialize") == "true" {
						serialize = true
					}
					break
				}
			}

			field := instance.Elem().FieldByName(fieldName)

			if !field.IsValid() {
				return nil, fmt.Errorf("m: No field `%s` in type %s (query: `%s`)", columnName, typ.Name(), query)
			}

			if serialize {
				values[x] = new([]byte)
				deserializeValues[x] = field.Addr().Interface()
			} else {
				values[x] = field.Addr().Interface()
			}

		}

		err = rows.Scan(values...)
		if err != nil {
			return nil, err
		}

		for i, v := range deserializeValues {
			data := *values[i].(*[]byte)
			if len(data) > 0 {
				err = json.Unmarshal(data, v)
				if err != nil {
					return nil, err
				}
			}
		}

		results = append(results, instance.Interface())
	}

	return results, nil
}

func (m *Mapping) lookupTable(thing interface{}) (reflect.Type, string) {
	typ := tableType(thing)
	if name, ok := m.tables[typ]; ok {
		return typ, name
	}

	panic(fmt.Sprintf("Unknown table for type: %v (%v)", tableType, typ.Kind()))
}

func tableType(thing interface{}) reflect.Type {
	thingPointer := reflect.ValueOf(thing)
	if thingPointer.Kind() != reflect.Ptr {
		panic(fmt.Sprintf("Expecting pointer, got %v (%v)", thing, thingPointer.Kind()))
	}
	return reflect.TypeOf(thingPointer.Elem().Interface())
}

func prepareSqlColumnsValues(thing interface{}, thingType reflect.Type) ([]string, []interface{}) {
	thingValue := reflect.Indirect(reflect.ValueOf(thing))
	numFields := thingType.NumField()
	var columns []string
	var values []interface{}

	for i := 0; i < numFields; i++ {
		field := thingType.Field(i)
		column := field.Tag.Get("db")
		serialize := field.Tag.Get("serialize")
		value := thingValue.Field(i)
		kind := value.Kind()

		// skip fields that aren't tagged with a column name, nil pointers, empty slices/maps/arrays
		if column == "" ||
			(kind == reflect.Ptr && value.IsNil()) ||
			((kind == reflect.Slice || kind == reflect.Map || kind == reflect.Array) && value.Len() < 1) {
			continue
		}

		if serialize != "" {
			// TODO(jr): don't eat this marshal error value
			marshaled, _ := json.Marshal(value.Interface())
			values = append(values, string(marshaled))
		} else {
			values = append(values, reflect.Indirect(value).Interface())
		}
		columns = append(columns, column)
	}

	return columns, values
}

func sqlInsertString(tableName string, columns []string) string {
	columnsStr := strings.Join(columns, ", ")
	valuesStr := strings.TrimRight(strings.Repeat("?, ", len(columns)), ", ")
	return fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)", tableName, columnsStr, valuesStr)
}
