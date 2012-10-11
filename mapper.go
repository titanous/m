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

func (m *Mapping) AddTable(name string, thing interface{}) {
	typ := reflect.TypeOf(thing)
	m.tables[typ] = name
}

func (m *Mapping) Insert(thing interface{}) error {
	typ, name := m.lookupTable(thing)
	columns, values := prepareSqlColumnsValues(thing, typ)
	_, err := m.DB.Exec(sqlInsertString(name, columns), values...)
	return err
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

		if column != "" {
			value := thingValue.Field(i)

			if value.Kind() != reflect.Ptr || !value.IsNil() {
				columns = append(columns, column)
				if serialize != "" {
					// TODO: don't eat this marshal error value
					marshaled, _ := json.Marshal(value.Interface())
					values = append(values, string(marshaled))
				} else {
					values = append(values, value.Interface())
				}
			}
		}
	}

	return columns, values
}

func sqlInsertString(tableName string, columns []string) string {
	columnsStr := strings.Join(columns, ", ")
	valuesStr := strings.TrimRight(strings.Repeat("?, ", len(columns)), ", ")
	return fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)", tableName, columnsStr, valuesStr)
}
