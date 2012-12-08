// Package m provides a simple way to bidirectionally marshal structs to a database.
package m

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strings"

	"github.com/titanous/go-backports/database/sql"
)

const (
	Cassandra DBType = iota
	PostgreSQL
)

type DBType int

func (t DBType) NewMapping() *Mapping {
	return &Mapping{Type: t, tables: make(map[reflect.Type]*tableMap)}
}

type Mapping struct {
	DB   *sql.DB
	Type DBType

	tables map[reflect.Type]*tableMap
}

type tableMap struct {
	Name    string
	Type    reflect.Type
	Columns []*columnMap
	m       *Mapping
}

type columnMap struct {
	Name       string
	Serialize  bool
	PrimaryKey bool
	Field      int
}

// AddTable adds a table to struct mapping to a Mapping.
//	M.AddTable("posts", Post{})
func (m *Mapping) AddTable(name string, thing interface{}) {
	typ := reflect.TypeOf(thing)
	m.tables[typ] = &tableMap{name, typ, getTableColumns(thing, typ), m}
}

func getTableColumns(thing interface{}, typ reflect.Type) []*columnMap {
	columns := make([]*columnMap, 0, typ.NumField())

	for i := 0; i < typ.NumField(); i++ {
		field := typ.Field(i)
		tag := strings.Split(field.Tag.Get("db"), ",")
		if len(tag) > 0 && tag[0] != "" {
			col := &columnMap{Field: i}
			for _, flag := range tag {
				switch flag {
				case "pk":
					col.PrimaryKey = true
				case "serialize":
					col.Serialize = true
				default:
					if col.Name == "" {
						col.Name = flag
					}
				}
			}
			columns = append(columns, col)
		}
	}

	return columns
}

// Insert takes a struct and inserts it into the appropriate table.
// If a field is nil it will not be part of the INSERT statement.
func (m *Mapping) Insert(thing interface{}) error {
	return m.lookupTable(thing).insert(thing)
}

func (m *Mapping) InsertValues(table string, columns []string, values ...interface{}) error {
	_, err := m.DB.Exec(sqlInsertString(table, columns, m.Type), values...)
	return err
}

// Update takes a struct and a map of column names to data and updates the struct and the database row.
func (m *Mapping) Update(thing interface{}, data map[string]interface{}) error {
	return m.lookupTable(thing).update(thing, data)
}

// Select queries the database and returns a slice containing the returned rows scanned into structs with 
// the same type as thing.
func (m *Mapping) Select(thing interface{}, query string, bindings ...interface{}) ([]interface{}, error) {
	return m.lookupTable(thing).doSelect(query, bindings...)
}

// SelectOne is a convenience function that returns a single record or nil if no record is found.
func (m *Mapping) SelectOne(thing interface{}, query string, bindings ...interface{}) (interface{}, error) {
	res, err := m.lookupTable(thing).doSelect(query, bindings...)
	if err == nil && len(res) < 1 {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return res[0], nil
}

func (t *tableMap) insert(thing interface{}) error {
	columns, values := prepareInsertSqlColumnsValues(thing, t)
	_, err := t.m.DB.Exec(sqlInsertString(t.Name, columns, t.m.Type), values...)
	return err
}

func (t *tableMap) update(thing interface{}, data map[string]interface{}) error {
	columns, values := updateAndGetSqlColumnsValues(thing, t, data)
	keyColumns, keyValues := keysForUpdate(thing, t)
	values = append(values, keyValues...)
	_, err := t.m.DB.Exec(sqlUpdateString(t.Name, columns, keyColumns, t.m.Type), values...)
	return err
}

// Mostly taken from https://github.com/coopernurse/gorp by James Cooper
func (t *tableMap) doSelect(query string, bindings ...interface{}) ([]interface{}, error) {
	rows, err := t.m.DB.Query(query, bindings...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		return nil, err
	}

	results := make([]interface{}, 0)

	for rows.Next() {
		instance := reflect.New(t.Type)
		values := make([]interface{}, len(columns))
		deserializeValues := make(map[int]interface{})

		for x := range columns {
			var column *columnMap
			columnName := columns[x]

			for i := 0; i < len(t.Columns); i++ {
				if t.Columns[i].Name == columnName {
					column = t.Columns[i]
					break
				}
			}

			if column.Name == "" {
				return nil, fmt.Errorf("m: No field `%s` in type %s (query: `%s`)", columnName, t.Type.Name(), query)
			}

			field := instance.Elem().Field(column.Field)

			if column.Serialize {
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

func (m *Mapping) lookupTable(thing interface{}) *tableMap {
	typ := tableType(thing)
	if table, ok := m.tables[typ]; ok {
		return table
	}

	panic(fmt.Sprintf("Unknown table for type: %v (%v)", tableType, typ.Kind()))
}

func tableType(thing interface{}) reflect.Type {
	thingVal := reflect.Indirect(reflect.ValueOf(thing))
	if thingVal.Kind() != reflect.Struct {
		panic(fmt.Sprintf("Expecting struct or struct pointer, got %v (%v)", thing, thingVal.Kind()))
	}
	return thingVal.Type()
}

func prepareInsertSqlColumnsValues(thing interface{}, table *tableMap) ([]string, []interface{}) {
	thingValue := reflect.Indirect(reflect.ValueOf(thing))
	columns := make([]string, 0, len(table.Columns))
	values := make([]interface{}, 0, len(table.Columns))

	for i := 0; i < len(table.Columns); i++ {
		column := table.Columns[i]
		value := thingValue.Field(column.Field)
		kind := value.Kind()

		// skip fields that are nil pointers or empty slices/maps/arrays
		if (kind == reflect.Ptr && value.IsNil()) ||
			((kind == reflect.Slice || kind == reflect.Map || kind == reflect.Array) && value.Len() < 1) {
			continue
		}

		if column.Serialize {
			// TODO(jr): don't eat this marshal error value
			marshaled, _ := json.Marshal(value.Interface())
			values = append(values, string(marshaled))
		} else {
			values = append(values, reflect.Indirect(value).Interface())
		}
		columns = append(columns, column.Name)
	}

	return columns, values
}

func sqlInsertString(tableName string, columns []string, dbt DBType) string {
	var valuesStr string
	columnsStr := strings.Join(columns, ", ")
	for i := range columns {
		var placeholder string
		if dbt == PostgreSQL {
			placeholder = fmt.Sprintf("$%d", i+1)
		} else {
			placeholder = "?"
		}
		valuesStr += placeholder
		if i < len(columns)-1 {
			valuesStr += ", "
		}
	}
	return fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)", tableName, columnsStr, valuesStr)
}

func updateAndGetSqlColumnsValues(thing interface{}, table *tableMap, data map[string]interface{}) ([]string, []interface{}) {
	thingValue := reflect.Indirect(reflect.ValueOf(thing))
	columns := make([]string, 0, len(table.Columns))
	values := make([]interface{}, 0, len(table.Columns))

	for i := 0; i < len(table.Columns); i++ {
		column := table.Columns[i]

		if val, ok := data[column.Name]; ok {
			destField := thingValue.Field(column.Field)
			value := reflect.ValueOf(val)

			// assign the value from the data map to the destination struct field
			destField.Set(value)

			if column.Serialize {
				// TODO(jr): don't eat this marshal error value
				marshaled, _ := json.Marshal(val)
				values = append(values, string(marshaled))
			} else {
				values = append(values, reflect.Indirect(value).Interface())
			}
			columns = append(columns, column.Name)
		}
	}

	return columns, values
}

func keysForUpdate(thing interface{}, table *tableMap) ([]string, []interface{}) {
	thingValue := reflect.Indirect(reflect.ValueOf(thing))
	columns := make([]string, 0, len(table.Columns))
	values := make([]interface{}, 0, len(table.Columns))

	for i := 0; i < len(table.Columns); i++ {
		column := table.Columns[i]

		if !column.PrimaryKey {
			continue
		}

		value := thingValue.Field(column.Field)

		columns = append(columns, column.Name)
		values = append(values, reflect.Indirect(value).Interface())
	}

	return columns, values
}

func columnPlaceholders(columns []string, sep string, dbt DBType) (res string) {
	count := len(columns)
	for i, column := range columns {
		var placeholder string
		if dbt == PostgreSQL {
			placeholder = fmt.Sprintf("$%d", i+1)
		} else {
			placeholder = "?"
		}

		res += column + " = " + placeholder
		if i+1 < count {
			res += sep
		}
	}
	return
}

func sqlUpdateString(tableName string, columns []string, keys []string, dbt DBType) string {
	return fmt.Sprintf("UPDATE %s SET %s WHERE %s", tableName, columnPlaceholders(columns, ", ", dbt), columnPlaceholders(keys, " AND ", dbt))
}
