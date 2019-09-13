package syncer

import (
	"context"
	"database/sql"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/bhmj/jsonslice"
	"github.com/bhmj/sqlsync/model"
	_ "github.com/denisenkom/go-mssqldb" // MS SQL driver
	_ "github.com/lib/pq"                // Postgres driver
)

type processor func(ctx context.Context, src *sql.DB, dst *sql.DB, pair *model.SyncPair, level int, quiet bool) error

func identPrintf(level int, format string, a ...interface{}) (str string) {
	for i := 0; i < level*2; i++ {
		str += fmt.Sprintf("  ")
	}
	str += fmt.Sprintf(" - ")
	str += fmt.Sprintf(format, a...)
	return
}

// DoSync ...
func DoSync(ctx context.Context, pair *model.SyncPair, quiet bool) {
	pair.Lock()
	process(ctx, pair, doSync, quiet)
	pair.Unlock()
}

// Init ...
func Init(pair *model.SyncPair) {
	process(context.Background(), pair, doInit, false)
}

func process(ctx context.Context, pair *model.SyncPair, fn processor, quiet bool) error {

	srcType := *pair.Source.Type
	dstType := *pair.Target.Type
	if srcType == "mssql" {
		srcType = "sqlserver"
	}
	if dstType == "mssql" {
		dstType = "sqlserver"
	}

	src, err := sql.Open(srcType, pair.SourceLink.ConnString)
	if err != nil {
		fmt.Fprintf(os.Stderr, "\nerror in %s: %s\n", *pair.Origin, err.Error())
		return err
	}
	defer src.Close()

	dst, err := sql.Open(dstType, pair.TargetLink.ConnString)
	if err != nil {
		fmt.Fprintf(os.Stderr, "\nerror in %s: %s\n", *pair.Origin, err.Error())
		return err
	}
	defer dst.Close()

	err = fn(ctx, src, dst, pair, 0, quiet)
	if err != nil {
		fmt.Fprintf(os.Stderr, "\n%s: %s\n", *pair.Origin, err.Error())
	}
	return err
}

func doSync(ctx context.Context, src *sql.DB, dst *sql.DB, pair *model.SyncPair, level int, quiet bool) (err error) {
	//dstType := *pair.Target.Type

	query, qargs, outs := buildQuery(pair)
	rows, err := src.QueryContext(ctx, query, qargs...)
	if err != nil {
		return
	}
	defer rows.Close()

	pv := make([]model.ColumnParamValue, len(pair.ColumnParam))
	copy(pv, pair.ColumnParam)

	args := ""
	for _, t := range pv {
		if len(args) > 0 {
			args += ", "
		}
		args += "@" + t.Param + "=" + strconv.FormatInt(t.Value, 10)
	}
	msg := "\n" + identPrintf(level, "%s %s  [0]: ", *pair.Origin, args)

	recs := 0
	recordset := 0
	for {
		mapper, err := NewMapper(rows, pair.Mapping, pair.ColumnParam)
		if err != nil {
			fmt.Print(msg)
			return err
		}

		heap := make([]interface{}, 0)
		nrows := 0

		for rows.Next() {
			err = rows.Scan(mapper.Vals...)
			if err != nil {
				fmt.Print(msg)
				return err
			}
			nrows++
			// update RVs
			for i := range pv { // source col, RV
				nv := mapper.int64ByName(pv[i].Column)
				if nv > pv[i].Value {
					pv[i].Value = nv
				}
			}
			// process data
			if len(pair.RowProc) > 0 {
				// process row
				fmt.Print(msg)
				msg = ""
				err = storeData(ctx, src, dst, pair, recordset, []interface{}{mapper.getRow()}, pv)
				if err != nil {
					return err
				}
				// call row proc(s)
				for p := 0; p < len(pair.RowProc); p++ {
					proc := &pair.RowProc[p]
					if proc.Condition != "" {
						js, err := json.Marshal(mapper.getRow())
						if err != nil {
							return err
						}
						cond := "$[?(" + proc.Condition + ")]"
						result, err := jsonslice.Get([]byte("["+string(js)+"]"), cond)
						if err != nil {
							return err
						}
						if string(result) == "[]" {
							continue
						}
					}
					for i := 0; i < len(proc.Sync); i++ {
						sp := &proc.Sync[i]
						// set proc params
						for ip := 0; ip < len(sp.ColumnParam); ip++ {
							val := mapper.int64ByName(sp.ColumnParam[ip].Column)
							sp.ColumnParam[ip].Value = val // real deal
						}
						err := doSync(ctx, src, dst, sp, level+1, quiet) // nested
						if err != nil {
							return err
						}
					}
				}
				// store RV
				err = storeRV(ctx, src, dst, pair, pv)
				if err != nil {
					fmt.Print(msg)
					return err
				}
			} else {
				heap = append(heap, mapper.copyRow())
			}
		} // for rows.Next()
		err = rows.Err()
		if err != nil {
			fmt.Print(msg)
			return err
		}
		// output params
		for i := 0; i < len(pv); i++ {
			if pv[i].Output && pv[i].Value < outs[i] {
				pv[i].Value = outs[i]
			}
		}

		msg += fmt.Sprintf("%d", nrows)
		if len(heap) > 0 {
			recs++
			err = storeData(ctx, src, dst, pair, recordset, heap, pv)
			if err != nil {
				return err
			}
		}

		err = storeRV(ctx, src, dst, pair, pv)
		if err != nil {
			return err
		}

		if !rows.NextResultSet() {
			break
		}
		recordset++
		msg += fmt.Sprintf("  [%d]: ", recordset)
	} // forever

	copy(pair.ColumnParam, pv)
	if recs > 0 && !quiet {
		fmt.Print(msg)
	}
	return
}

func doInit(ctx context.Context, src *sql.DB, dst *sql.DB, pair *model.SyncPair, level int, quiet bool) error {
	ph := "?"
	if *pair.Target.Type == "postgres" {
		ph = "$1"
	}
	var sync *sql.DB
	if pair.SyncTableSide == "src" {
		sync = src
	} else {
		sync = dst
	}

	query := "select * from " + *pair.SyncTable + " where tbl = " + ph
	rows, err := sync.QueryContext(ctx, query, *pair.Origin)
	if err != nil {
		return err
	}
	defer rows.Close()

	mapper, err := NewMapper(rows, map[string]string{"param": "param", "value": "value"}, nil)
	if err != nil {
		return err
	}

	for rows.Next() {
		err = rows.Scan(mapper.Vals...)
		if err != nil {
			return err
		}
		dbParam := mapper.stringByName("param")
		// through config params
		for p := 0; p < len(pair.ColumnParam); p++ {
			if dbParam == pair.ColumnParam[p].Param {
				val := mapper.int64ByName("value")
				pair.ColumnParam[p].Value = val // real deal
			}
		}
	}
	return err
}

// Mapper ...
type Mapper struct {
	Vals  []interface{}
	Map   map[string]int
	PVals []interface{}
}

// NewMapper ...
func NewMapper(rows *sql.Rows, mapping map[string]string, pv []model.ColumnParamValue) (*Mapper, error) {
	cols, err := rows.Columns()
	if err != nil {
		return nil, err
	}
	mapper := &Mapper{}
	mapper.PVals = make([]interface{}, 0)
	mapper.Vals = make([]interface{}, len(cols))
	for i := 0; i < len(cols); i++ {
		mapper.Vals[i] = new(interface{})
	}
	mapper.Map = make(map[string]int)
	for c := 0; c < len(cols); c++ {
		col := cols[c]
		dst, ok := mapping[col]
		if ok {
			mapper.Map[dst] = c
		} else {
			mapper.Map[col] = c
		}
	}
	var notFound []string
	for colsField, dstField := range mapping {
		if colsField[:1] == "@" {
			found := -1
			for ii := 0; ii < len(pv); ii++ {
				if pv[ii].Param == colsField[1:] {
					found = ii
					break
				}
			}
			if found >= 0 {
				mapper.PVals = append(mapper.PVals, pv[found].Value)
				mapper.Map[dstField] = -len(mapper.PVals)
			} else {
				notFound = append(notFound, colsField)
			}
			continue
		}
		_, ok := mapper.Map[dstField]
		if !ok {
			notFound = append(notFound, colsField+"("+dstField+")")
		}
	}
	if len(notFound) > 0 {
		for i := 0; i < len(notFound); i++ {
			if i == 0 {
				fmt.Printf("\tMissing fields: ")
			} else {
				fmt.Printf(", ")
			}
			fmt.Printf("%s", notFound[i])
		}
		fmt.Println("")
	}
	return mapper, nil
}

func (m *Mapper) getRow() interface{} {
	row := make(map[string]interface{})
	for k, v := range m.Map {
		if v < 0 {
			row[k] = m.PVals[-v-1]
		} else {
			row[k] = m.Vals[v]
		}
	}
	return row
}

func (m *Mapper) copyRow() interface{} {
	row := make(map[string]interface{})
	for fld, idx := range m.Map {
		if idx < 0 {
			row[fld] = m.PVals[-idx-1]
		} else {
			pval := m.Vals[idx]
			row[fld] = *pval.(*interface{})
		}
	}
	return row
}

func (m *Mapper) fieldByName(name string) interface{} {
	i, ok := m.Map[name]
	if !ok {
		return nil
	}
	if i < 0 {
		return m.PVals[-i-1]
	}
	return *(m.Vals[i].(*interface{}))
}

func (m *Mapper) stringByName(name string) string {
	v := m.fieldByName(name)
	if v == nil {
		return ""
	}
	switch v.(type) {
	case string:
		return v.(string)
	}
	return ""
}

func (m *Mapper) int64ByName(name string) int64 {
	v := m.fieldByName(name)
	if v == nil {
		return 0
	}
	switch v.(type) {
	case int64:
		return v.(int64)
	case uint64:
		return int64(v.(uint64))
	case []byte:
		return int64(binary.BigEndian.Uint64(v.([]byte)))
	}
	return 0
}

func (m *Mapper) hasField(name string) bool {
	_, ok := m.Map[name]
	return ok
}

func buildQuery(pair *model.SyncPair) (query string, args []interface{}, outs []int64) {
	switch *pair.Source.Type {
	case "postgres":
		query = "select * from " + *pair.Origin + "("
		n := 0
		for p := range pair.ColumnParam {
			if n > 0 {
				query += ", "
			}
			query += strconv.FormatInt(pair.ColumnParam[p].Value, 10)
			n++
		}
		query += ")"
	case "mssql":
		outs = make([]int64, len(pair.ColumnParam))
		query = *pair.Origin
		for p := range pair.ColumnParam {
			var val interface{}
			if pair.ColumnParam[p].BigEnd {
				buf := make([]byte, 8)
				binary.BigEndian.PutUint64(buf, uint64(pair.ColumnParam[p].Value))
				val = buf
			} else {
				val = pair.ColumnParam[p].Value
			}
			if pair.ColumnParam[p].Output {
				outs[p] = pair.ColumnParam[p].Value
				args = append(args, sql.Named(pair.ColumnParam[p].Param, sql.Out{Dest: &outs[p]}))
			} else {
				args = append(args, sql.Named(pair.ColumnParam[p].Param, val))
			}
		}
	}
	return
}

func storeData(ctx context.Context, src *sql.DB, dst *sql.DB, pair *model.SyncPair, recordset int, heap []interface{}, pv []model.ColumnParamValue) error {

	if recordset >= len(pair.Dest) {
		fmt.Println("not enough Dest procedures (extra recordset(s) encountered) in", *pair.Origin)
		return nil
	}

	var rows *sql.Rows
	var err error

	query := ""
	switch *pair.Target.Type {
	case "postgres":
		js, err := json.Marshal(heap)
		if err != nil {
			return err
		}
		query = "select * from " + *pair.Dest[recordset] + "($1)"
		rows, err = dst.QueryContext(ctx, query, js)
	case "mssql":
		m := heap[0].(map[string]interface{})
		// fix fields order
		fields := make([]string, 0)
		flds := make([]string, 0)
		for k := range m {
			fields = append(fields, "["+k+"]")
			flds = append(flds, k)
		}
		if pair.TableType[recordset] != "" {
			// call through table type
			query += "DECLARE @tbl AS " + pair.TableType[recordset] + "\n"
			// insert
			query += "INSERT INTO @tbl (" + strings.Join(fields, ", ") + ")\n"
			// select
			query += "SELECT * FROM ( VALUES\n"
			for i := range heap {
				if i > 0 {
					query += ","
				}
				m := heap[i].(map[string]interface{})
				vals := valsTable(m, flds)
				query += "(" + vals + ")\n"
			}
			query += ") t (" + strings.Join(fields, ", ") + ");\n"
			query += "EXEC " + *pair.Dest[recordset] + " @tbl;"
		} else {
			// call with named parameters
			for i := range heap {
				m := heap[i].(map[string]interface{})
				query += "EXEC " + *pair.Dest[recordset] + " " + valsList(m, flds) + ";\n"
			}
		}
		rows, err = dst.QueryContext(ctx, query)
	} // switch

	if err != nil {
		return err
	}
	if rows == nil {
		return errors.New("rows == nil (probably destination proc does not exist)")
	}
	defer rows.Close()

	for rows.Next() {
		str := ""
		err = rows.Scan(&str)
		if err != nil {
			return err
		}
	}
	err = rows.Err()
	if err != nil {
		return err
	}

	select {
	default:
	case <-ctx.Done():
		return ctx.Err()
	}

	return nil
}

func valsTable(m map[string]interface{}, fields []string) (query string) {
	for f := 0; f < len(fields); f++ {
		if f > 0 {
			query += ","
		}
		v := m[fields[f]]
		switch v.(type) {
		case *interface{}:
			vv := v.(*interface{})
			v = *vv
		}
		query += valEncode(v)
	}
	return
}

func valsList(m map[string]interface{}, fields []string) (query string) {
	for f := 0; f < len(fields); f++ {
		if f > 0 {
			query += ", "
		}
		v := m[fields[f]]
		switch v.(type) {
		case *interface{}:
			vv := v.(*interface{})
			v = *vv
		}
		val := "@" + fields[f] + "=" + valEncode(v)
		query += val
	}
	return
}

func valEncode(v interface{}) (val string) {
	switch v.(type) {
	case int8, uint8,
		int16, uint16,
		int32, uint32,
		int64, uint64,
		float32, float64:
		val = fmt.Sprintf("%v", v)
	case string:
		val = fmt.Sprintf("'%v'", strings.Replace(v.(string), "'", "''", -1))
	case bool:
		if v.(bool) {
			val = "1"
		} else {
			val = "0"
		}
	case time.Time:
		val = fmt.Sprintf("'%s'", (v.(time.Time)).Format("2006-01-02 15:04:05"))
	default:
		val = "null"
	}
	return
}

func storeRV(ctx context.Context, src *sql.DB, dst *sql.DB, pair *model.SyncPair, pv []model.ColumnParamValue) error {

	changes := false
	for i := 0; i < len(pv) && !changes; i++ {
		if pv[i].Value != pair.ColumnParam[i].Value {
			changes = true
		}
	}
	if !changes {
		return nil
	}

	var sync *sql.DB
	if pair.SyncTableSide == "src" {
		sync = src
	} else {
		sync = dst
	}
	prms := ""
	for i := 0; i < len(pair.ColumnParam); i++ {
		if len(prms) > 0 {
			prms += ","
		}
		prms += "'" + pair.ColumnParam[i].Param + "'"
	}
	if pair.SyncTable == nil || pair.Origin == nil {
		println("err")
	}
	query := "select param, value from " + *pair.SyncTable + " where tbl = '" + *pair.Origin + "' and param in (" + prms + ")"
	rows, err := sync.QueryContext(ctx, query)
	if err != nil {
		return err
	}
	defer rows.Close()

	mapper, err := NewMapper(rows, map[string]string{"param": "param", "value": "value"}, nil)
	if err != nil {
		return err
	}

	saved := make(map[string]bool)
	for rows.Next() {
		err = rows.Scan(mapper.Vals...)
		if err != nil {
			return err
		}
		// seek RV in memory
		for i := range pv {
			if pv[i].Param == mapper.stringByName("param") {
				sql := "update " + *pair.SyncTable + " set value = " + strconv.FormatInt(pv[i].Value, 10) + " where tbl = '" + *pair.Origin + "' and param = '" + pv[i].Param + "'"
				_, err := sync.Exec(sql)
				if err != nil {
					return err
				}
				saved[pv[i].Param] = true
			}
		}
	}
	err = rows.Err()
	if err != nil {
		return err
	}
	// insert absent
	for i := range pv {
		if _, ok := saved[pv[i].Param]; ok {
			continue
		}
		sql := "insert into " + *pair.SyncTable + " (tbl, param, value) values ('" + *pair.Origin + "','" + pv[i].Param + "'," + strconv.FormatInt(pv[i].Value, 10) + ")"
		_, err := sync.Exec(sql)
		if err != nil {
			return err
		}
	}

	return nil
}
