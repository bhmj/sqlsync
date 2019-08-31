package syncer

import (
	"database/sql"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"regexp"
	"strconv"

	"github.com/bhmj/jsonslice"
	"github.com/bhmj/sqlsync/model"
	_ "github.com/denisenkom/go-mssqldb" // MS SQL driver
	_ "github.com/lib/pq"                // Postgres driver
)

var validParam *regexp.Regexp

func init() {
	validParam = regexp.MustCompile(`^([A-za-z_]+)=([A-za-z_]+)$`)
}

type processor func(src *sql.DB, dst *sql.DB, pair *model.SyncPair, level int) error

func identPrintf(level int, format string, a ...interface{}) {
	v := append([]interface{}{"- "}, a...)
	fmt.Printf("%"+strconv.Itoa(level*2)+"s"+format, v...)
}

// DoSync ...
func DoSync(pair *model.SyncPair) {
	process(pair, doSync)
}

// Init ...
func Init(pair *model.SyncPair) {
	process(pair, doInit)
}

func process(pair *model.SyncPair, fn processor) error {

	srcType := *pair.Source.Type
	dstType := *pair.Target.Type

	src, err := sql.Open(srcType, pair.SourceLink.ConnString)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error in %s: %s\n", *pair.Origin, err.Error())
		return err
	}
	defer src.Close()

	dst, err := sql.Open(dstType, pair.TargetLink.ConnString)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error in %s: %s\n", *pair.Origin, err.Error())
		return err
	}
	defer dst.Close()

	err = fn(src, dst, pair, 0)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error processing query in %s: %s\n", *pair.Origin, err.Error())
	}
	return err
}

func doSync(src *sql.DB, dst *sql.DB, pair *model.SyncPair, level int) (err error) {
	//dstType := *pair.Target.Type

	rows, err := src.Query(buildQuery(pair))
	if err != nil {
		return
	}
	defer rows.Close()

	pv := pair.ParamValues

	fmt.Println("")
	args := ""
	for k, v := range pv {
		if len(args) > 0 {
			args += ", "
		}
		args += "@" + k + "=" + strconv.FormatInt(v, 10)
	}
	identPrintf(level, "%s %s", *pair.Origin, args)

	recordset := 0
	for {
		mapper, err := NewMapper(rows, pair.Mapping, pair.ParamValues)
		if err != nil {
			return err
		}

		heap := make([]interface{}, 0)
		nrows := 0

		for rows.Next() {
			err = rows.Scan(mapper.Vals...)
			if err != nil {
				return err
			}
			nrows++
			// update RVs
			for k, v := range pv {
				nv := mapper.int64ByName(pair.ParamColumn[k])
				if nv > v {
					pv[k] = nv
				}
			}
			// process data
			if len(pair.RowProc) > 0 {
				// process row
				storeData(dst, pair, recordset, []interface{}{mapper.getRow()}, pv)
				for p := 0; p < len(pair.RowProc); p++ {
					side := &pair.RowProc[p]
					if side.Condition != "" {
						js, err := json.Marshal(mapper.getRow())
						if err != nil {
							return err
						}
						cond := "$[?(" + side.Condition + ")]"
						result, err := jsonslice.Get([]byte("["+string(js)+"]"), cond)
						if err != nil {
							return err
						}
						if string(result) == "[]" {
							continue
						}
					}
					for i := 0; i < len(side.Sync); i++ {
						sp := &side.Sync[i]
						if sp.ParamValues == nil {
							sp.ParamValues = make(map[string]int64)
						}
						// set params
						for ip := 0; ip < len(sp.Params); ip++ {
							prm := validParam.FindStringSubmatch(sp.Params[ip])
							val := mapper.int64ByName(prm[2])
							sp.ParamValues[prm[1]] = val // real deal
						}
						err := doSync(src, dst, sp, level+1) // nested
						if err != nil {
							return err
						}
					}
				}
			} else {
				heap = append(heap, mapper.copyRow())
			}
		}
		fmt.Printf(" : %d", nrows)
		if len(heap) > 0 {
			if nrows > 10000 {
				print("")
			}
			storeData(dst, pair, recordset, heap, pv)
		}
		if !rows.NextResultSet() {
			break
		}
		recordset++
		fmt.Println("")
		identPrintf(level, "recordset %d\n", recordset)
	}

	pair.ParamValues = pv

	return
}

func doInit(src *sql.DB, dst *sql.DB, pair *model.SyncPair, level int) error {
	tbl := "sync.sqlsync"
	if pair.SyncTable != nil {
		tbl = *pair.SyncTable
	}
	ph := "?"
	if *pair.Target.Type == "postgres" {
		ph = "$1"
	}
	rows, err := dst.Query("select * from "+tbl+" where tbl = "+ph, *pair.Origin)
	if err != nil {
		return err
	}
	defer rows.Close()

	mapper, err := NewMapper(rows, map[string]string{"param": "param", "value": "value"}, nil)
	if err != nil {
		return err
	}

	pair.ParamValues = make(map[string]int64)
	pair.ParamColumn = make(map[string]string)
	for p := 0; p < len(pair.Params); p++ {
		prm := validParam.FindStringSubmatch(pair.Params[p])
		pair.ParamValues[prm[1]] = 0 // init with 0
		pair.ParamColumn[prm[1]] = prm[2]
	}

	for rows.Next() {
		err = rows.Scan(mapper.Vals...)
		if err != nil {
			return err
		}
		dbParam := mapper.stringByName("param")
		// through config params
		for p := 0; p < len(pair.Params); p++ {
			prm := validParam.FindStringSubmatch(pair.Params[p])
			if prm[1] == dbParam {
				val := mapper.int64ByName("value")
				pair.ParamValues[dbParam] = val // real deal
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
func NewMapper(rows *sql.Rows, mapping map[string]string, pv map[string]int64) (*Mapper, error) {
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
	for colsField, dstField := range mapping {
		if colsField[:1] == "@" {
			mapper.PVals = append(mapper.PVals, pv[colsField[1:]])
			mapper.Map[dstField] = -len(mapper.PVals)
			continue
		}
		found := false
		for c := 0; c < len(cols); c++ {
			if colsField == cols[c] {
				mapper.Map[dstField] = c
				found = true
				break
			}
		}
		if !found {
			fmt.Println(colsField, "not found")
		}
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

func buildQuery(pair *model.SyncPair) (query string) {
	switch *pair.Source.Type {
	case "postgres":
		query = "select * from " + *pair.Origin + "("
		n := 0
		for p := range pair.Params {
			if n > 0 {
				query += ", "
			}
			prm := validParam.FindStringSubmatch(pair.Params[p])
			val := pair.ParamValues[prm[1]]
			query += strconv.FormatInt(val, 10)
			n++
		}
		query += ")"
	case "mssql":
		query = "EXEC " + *pair.Origin
		n := 0
		for p := range pair.Params {
			if n > 0 {
				query += ","
			}
			prm := validParam.FindStringSubmatch(pair.Params[p])
			val := pair.ParamValues[prm[1]]
			query += " @" + prm[1] + "=" + strconv.FormatInt(val, 10)
			n++
		}
	}
	return
}

func storeData(dst *sql.DB, pair *model.SyncPair, recordset int, heap []interface{}, pv map[string]int64) error {

	if recordset >= len(pair.Dest) {
		fmt.Println("not enough Dest procedures (extra recordset(s) encountered) in", *pair.Origin)
		return nil
	}
	query := ""
	switch *pair.Target.Type {
	case "postgres":
		query = "select * from " + *pair.Dest[recordset] + "($1, $2, $3)"
	case "mssql":
		return errors.New("MS SQL is not supported as a target")
	}

	js, err := json.Marshal(heap)
	if err != nil {
		return err
	}
	rvs, err := json.Marshal(pv)
	if err != nil {
		return err
	}

	// fmt.Printf("%s\n%s\n%s\n", query, string(js), string(rvs))

	rows, err := dst.Query(query, js, rvs, pair.Origin)
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		str := ""
		err = rows.Scan(&str)
		if err != nil {
			return err
		}
		// fmt.Println(str)
	}

	return nil
}
