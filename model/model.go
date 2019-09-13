package model

import (
	"encoding/json"
	"errors"
	"sync"
	"time"
)

// Duration ...
type Duration struct {
	time.Duration
}

// DBServer stores server info
type DBServer struct {
	Type     *string // mssql, postgre, http
	Host     *string
	Failover *string
	Port     *int
	DB       *string
	User     *string
	Password *string
}

// SideOrigin ...
type SideOrigin struct {
	Condition string     // jsonpath
	Sync      []SyncPair // Params contain params for origin proc
}

// ColumnParamValue ...
type ColumnParamValue struct {
	Column string
	Param  string
	Value  int64
	BigEnd bool
	Output bool
}

// SyncPair represents a single job
type SyncPair struct {
	sync.Mutex
	Source DBServer // optional
	Target DBServer // optional
	//
	Origin      *string            // source proc
	Dest        []*string          // destination proc
	ColumnParam []ColumnParamValue // params for origin proc ("column => param (value)")
	Mapping     map[string]string  // origin -> dest field mapping (field -> field)
	RowProc     []SideOrigin       // proc to call for every row (on condition)
	//
	SourceLink *DBConnection
	TargetLink *DBConnection
	//
	Period Duration
	//
	SyncTable     *string  // RV table name & location. Default is dst.sync.sqlsync (tbl varchar, param varchar, val bigint)
	SyncTableSide string   // runtime: src or dst
	TableType     []string // runtime: table type
}

// Settings holds all the parameters for the syncer
type Settings struct {
	sync.RWMutex
	Source DBServer // common
	Target DBServer // common
	Sync   []SyncPair
	// aux
	Link []DBConnection
}

// DBConnection ...
type DBConnection struct {
	ConnString string
	//DB         *sql.DB
}

// MarshalJSON ...
func (d Duration) MarshalJSON() ([]byte, error) {
	return json.Marshal(d.String())
}

// UnmarshalJSON ...
func (d *Duration) UnmarshalJSON(b []byte) error {
	var v interface{}
	if err := json.Unmarshal(b, &v); err != nil {
		return err
	}
	switch value := v.(type) {
	case float64:
		d.Duration = time.Duration(value)
		return nil
	case string:
		var err error
		d.Duration, err = time.ParseDuration(value)
		if err != nil {
			return err
		}
		return nil
	default:
		return errors.New("invalid duration")
	}
}
