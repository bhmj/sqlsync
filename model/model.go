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

// SyncPair represents a single job
type SyncPair struct {
	Source DBServer // optional
	Target DBServer // optional
	//
	Origin  *string           // source proc
	Dest    []*string         // destination proc
	Params  []string          // params for origin proc ("param=column")
	Mapping map[string]string // origin -> dest field mapping (field -> field)
	RowProc []SideOrigin      // proc to call for every row (on condition)
	//
	SourceLink *DBConnection
	TargetLink *DBConnection
	//
	Period Duration
	//
	ParamValues map[string]int64  // runtime
	ParamColumn map[string]string // runtime
	SyncTable   *string           // RV table on Target. Default is sync.sqlsync (tbl varchar, param varchar, val bigint)
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
