package config

import (
	"encoding/json"
	"fmt"
	"os"
	"regexp"

	"github.com/bhmj/sqlsync/model"
)

// ReadConfig reads config
func ReadConfig(fname string) (cfg *model.Settings, err error) {

	println("reading", fname)
	conf, err := os.Open(fname)
	if err != nil {
		return
	}
	defer conf.Close()

	jsonParser := json.NewDecoder(conf)
	cfg = &model.Settings{}
	err = jsonParser.Decode(cfg)
	if err != nil {
		return
	}

	return cfg, ValidateConfig(cfg)
}

// ValidateConfig ...
func ValidateConfig(cfg *model.Settings) error {

	for i := 0; i < len(cfg.Sync); i++ {
		conns, err := CheckPair(cfg.Sync[i].Source, cfg.Sync[i].Target, cfg.Source, cfg.Target)
		if err != nil {
			return err
		}
		cfg.Sync[i].Source.Type = coalesceString(cfg.Sync[i].Source.Type, cfg.Source.Type)
		cfg.Sync[i].Target.Type = coalesceString(cfg.Sync[i].Target.Type, cfg.Target.Type)
		for c := 0; c < len(conns); c++ {
			found := -1
			for k := 0; k < len(cfg.Link); k++ {
				if cfg.Link[k].ConnString == conns[c] {
					found = k
					break
				}
			}
			if found == -1 {
				cfg.Link = append(cfg.Link, model.DBConnection{ConnString: conns[c]})
				found = len(cfg.Link) - 1
			}
			if c == 0 {
				cfg.Sync[i].SourceLink = &cfg.Link[found]
			} else {
				cfg.Sync[i].TargetLink = &cfg.Link[found]
			}
		}
		// TODO: validate params
		// propagate connections to row proc
		for p := 0; p < len(cfg.Sync[i].RowProc); p++ {
			for s := 0; s < len(cfg.Sync[i].RowProc[p].Sync); s++ {
				sub := &cfg.Sync[i].RowProc[p].Sync[s]
				sub.Source = cfg.Sync[i].Source
				sub.Target = cfg.Sync[i].Target
				sub.SourceLink = cfg.Sync[i].SourceLink
				sub.TargetLink = cfg.Sync[i].TargetLink
			}
		}
		// sync table parsing
		s := "sync.sqlsync"
		if cfg.Sync[i].SyncTable != nil {
			v1 := regexp.MustCompile(`^(src|dst)\.([\w\.]+)$`)
			v2 := regexp.MustCompile(`^(src|dst)$`)
			v3 := regexp.MustCompile(`^[\w+\.]+$`)
			if v1.MatchString(*cfg.Sync[i].SyncTable) {
				tokens := v1.FindStringSubmatch(*cfg.Sync[i].SyncTable)
				cfg.Sync[i].SyncTableSide = tokens[1]
				cfg.Sync[i].SyncTable = &tokens[2]
			} else if v2.MatchString(*cfg.Sync[i].SyncTable) {
				tokens := v2.FindStringSubmatch(*cfg.Sync[i].SyncTable)
				cfg.Sync[i].SyncTableSide = tokens[0]
				cfg.Sync[i].SyncTable = &s
			} else if v3.MatchString(*cfg.Sync[i].SyncTable) {
				tokens := v2.FindStringSubmatch(*cfg.Sync[i].SyncTable)
				cfg.Sync[i].SyncTableSide = "dst"
				cfg.Sync[i].SyncTable = &tokens[0]
			} else {
				return fmt.Errorf("invalid SyncTable: %s", *cfg.Sync[i].SyncTable)
			}
		}
		// MS SQL table type support
		cfg.Sync[i].TableType = make([]string, len(cfg.Sync[i].Dest))
		mstt := regexp.MustCompile(`^([\w\.]+)\s+(@([\w\.]+))$`)
		for d := 0; d < len(cfg.Sync[i].Dest); d++ {
			if mstt.MatchString(*cfg.Sync[i].Dest[d]) {
				tokens := mstt.FindStringSubmatch(*cfg.Sync[i].Dest[d])
				cfg.Sync[i].Dest[d] = &tokens[1]
				cfg.Sync[i].TableType[d] = tokens[3]
			}
		}
		// TODO: validate ColumnParam in RowProc to 1) non-nil 2) match column names to parent column set
		// TODO: validate Mapping in RowProc for @ columns to match to params
	}
	return nil
}

// CheckPair ...
func CheckPair(
	left model.DBServer,
	right model.DBServer,
	defLeft model.DBServer,
	defRight model.DBServer,
) (conns [2]string, err error) {
	typ := coalesceString(left.Type, defLeft.Type)
	host := coalesceString(left.Host, defLeft.Host)
	fovr := coalesceString(left.Failover, defLeft.Failover)
	port := coalesceInt(left.Port, defLeft.Port)
	db := coalesceString(left.DB, defLeft.DB)
	user := coalesceString(left.User, defLeft.User)
	pass := coalesceString(left.Password, defLeft.Password)
	conns[0], err = makeConn(typ, host, fovr, port, db, user, pass)
	if err != nil {
		return
	}
	typ = coalesceString(right.Type, defRight.Type)
	host = coalesceString(right.Host, defRight.Host)
	fovr = coalesceString(right.Failover, defRight.Failover)
	port = coalesceInt(right.Port, defRight.Port)
	db = coalesceString(right.DB, defRight.DB)
	user = coalesceString(right.User, defRight.User)
	pass = coalesceString(right.Password, defRight.Password)
	conns[1], err = makeConn(typ, host, fovr, port, db, user, pass)
	return
}

func coalesceString(left *string, right *string) *string {
	if left != nil {
		return left
	}
	return right
}

func coalesceInt(left *int, right *int) *int {
	if left != nil {
		return left
	}
	return right
}

func makeConn(typ *string, host *string, fovr *string, port *int, db *string, user *string, pass *string) (conn string, err error) {
	if typ == nil {
		return conn, fmt.Errorf("empty type")
	}
	if host == nil || *host == "" || db == nil || *db == "" || user == nil || *user == "" || pass == nil || *pass == "" {
		return "", fmt.Errorf("host, db, user, password are required")
	}
	switch *typ {
	case "mssql":
		sfovr := ""
		if fovr != nil && *fovr != "" {
			sfovr = "failoverpartner=" + *fovr + "; "
		}
		iport := 1433
		if port != nil && *port > 0 {
			iport = *port
		}
		conn = fmt.Sprintf("server=%s; %sdatabase=%s; port=%d; user id=%s; password=%s",
			*host, sfovr, *db, iport, *user, *pass)
	case "postgres":
		iport := 5432
		if port != nil && *port != 0 {
			iport = *port
		}
		conn = fmt.Sprintf("host=%s port=%d dbname=%s user=%s password=%s sslmode=disable",
			*host, iport, *db, *user, *pass)
	default:
		return "", fmt.Errorf("unsupported type: %s", *typ)
	}
	return
}
