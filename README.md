# SQL sync microservice

Allows syncing between Postgres and MS SQL in any direction.
Can pull data from HTTP, MS SQL, Postgres.
Can receive HTTP requests with modified data / diffs.

## Usage

`go build .`  
`./sqlsync --config config.json`

## Config file format

```json
{
	"Source": { ... },  // common source, see below
	"Target": { ... },  // common target, see below
	"Sync": [
		{ /* sync pair, see below */ },
		...
	]
}
```

**Source**, **Target** :  
```json
{
	"Type":     "mssql",              // "postgres", "http" (required)
	"Host":     "riverside.wb.ru",    // hostname (required)
	"Failover": "springfield.wb.ru",  // failover (optional)
	"Port":     "1433",               // db port (optional)
	"DB":       "dummy_db",           // database name (required)
	"User":     "username",           // username (required)
	"Password": "password"            // password (required)
}
```

**Sync pair** 
```json
{
	"Source": { ... },  // optional, common used if omitted
	"Target": { ... },  // optional, common used if omitted

	"Period": "10s",             // call period (Golang notation)

	"Origin": "foo.get_data",    // stored procedure on source
	"Dest":   ["bar.set_data"],  // stored procedure on destination
	"ColumnParam": [             // params for procedure on source
		{ 
			"Column": "rv",           // column name to get values from
			"Param":  "last_seen_rv", // param name for source procedure
			"Output": "true"          // optional, receives value from SP if set to true
		}
	],
	"Mapping": {                 // optional
		"user_id":     "uid",    //
		"first_name":  "fname",  // field name mapping (source -> destination)
		"middle_name": "mname",  //
		"last_name":   "lname"   // 
	},

	"RowProc": [ { ... } ]       // optional, see below
}
```

**RowProc**
```json
{
	"Condition": "1==1", // use "@.field == value" notation
	"Sync": [
		{ /* sync pair, see above */ }
	]
}
```