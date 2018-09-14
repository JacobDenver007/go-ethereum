package testcount

import (
	"database/sql"
	"fmt"
	"net/url"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

var (
	StatedbReadCount   int64
	StatedbWriteCount  int64
	StatedbDeleteCount int64

	LeveldbGetCount   int64
	LeveldbHasCount   int64
	LeveldbWriteCount int64

	BatchLeveldbPutCount   int64
	BatchLeveldbWriteCount int64

	StatedbReadTime   time.Duration
	StatedbWriteTime  time.Duration
	StatedbDeleteTime time.Duration

	LeveldbGetTime   time.Duration
	LeveldbHasTime   time.Duration
	LeveldbWriteTime time.Duration

	BatchLeveldbPutTime   time.Duration
	BatchLeveldbWriteTime time.Duration

	SqlDB *DB
)

var createSQL = `
CREATE TABLE IF NOT EXISTS t_countinfo (
	id INTEGER(11)       PRIMARY KEY AUTO_INCREMENT,
	i_height             INTEGER(11) NOT NULL,
	i_statedbReadCount              INTEGER(11),
	i_statedbReadAverage           INTEGER(11),
	i_statedbWriteCount INTEGER(11),
	i_statedbWriteAverage INTEGER(11),
	i_statedbDeleteCount INTEGER(11),
	i_statedbDeleteAverage INTEGER(11),
	i_leveldbGetCount INTEGER(11),
	i_leveldbGetAverage INTEGER(11),
	i_leveldbHasCount INTEGER(11),
	i_leveldbHasAverage INTEGER(11),
	i_leveldbWriteCount INTEGER(11),
	i_leveldbWriteAverage INTEGER(11),
	i_batchLeveldbPutCount INTEGER(11),
	i_batchLeveldbPutTime INTEGER(11),
	i_batchLeveldbWriteCount INTEGER(11),
	i_batchLeveldbWriteTime INTEGER(11)
);
`

func Start() {
	StatedbReadCount = 0
	StatedbWriteCount = 0
	StatedbDeleteCount = 0

	LeveldbGetCount = 0
	LeveldbHasCount = 0
	LeveldbWriteCount = 0

	BatchLeveldbPutCount = 0
	BatchLeveldbWriteCount = 0

	StatedbReadTime = 0
	StatedbWriteTime = 0
	StatedbDeleteTime = 0

	LeveldbGetTime = 0
	LeveldbHasTime = 0
	LeveldbWriteTime = 0

	BatchLeveldbPutTime = 0
	BatchLeveldbWriteTime = 0

}

func OpenDB() {
	SqlDB := &DB{}
	SqlDB.Open()
}

type DB struct {
	sqlDB *sql.DB
}

func (db *DB) execSQL(sqlStr string) error {
	//log.Debugf("ExecSQL: %s", sqlStr)
	sqlStrs := strings.Split(sqlStr, ";")
	tx, err := db.sqlDB.Begin()
	if err != nil {
		return err
	}
	defer func() {
		if tx != nil {
			tx.Rollback()
		}
	}()
	for _, sqlStr := range sqlStrs {
		sqlStr = strings.TrimSpace(sqlStr)
		if len(sqlStr) != 0 {
			if _, err := tx.Exec(fmt.Sprintf("%s;", sqlStr)); err != nil {
				return fmt.Errorf("%s - %s", sqlStr, err)
			}
		}
	}
	err = tx.Commit()
	if err == nil {
		tx = nil
	}
	return err
}

func (db *DB) Open() error {
	//mysql
	sdb, err := sql.Open("mysql", fmt.Sprintf("%s:%s@tcp(%s)/%s?charset=utf8&loc=%s&parseTime=true",
		"root", "Bochen@123", "127.0.0.1:3306", "chain", url.QueryEscape("Asia/Shanghai")))
	if err != nil {
		return err
	}
	sdb.SetMaxOpenConns(2000)
	sdb.SetMaxIdleConns(2000)
	sdb.SetConnMaxLifetime(60 * time.Second)
	db.sqlDB = sdb
	if err := db.execSQL(createSQL); err != nil {
		sdb.Close()
		return err
	}
	return nil
}

func (db *DB) InsertCountInfo(height int64) {
	sqlStr := fmt.Sprintf(`"INSERT INTO t_countinfo(i_height, i_statedbReadCount, 
	i_statedbReadAverage,
	i_statedbWriteCount,
	i_statedbWriteAverage,
	i_statedbDeleteCount,
	i_statedbDeleteAverage,
	i_leveldbGetCount,
	i_leveldbGetAverage,
	i_leveldbHasCount,
	i_leveldbHasAverage,
	i_leveldbWriteCount,
	i_leveldbWriteAverage,
	i_batchLeveldbPutCount,
	i_batchLeveldbPutTime,
	i_batchLeveldbWriteCount,
	i_batchLeveldbWriteTime) values(%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d)"`,
		height, StatedbReadCount, StatedbReadTime,
		StatedbWriteCount, StatedbWriteTime,
		StatedbDeleteCount, StatedbDeleteTime,
		LeveldbGetCount, LeveldbGetTime,
		LeveldbHasCount, LeveldbHasTime,
		LeveldbWriteCount, LeveldbWriteTime,
		BatchLeveldbPutCount, BatchLeveldbPutTime,
		BatchLeveldbWriteCount, BatchLeveldbWriteTime)

	db.execSQL(sqlStr)
}
