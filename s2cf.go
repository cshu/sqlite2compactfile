package main

import (
	"database/sql"
	"encoding/binary"
	"fmt"
	_ "github.com/mattn/go-sqlite3"
	rs "github.com/cshu/golangrs"
	"log"
	"math"
	"os"
)

var db *sql.DB
var rows *sql.Rows

func main() {
	dbfilenm := os.Getenv("DB_FILE")
	if dbfilenm == "" {
		log.Println(`DB_FILE not set`)
		return
	}
	outfilenm := os.Getenv("OUT_FILE")
	if outfilenm == "" {
		log.Println(`OUT_FILE not set`)
		return
	}
	if len(os.Args) < 2 {
		log.Println(`No query supplied`)
		return
	}
	querystr := os.Args[1]
	var err error
	db, err = sql.Open("sqlite3", `file:`+dbfilenm+`?_locking_mode=EXCLUSIVE&mode=ro&_cache_size=90000000&_journal_mode=MEMORY&_synchronous=OFF`)
	rs.CheckErr(err)
	rs.DbExecSql(db, `PRAGMA synchronous = OFF`)
	rs.DbExecSql(db, `PRAGMA journal_mode = MEMORY`)
	rs.DbExecSql(db, `PRAGMA cache_size = 90000000`)
	rs.DbExecSql(db, `PRAGMA temp_store = MEMORY`)
	rs.DbExecSql(db, `PRAGMA automatic_index = 0`)
	rs.DbExecSql(db, `PRAGMA mmap_size = 30000000000`)
	rows, err = db.Query(querystr)
	rs.CheckErr(err)
	var p1 float64
	var p2 int64
	var outFile *os.File
	outFile, err = os.Create(outfilenm)
	rs.CheckErr(err)
	defer func() {
		err := outFile.Close()
		rs.CheckErr(err)
	}()
	for rows.Next() {
		err = rows.Scan(&p1, &p2)
		rs.CheckErr(err)
		//
		var bytesBuf []byte = make([]byte, 16, 16)
		bytesBuf1 := bytesBuf[:8]
		bytesBuf2 := bytesBuf[8:]
		binary.LittleEndian.PutUint64(bytesBuf1, math.Float64bits(p1))
		binary.LittleEndian.PutUint64(bytesBuf2, uint64(p2))
		_, err = outFile.Write(bytesBuf)
		rs.CheckErr(err)
	}
	rs.CheckErrWithoutPanic(rows.Err())
	rows.Close()
	db.Close()
	fmt.Println(`All Done`)
}
