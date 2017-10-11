package manager

import (
	"database/sql"
	"github.com/ipdcode/containerfs/logger"
	"reflect"
)

var DB *sql.DB

// for DB insert/delete/update opreation
func Exec(sql string, args ...interface{}) (int, int64) {
	tx, err := DB.Begin()
	if err != nil {
		logger.Error("Db Begin error:%v for sql(%v)!", err, sql)
		return -1, -1
	}

	ret, err := tx.Exec(sql, args...)
	if err != nil {
		logger.Error("Db Exec error:%v for sql(%v)!", err, sql)
		return -1, -1
	}

	if err := tx.Commit(); err != nil {
		logger.Error("Db Commit error:%v for sql(%v)!", err, sql)
		return -1, -1
	}

	lastInsertId, err := ret.LastInsertId()
	if err != nil {
		logger.Error("Get LastInsertId error:%v", err)
		return -1, -1
	}
	return 0, lastInsertId
}

// for DB select opreation
func Select(sql string, struc interface{}, args ...interface{}) ([]interface{}, error) {
	rows, err := DB.Query(sql, args...)
	if err != nil {
		logger.Error("Db Query error:%v for sql(%v)!", err, sql)
		return nil, err
	}
	defer rows.Close()

	result := make([]interface{}, 0)
	s := reflect.ValueOf(struc).Elem()
	leng := s.NumField()
	onerow := make([]interface{}, leng)
	for i := 0; i < leng; i++ {
		onerow[i] = s.Field(i).Addr().Interface()
	}

	for rows.Next() {
		err = rows.Scan(onerow...)
		if err != nil {
			logger.Error("Vitess Db Scan error:%v for sql(%v)!", err, sql)
			return nil, err
		}
		result = append(result, s.Interface())
	}
	return result, nil
}
