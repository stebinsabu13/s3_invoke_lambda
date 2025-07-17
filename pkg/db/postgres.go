package db

import (
	"database/sql"
	"fmt"
	"log"

	_ "github.com/lib/pq"
)

var dbInstance *sql.DB

func InitPostgres(host, port, user, password, dbname string) error {
	connStr := fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=require",
		host, port, user, password, dbname)

	var err error
	dbInstance, err = sql.Open("postgres", connStr)
	if err != nil {
		return fmt.Errorf("error connecting to PostgreSQL: %v", err)
	}

	err = dbInstance.Ping()
	if err != nil {
		return fmt.Errorf("error pinging PostgreSQL: %v", err)
	}

	log.Println("Successfully connected to PostgreSQL")
	return nil
}

func GetPostgres() *sql.DB {
	return dbInstance
}
