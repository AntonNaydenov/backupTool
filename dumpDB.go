package main

import (
	// "bufio"
	"database/sql"
	"fmt"
	"github.com/aliakseiz/go-mysqldump"
	"github.com/go-sql-driver/mysql"
	"os"
	"time"
)

func dumpDB(dbName string) error {
	config := mysql.NewConfig()
	config.User = "backup_user"
	config.Passwd = "Backup_user_PassWS%!0"
	config.DBName = dbName
	config.Net = "tcp"
	config.Addr = "10.50.2.151:3306"
	dumpDir := fmt.Sprintf("/home/mysql/dumps/%s", dbName)
	err := os.MkdirAll(dumpDir, os.ModePerm)
	if err != nil {
		fmt.Printf("Error creating directory: %v\n", err)
		return err
	}
	t := time.Now()
	fmt.Printf("Time now timestamp: %s", t.Format("20060102150405"))
	timeStamp := t.Format("20060102150405")
	dumpFilenameFormat := fmt.Sprintf("%s-%s", dbName, timeStamp)

	db, err := sql.Open("mysql", fmt.Sprintf("%s:%s@tcp(%s)/%s", config.User, config.Passwd, config.Addr, config.DBName))
	if err != nil {
		fmt.Println("Error opening database: ", err)
		return nil
	}

	dumper, err := mysqldump.Register(db, dumpDir, dumpFilenameFormat, dbName)

	if err != nil {
		fmt.Println("Error registering databse:", err)
		return nil
	}

	dumpErr := dumper.Dump()
	if dumpErr != nil {
		fmt.Println("Error dumping:", err)
		return nil
	}
	fmt.Println("File is saved")

	// Close dumper and connected database
	dumper.Close()

	return nil
}
