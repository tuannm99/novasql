package main

import (
	"fmt"
	"time"

	"github.com/tuannm99/novasql/sqlclient"
)

func main() {
	c, err := sqlclient.Dial("127.0.0.1:8866", 2*time.Second)
	if err != nil {
		panic(err)
	}
	defer func() { _ = c.Close() }()

	c.SetRWTimeout(5 * time.Second)

	// _, _ = c.Exec("CREATE DATABASE testdb;")
	_, _ = c.Exec("USE testdb;")
	// _, _ = c.Exec("CREATE TABLE users (id INT, name TEXT, active BOOL);")
	// _, _ = c.Exec("INSERT INTO users VALUES (1, 'a', true);")

	res, err := c.Exec("SELECT * FROM users;")
	if err != nil {
		panic(err)
	}
	fmt.Println(res.Columns)
	fmt.Println(res.Rows)
}
