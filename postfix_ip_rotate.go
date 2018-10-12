package main

import (
    "os"
    "bufio"
    "strings"
    "fmt"
    "math/rand"
    "time"
    "database/sql"
    _ "github.com/lib/pq"
)

const(
    host = "127.0.0.1"
    port = 5432
    user = "postfix"
    password = "password"
    dbname = "postfix_transport"
)

var db *sql.DB

func main() {
    psqlInfo := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable", host, port, user, password, dbname)
    
    initDB(psqlInfo) 

    scanner := bufio.NewScanner(os.Stdin)
    sendSrvName := os.Args[1:]

    for scanner.Scan() {
        input := strings.Split(scanner.Text(), "@")

        if len(input) > 1 {
            domain := input[1]
            smtp, errc := getDomainRoute(sendSrvName, domain)
            if (smtp != "" && errc == 0) {
                fmt.Printf("200 smtp:[%s]\n", smtp)
            } else {
	        fmt.Printf("200 %s:\n", smtp)
            }
        } else {
           fmt.Println("200 smtp:")
        }
        
    }
}

func checkErr(err error) {
    if err != nil {
        panic(err)
    }
}

func initDB(connparam string) {
    var err error
    db, err = sql.Open("postgres", connparam)
    checkErr(err)

    err = db.Ping()
    checkErr(err)

}

func getDomainRoute(sendSrvIP []string, domain string)(string, int) {
    var smtp string

    err := db.QueryRow("select smtp from transport_map where domain='" + domain + "'").Scan(&smtp)

    if err == sql.ErrNoRows {
        return getRandSender(sendSrvIP), 10
    }
    checkErr(err)
   
    return smtp, 0

}

func getRandSender(sendSrvIP []string)(string) {
    rand.Seed(time.Now().Unix())
    return sendSrvIP[rand.Intn(len(sendSrvIP))]
}
