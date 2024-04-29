package main

import (
	"context"
	_ "embed"
	"log"

	"github.com/gocarina/gocsv"
	"github.com/redis/go-redis/v9"
)

type Customer struct {
	Surname   string  `csv:"surname"  json:"surname"`
	FirstName string  `csv:"firstname" json:"firstname"`
	Email     string  `csv:"email" json:"email"`
	IP        string  `csv:"ip" json:"ip"`
	AccountNo int64   `csv:"accountno" json:"accountno"`
	Owner     string  `csv:"owner" json:"owner"`
	Balance   float64 `csv:"balance" json:"balance"`
}

//go:embed customers_test.csv
var customerData string

var indexCmd = []interface{}{"FT.CREATE", "demoindex", "ON", "JSON", "PREFIX", "1", "customer:", "SCORE", "1", "SCHEMA", "$.owner", "AS", "owner", "TAG", "SORTABLE", "$.accountno", "AS", "account", "NUMERIC", "SORTABLE"}

func main() {

	client := redis.NewClient(&redis.Options{Addr: "localhost:6379"})

	if err := client.Ping(context.Background()).Err(); err != nil {
		log.Fatalf("redis connection error: %+v\n", err)
	}

	client.FlushAll(context.Background())

	cmd := redis.NewCmd(context.Background(), indexCmd...)

	if cmd.Err() != nil {
		log.Fatalf("redis error creating index: %+v", cmd.Err())
	}

	if err := loadData(client); err != nil {
		log.Fatalf("error loading data: %+v", err)
	}

	// Assuming RESP2 here (go-redis uses RESP3 automatically if possible)

	/*
		for n := 1; n < len(rawResults.([]interface{})); n++ {
			result := map[string]interface{}{}
			for k, v := range internal.ToMap(r[n]) {
				result[k.(string)] = v
			}
			results = append(results, result)
		}
	*/
}

func loadData(client *redis.Client) error {

	customers := []*Customer{}

	if err := gocsv.UnmarshalString(customerData, &customers); err != nil { // Load clients from file
		log.Fatalf("csv error: %+v", err)
	}

	for _, customer := range customers {
		if err := client.JSONSet(context.Background(), "customer:"+customer.IP, "$", customer).Err(); err != nil {
			return err
		}
	}

	return nil
}
