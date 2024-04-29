package main

import (
	"context"
	_ "embed"
	"fmt"
	"log"
	"strings"

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

var indexArgs = []interface{}{"FT.CREATE", "demoindex", "ON", "JSON", "PREFIX", "1", "customer:", "SCORE", "1", "SCHEMA", "$.owner", "AS", "owner", "TAG", "SORTABLE", "$.accountno", "AS", "account", "NUMERIC", "SORTABLE"}
var searchArgs = []interface{}{"FT.SEARCH", "demoindex", "@owner:{lara\\.croft}", "SLOP", "0", "DIALECT", "3"}
var aggregateArgs = []interface{}{"FT.AGGREGATE", "demoindex", "*", "load", "3", "$balance", "as", "balance", "GROUPBY", "1", "@owner", "reduce", "sum", "1", "balance", "as", "balance", "SORTBY", "2", "@balance", "DESC", "dialect", "3"}

func main() {

	client := redis.NewClient(&redis.Options{Addr: "localhost:6379", Protocol: 2})

	if err := client.Ping(context.Background()).Err(); err != nil {
		log.Fatalf("redis connection error: %+v\n", err)
	}

	client.FlushAll(context.Background())

	cmd := redis.NewCmd(context.Background(), indexArgs...)
	client.Process(context.Background(), cmd)

	if cmd.Err() != nil {
		log.Fatalf("redis error creating index: %+v", cmd.Err())
	}

	if err := loadData(client); err != nil {
		log.Fatalf("error loading data: %+v", err)
	}

	searchCmd := redis.NewSliceCmd(context.Background(), searchArgs...)
	client.Process(context.Background(), searchCmd)

	fmt.Println("\n\nsearch for customers owned by lara croft")

	if searchCmd.Err() != nil {
		log.Fatalf("redis error executing search: %+v", cmd.Err())
	}

	searchResults := searchCmd.Val()

	for n := 1; n < len(searchResults); n += 2 {
		key := searchResults[n].(string)
		val := searchResults[n+1].([]interface{})[1].(string)
		fmt.Printf("%s:\t\t%s\n", key, val)
	}

	fmt.Println("\n\naggregate balance by owner: FT.AGGREGATE")

	aggregateCmd := redis.NewSliceCmd(context.Background(), aggregateArgs...)
	client.Process(context.Background(), aggregateCmd)

	if aggregateCmd.Err() != nil {
		log.Fatalf("redis error executing aggregate: %+v", cmd.Err())
	}

	aggregateResults := aggregateCmd.Val()

	titles := []string{}
	r1 := aggregateResults[1].([]interface{})
	for n := 0; n < len(r1); n += 2 {
		titles = append(titles, r1[n].(string))
	}
	fmt.Println(strings.Join(titles, ", "))

	for n := 1; n < len(aggregateResults); n++ {
		r := aggregateResults[n].([]interface{})
		values := []string{}
		for m := 1; m < len(r); m += 2 {
			values = append(values, r[m].(string))
		}
		fmt.Println(strings.Join(values, ", "))
	}

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
