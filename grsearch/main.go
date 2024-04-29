package main

import (
	"context"
	_ "embed"
	"fmt"
	"log"
	"strings"

	"github.com/gocarina/gocsv"
	"github.com/goslogan/grsearch"
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

func main() {

	options := redis.Options{
		Addr: "localhost:6379",
	}
	client := grsearch.NewClient(&options)

	if err := client.Ping(context.Background()).Err(); err != nil {
		log.Fatalf("redis error: %+v", err)
	}

	client.FlushAll(context.Background())

	indexOptions := grsearch.NewIndexBuilder().On("json").
		Schema(&grsearch.TagAttribute{Name: "$.owner", Alias: "owner", Sortable: true}).
		Schema(&grsearch.NumericAttribute{Name: "$.accountno", Alias: "account", Sortable: true}).
		Prefix("customer:").Options()

	if err := client.FTCreate(context.Background(), "demoindex", indexOptions).Err(); err != nil {
		log.Fatalf("index creation error: %+v", err)
	}

	if err := loadData(client); err != nil {
		log.Fatalf("error loading data: %+v", err)
	}

	// Do a regular search
	fmt.Println("\n\nsearch for customers owned by lara croft")

	cmd := client.FTSearchJSON(context.Background(), "demoindex", `@owner:{lara\.croft}`, &grsearch.QueryOptions{Dialect: 3})
	if cmd.Err() != nil {
		log.Fatalf("error searching: %+v", cmd.Err())
	} else {
		for _, key := range cmd.Keys() {
			fmt.Printf("%s:\t\t%s\n", key, cmd.Key(key).Values["$"])
		}
	}

	// Aggregation.
	fmt.Println("\n\naggregate balance by owner: FT.AGGREGATE")
	builder := grsearch.NewAggregateBuilder().
		Dialect(3).
		Load("$balance", "balance").
		GroupBy(grsearch.NewGroupByBuilder().
			Property("@owner").
			Reduce(grsearch.ReduceSum("balance", "balance")).
			GroupBy()).SortBy([]grsearch.AggregateSortKey{{Name: "balance", Order: "DESC"}})
	ar, err := client.FTAggregate(context.Background(), "demoindex", "*", builder.Options()).Result()
	if err != nil {
		log.Fatalf("error running aggregate: %+v", err)
	} else {
		titles := []string{}
		for k := range ar[0] {
			titles = append(titles, k)
		}
		fmt.Println(strings.Join(titles, ", "))
		for _, m := range ar {
			values := []string{}
			for _, k := range titles {
				values = append(values, m[k].(string))
			}
			fmt.Println(strings.Join(values, ", "))

		}
	}

}

func loadData(client *grsearch.Client) error {

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
