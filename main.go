package main

import (
	"fmt"
	"golang.org/x/net/context"
	elastic "gopkg.in/olivere/elastic.v5"
	"time"
	"strconv"
)

// Tweet is a structure used for serializing/deserializing data in Elasticsearch.
type Tweet struct {
	User     string                `json:"user"`
	Message  string                `json:"message"`
	Retweets int64                 `json:"retweets"`
	Image    string                `json:"image,omitempty"`
	Created  time.Time             `json:"created,omitempty"`
	Tags     []string              `json:"tags,omitempty"`
	Location string                `json:"location,omitempty"`
	Suggest  *elastic.SuggestField `json:"suggest_field,omitempty"`
}

func main() {
	// Starting with elastic.v5, you must pass a context to execute each service
	ctx := context.Background()

	client, err := elastic.NewClient()
	if err != nil {
		// Handle error
		panic(err)
	}

	// Ping the Elasticsearch server to get e.g. the version number
	info, code, err := client.Ping("http://127.0.0.1:9200").Do(ctx)
	if err != nil {
		// Handle error
		panic(err)
	}
	fmt.Printf("Elasticsearch returned with code %d and version %s", code, info.Version.Number)

	// Getting the ES version number is quite common, so there's a shortcut
	esversion, err := client.ElasticsearchVersion("http://127.0.0.1:9200")
	if err != nil {
		// Handle error
		panic(err)
	}
	fmt.Printf("Elasticsearch version %s", esversion)

	// Use the IndexExists service to check if a specified index exists.
	exists, err := client.IndexExists("twitter-weekly").Do(ctx)
	if err != nil {
		// Handle error
		panic(err)
	}
	if !exists {
		// Create a new index.
		createIndex, err := client.CreateIndex("twitter-weekly").Do(ctx)
		if err != nil {
			// Handle error
			panic(err)
		}
		if !createIndex.Acknowledged {
			// Not acknowledged
		}
	}


	// Create an aggregation for users and a sub-aggregation for a date histogram of tweets (per year).
	timeline := elastic.NewTermsAggregation().Field("user.keyword").Size(20).OrderByCountDesc()
	histogram := elastic.NewDateHistogramAggregation().Field("@timestamp").Interval("week")
	timeline = timeline.SubAggregation("history", histogram)
	// Search with a term query
	searchResult, err := client.Search().
		Index("logstash-twitter").                  // search in index "twitter"
		Query(elastic.NewMatchAllQuery()). // return all results, but ...
		SearchType("query_then_fetch").                // ... do not return hits, just the count
		Aggregation("timeline", timeline). // add our aggregation to the query
		Pretty(true).                      // pretty print request and response JSON
		Do(context.Background())           // execute
	if err != nil {
		// Handle error
		panic(err)
	}

	// Access "timeline" aggregate in search result.
	agg, found := searchResult.Aggregations.Terms("timeline")
	if !found {
		fmt.Printf("we should have a terms aggregation called %q", "timeline")
	}

	// fmt.Printf("agg.Buckets => %q", agg.Buckets)

  count :=1
	for _, userBucket := range agg.Buckets {
		// fmt.Printf	("userBucket => %q", userBucket)
		// Every bucket should have the user field as key.
		user := userBucket.Key

		// The sub-aggregation history should have the number of tweets per year.
		histogram, found := userBucket.DateHistogram("history")

		if found {
			for _, year := range histogram.Buckets {
				fmt.Printf("user %q has %d tweets in %q\n", user, year.DocCount, year.KeyAsString)

				if user_str, ok := user.(string); ok {
					// Index a tweet (using JSON serialization)
					tweet1 := Tweet{User: user_str, Message: "weekly", Retweets: year.DocCount, Created: time.Now()}
					put1, err := client.Index().
						Index("twitter-weekly").
						Type("tweet").
						Id(strconv.Itoa(count)).
						BodyJson(tweet1).
						Do(context.Background())
						count++
					if err != nil {
						// Handle error
						panic(err)
					}
					fmt.Printf("Indexed tweet( %s )to index %s, type %s\n", put1.Id, put1.Index, put1.Type)

				}
			}
		}
	}
}
