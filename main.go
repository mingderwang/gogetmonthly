package main

import (
	"fmt"
	"golang.org/x/net/context"
	elastic "gopkg.in/olivere/elastic.v5"
	"time"
)

// Tweet is a structure used for serializing/deserializing data in Elasticsearch.
type Tweet struct {
	User     string                `json:"user"`
	Message  string                `json:"message"`
	Retweets int                   `json:"retweets"`
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
	exists, err := client.IndexExists("twitter").Do(ctx)
	if err != nil {
		// Handle error
		panic(err)
	}
	if !exists {
		// Create a new index.
		createIndex, err := client.CreateIndex("twitter").Do(ctx)
		if err != nil {
			// Handle error
			panic(err)
		}
		if !createIndex.Acknowledged {
			// Not acknowledged
		}
	}

	get1, err := client.Get().
		Index("logstash-twitter").
		Type("logs").
		Id("AVsDYvgq978bpCTDrGz6").
		Do(ctx)
	if err != nil {
		// Handle error
		panic(err)
	}
	if get1.Found {
		fmt.Printf("Got document %s in version %d from index %s, type %s\n", get1.Id, get1.Version, get1.Index, get1.Type)
	}

	// Flush to make sure the documents got written.
	_, err = client.Flush().Index("logstash-twitter").Do(ctx)
	if err != nil {
		panic(err)
	}

	// Update a tweet by the update API of Elasticsearch.
	// We just increment the number of retweets.
	update, err := client.Update().Index("logstash-twitter").Type("logs").Id("AVsDdqYX978bpCTDrHco").
		Script(elastic.NewScriptInline("ctx._source.retweeted = params.num").Lang("painless").Param("num", true)).
		Upsert(map[string]interface{}{"retweets": 0}).
		Do(ctx)
	if err != nil {
		// Handle error
		panic(err)
	}
	fmt.Printf("New version of tweet %q is now %d\n\n", update.Id, update.Version)

	// Create an aggregation for users and a sub-aggregation for a date histogram of tweets (per year).
	timeline := elastic.NewTermsAggregation().Field("user.keyword").Size(10).OrderByCountDesc()
	histogram := elastic.NewDateHistogramAggregation().Field("@timestamp").Interval("hour")
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
	for _, userBucket := range agg.Buckets {
		// Every bucket should have the user field as key.
		user := userBucket.Key

		// The sub-aggregation history should have the number of tweets per year.
		histogram, found := userBucket.DateHistogram("history")
		if found {
			for _, year := range histogram.Buckets {
				fmt.Printf("user %q has %d tweets in %q\n", user, year.DocCount, year.KeyAsString)
			}
		}
	}
}
