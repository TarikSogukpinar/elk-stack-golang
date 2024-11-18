package elastic

import (
	"fmt"
	"github.com/elastic/go-elasticsearch/v8"
	"log"
)

var Client *elasticsearch.TypedClient

func InitElasticClient() {
	elasticSearchConfiguration := elasticsearch.Config{
		Addresses: []string{
			"http://localhost:9200",
		},
		Username: "elastic",
		Password: "changeme",
	}
	client, err := elasticsearch.NewTypedClient(elasticSearchConfiguration)
	if err != nil {
		log.Fatalf("ElasticSearch client oluşturulamadı: %v", err)
	}

	var res = client.Ping()

	if res == nil {
		log.Fatalf("Elastic search connection error")
	}

	fmt.Println("Elasticsearch connection is ok!", res)

	Client = client
}
