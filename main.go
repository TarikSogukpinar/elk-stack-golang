package main

import (
	"context"
	"elk-stack-golang/elastic"
	"fmt"
	"github.com/elastic/go-elasticsearch/v8/typedapi/core/search"
	"github.com/elastic/go-elasticsearch/v8/typedapi/some"
	"github.com/elastic/go-elasticsearch/v8/typedapi/types"
	"github.com/gofiber/fiber/v2"
)

func main() {
	elastic.InitElasticClient()
	app := fiber.New()

	app.Get("/flights", func(c *fiber.Ctx) error {
		if elastic.Client == nil {
			return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
				"error": "ElasticSearch client is not initialized",
			})
		}

		res, err := elastic.Client.Search().Index("kibana_sample_data_flights").Request(&search.Request{
			Query: &types.Query{MatchAll: &types.MatchAllQuery{}},
		}).Do(context.TODO())

		if err != nil {
			return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
				"error": fmt.Sprintf("ElasticSearch query error: %v", err),
			})
		}

		if res == nil || len(res.Hits.Hits) == 0 {
			return c.Status(fiber.StatusNotFound).JSON(fiber.Map{
				"error": "No documents found",
			})
		}

		return c.JSON(res.Hits.Hits)
	})

	app.Get("/flights/dest-country/:country", func(c *fiber.Ctx) error {
		country := c.Params("country")
		res, err := elastic.Client.Search().
			Index("kibana_sample_data_flights").
			Request(&search.Request{
				Query: &types.Query{
					Match: map[string]types.MatchQuery{
						"DestCountry": {
							Query: country,
						},
					},
				},
			}).
			Do(context.Background())

		if err != nil {
			return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
				"error": fmt.Sprintf("ElasticSearch query error: %v", err),
			})
		}

		fmt.Println(res)
		return c.JSON(res.Hits.Hits)
	})

	app.Get("/flights/distance", func(c *fiber.Ctx) error {
		minDistance := c.Query("min", "0")
		maxDistance := c.Query("max", "10000")

		res, err := elastic.Client.Search().
			Index("kibana_sample_data_flights").
			Request(&search.Request{
				Size: some.Int(0),
				Aggregations: map[string]types.Aggregations{
					"minDistance": {
						Min: &types.MinAggregation{
							Field: some.String(minDistance),
						},
					},
					"maxDistance": {
						Max: &types.MaxAggregation{
							Field: some.String(maxDistance),
						},
					},
				},
			}).
			Do(context.Background())

		if err != nil {
			return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
				"error": fmt.Sprintf("ElasticSearch query error: %v", err),
			})
		}

		if res == nil || len(res.Hits.Hits) == 0 {
			return c.Status(fiber.StatusNotFound).JSON(fiber.Map{
				"error": "No documents found",
			})
		}

		return c.JSON(res.Hits.Hits)
	})

	//app.Get("/flights/aggregations/dest-country", func(c *fiber.Ctx) error {
	//	agg := map[string]types.Aggregations{
	//		"dest_country_count": {
	//			Terms: &types.TermsAggregation{
	//				Field: "DestCountry.keyword",
	//			},
	//		},
	//	}
	//
	//	res, err := elastic.Client.Search().
	//		Index("kibana_sample_data_flights").
	//		Request(&search.Request{
	//			Aggregations: agg,
	//			Size:         0,
	//		}).
	//		Do(context.Background())
	//
	//	if err != nil {
	//		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
	//			"error": fmt.Sprintf("ElasticSearch query error: %v", err),
	//		})
	//	}
	//
	//	return c.JSON(res.Aggregations)
	//})

	err := app.Listen(":8080")
	if err != nil {
		return
	}
}

func executeSearch(c *fiber.Ctx, query *types.Query) error {
	res, err := elastic.Client.Search().
		Index("kibana_sample_data_flights").
		Request(&search.Request{
			Query: query,
		}).
		Do(context.Background())

	if err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"error": fmt.Sprintf("ElasticSearch query error: %v", err),
		})
	}

	if res == nil || len(res.Hits.Hits) == 0 {
		return c.Status(fiber.StatusNotFound).JSON(fiber.Map{
			"error": "No documents found",
		})
	}

	return c.JSON(res.Hits.Hits)
}
