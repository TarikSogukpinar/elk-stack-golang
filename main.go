package main

import (
	"context"
	"elk-stack-golang/elastic"
	"fmt"
	"github.com/elastic/go-elasticsearch/v8/typedapi/core/search"
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

	app.Get("/flights/dest", func(c *fiber.Ctx) error {
		var destination = c.Params("destination", "Ataturk International Airport")

		res, err := elastic.Client.Search().
			Index("kibana_sample_data_flights").
			Request(&search.Request{
				Query: &types.Query{
					Match: map[string]types.MatchQuery{
						"Dest": {Query: destination},
					},
				},
			}).
			Do(context.Background())

		if err != nil {
			return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
				"error": fmt.Sprintf("ElasticSearch query error: %v", err),
			})
		}

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

	err := app.Listen(":8080")
	if err != nil {
		return
	}
}
