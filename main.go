package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"regexp"
	"strings"

	"cloud.google.com/go/bigquery"
	"golang.org/x/sync/errgroup"
	"google.golang.org/api/iterator"
)

func main() {
	ctx := context.Background()
	bigqueryClient, err := bigquery.NewClient(ctx, os.Getenv("BIGQUERY_PROJECT"))
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("digraph bigquery {")

	g, ctx := errgroup.WithContext(ctx)
	datasets := bigqueryClient.Datasets(ctx)
	for {
		dataset, err := datasets.Next()
		if err != nil {
			if err == iterator.Done {
				break
			}
			panic(err)
		}

		g.Go(func() error {
			return analyzeDataset(ctx, dataset)
		})
	}

	if err := g.Wait(); err != nil {
		panic(err)
	}

	fmt.Println("}")
}

func analyzeDataset(ctx context.Context, dataset *bigquery.Dataset) error {
	g, ctx := errgroup.WithContext(ctx)
	tables := dataset.Tables(ctx)
	for {
		table, err := tables.Next()
		if err != nil {
			if err == iterator.Done {
				break
			}
			return err
		}

		g.Go(func() error {
			return analyzeTable(ctx, table)
		})
	}
	return g.Wait()
}

var tablePattern = regexp.MustCompile("`([^`]+)`")

func analyzeTable(ctx context.Context, table *bigquery.Table) error {
	m, err := table.Metadata(ctx)
	if err != nil {
		return err
	}
	fmt.Printf("\"%s.%s\";\n", table.DatasetID, table.TableID)
	if m.Type == bigquery.ViewTable {
		for _, match := range tablePattern.FindAllStringSubmatch(m.View, -1) {
			parts := strings.Split(match[1], ".")
			fmt.Printf("\"%s.%s\" -> \"%s.%s\";\n", table.DatasetID, table.TableID, parts[1], parts[2])
		}
		// fmt.Println(table, m.View)
	}
	return nil
}
