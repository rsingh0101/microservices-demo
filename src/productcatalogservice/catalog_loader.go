// Copyright 2024 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"os"
	"strings"

	"cloud.google.com/go/alloydbconn"
	pb "github.com/GoogleCloudPlatform/microservices-demo/src/productcatalogservice/genproto"
	"github.com/golang/protobuf/jsonpb"
	"github.com/jackc/pgx/v5/pgxpool"
)

func loadCatalog(catalog *pb.ListProductsResponse) error {
	catalogMutex.Lock()
	defer catalogMutex.Unlock()

	// If the ALLOYDB_CLUSTER_NAME env is set, then we load from AlloyDB
	if os.Getenv("ALLOYDB_CLUSTER_NAME") != "" {
		if err := loadCatalogFromAlloyDB(catalog); err != nil {
			return err
		}
	}

	// Else, we load from the local file
	if err := loadCatalogFromLocalFile(catalog); err != nil {
		return err
	}

	return nil
}

func loadCatalogFromLocalFile(catalog *pb.ListProductsResponse) error {
	catalogJSON, err := os.ReadFile("products.json")
	if err != nil {
		log.Fatalf("failed to open product catalog json file: %v", err)
		return err
	}

	if err := jsonpb.Unmarshal(bytes.NewReader(catalogJSON), catalog); err != nil {
		log.Warnf("failed to parse the catalog JSON: %v", err)
		return err
	}

	log.Info("successfully parsed product catalog json")
	return nil
}

func loadCatalogFromAlloyDB(catalog *pb.ListProductsResponse) error {
	projectID := os.Getenv("PROJECT_ID")
	region := os.Getenv("REGION")
	pgClusterName := os.Getenv("ALLOYDB_CLUSTER_NAME")
	pgInstanceName := os.Getenv("ALLOYDB_INSTANCE_NAME")
	pgInstanceURI := fmt.Sprintf("projects/%s/locations/%s/clusters/%s/instances/%s", projectID, region, pgClusterName, pgInstanceName)
	pgDatabaseName := os.Getenv("ALLOYDB_DATABASE_NAME")
	pgTableName := os.Getenv("ALLOYDB_TABLE_NAME")
	pgPassword := "thisispassword"

	dialer, err := alloydbconn.NewDialer(context.Background())
	if err != nil {
		return err
	}
	cleanup := func() error { return dialer.Close() }
	defer cleanup()

	dsn := fmt.Sprintf(
		"user=%s password=%s dbname=%s sslmode=disable",
		"postgres", pgPassword, pgDatabaseName,
	)

	config, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return err
	}

	config.ConnConfig.DialFunc = func(ctx context.Context, _ string, _ string) (net.Conn, error) {
		return dialer.Dial(ctx, pgInstanceURI)
	}

	pool, err := pgxpool.NewWithConfig(context.Background(), config)
	if err != nil {
		return err
	}
	defer pool.Close()

	query := "SELECT id, name, description, picture, price_usd_currency_code, price_usd_units, price_usd_nanos, categories FROM " + pgTableName
	rows, err := pool.Query(context.Background(), query)
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		product := pb.Product{}
		var categories string
		err = rows.Scan(&product.Id, &product.Name, &product.Description,
			&product.Picture, &product.PriceUsd.CurrencyCode, &product.PriceUsd.Units,
			&product.PriceUsd.Nanos, &categories)
		if err != nil {
			return err
		}
		categories = strings.ToLower(categories)
		product.Categories = strings.Split(categories, ",")
		fmt.Println(product.Id, product.Name, product.Categories[0] /* ... */)
	}

	return nil
}