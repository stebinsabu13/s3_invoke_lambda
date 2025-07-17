package handlers

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/go-redis/redis/v8"
	"github.com/gocarina/gocsv"

	"github.com/stebinsabu13/lambda/pkg/db"
	"github.com/stebinsabu13/lambda/pkg/models"
)

func init() {
	// Initialize database connections
	err := db.InitPostgres(
		os.Getenv("PG_HOST"),
		os.Getenv("PG_PORT"),
		os.Getenv("PG_USER"),
		os.Getenv("PG_PASSWORD"),
		os.Getenv("PG_DBNAME"),
	)
	if err != nil {
		log.Fatalf("Failed to initialize PostgreSQL: %v", err)
	}

	err = db.InitRedis(
		os.Getenv("REDIS_HOST"),
		os.Getenv("REDIS_PASSWORD"),
		0,
	)
	if err != nil {
		log.Fatalf("Failed to initialize Redis: %v", err)
	}
}

func HandleUploadProduct(ctx context.Context, event events.S3Event) error {
	mySession := session.Must(session.NewSession())
	svc := s3.New(mySession, aws.NewConfig().WithRegion("us-west-2"))
	pg := db.GetPostgres()
	rdb := db.GetRedis()
	var validateErr error
	for _, record := range event.Records {
		bucket := record.S3.Bucket.Name
		key := record.S3.Object.Key

		// Get the CSV file from S3
		result, err := svc.GetObject(&s3.GetObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(key),
		})
		if err != nil {
			return fmt.Errorf("failed to get object %s from bucket %s: %v", key, bucket, err)
		}
		defer result.Body.Close()

		// Parse CSV
		var products []*models.Product
		if err := gocsv.Unmarshal(result.Body, &products); err != nil {
			return fmt.Errorf("failed to parse CSV: %v", err)
		}

		// Process each product
		for _, product := range products {
			// Validate product
			if err := validateProduct(product); err != nil {
				errors.Join(validateErr, fmt.Errorf("invalid product %s: %v, ", product.ID, err))
				log.Printf("Invalid product %s: %v", product.ID, err)
				continue
			}

			// Update PostgreSQL (idempotent operation)
			err = updateProductInPostgres(pg, product)
			if err != nil {
				errors.Join(validateErr, fmt.Errorf("failed to update product %s in PostgreSQL: %v, ", product.ID, err))
				log.Printf("Failed to update product %s in PostgreSQL: %v", product.ID, err)
				continue
			}

			// Update Redis cache
			err = updateProductInRedis(rdb, product)
			if err != nil {
				errors.Join(validateErr, fmt.Errorf("failed to update product %s in Redis: %v, ", product.ID, err))
				log.Printf("Failed to update product %s in Redis: %v", product.ID, err)
			}
		}
	}

	return nil
}

func validateProduct(p *models.Product) error {
	if p.ID == "" {
		return fmt.Errorf("product ID is required")
	}
	if p.Name == "" {
		return fmt.Errorf("product name is required")
	}
	if p.Price < 0 {
		return fmt.Errorf("price cannot be negative")
	}
	if p.Qty < 0 {
		return fmt.Errorf("stock cannot be negative")
	}
	return nil
}

func updateProductInPostgres(db *sql.DB, p *models.Product) error {
	query := `
        INSERT INTO products (id, name, description, price, stock, category, updated_at)
        VALUES ($1, $2, $3, $4, $5, $6, NOW())
        ON CONFLICT (id) DO UPDATE SET
            name = EXCLUDED.name,
            description = EXCLUDED.description,
            price = EXCLUDED.price,
            stock = EXCLUDED.stock,
            category = EXCLUDED.category,
            updated_at = NOW()
    `
	_, err := db.Exec(query, p.ID, p.Name, p.Image, p.Price, p.Qty)
	return err
}

func updateProductInRedis(rdb *redis.Client, p *models.Product) error {
	ctx := context.Background()
	productJSON, err := json.Marshal(p)
	if err != nil {
		return err
	}

	// Store in Redis with expiration
	err = rdb.Set(ctx, fmt.Sprintf("product:%s", p.ID), productJSON, 24*time.Hour).Err()
	if err != nil {
		return err
	}

	// Also update the products list cache
	return rdb.SAdd(ctx, "products:list", p.ID).Err()
}
