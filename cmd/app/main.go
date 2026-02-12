package main

import (
	"log"

	"github.com/aws/aws-lambda-go/lambda"
	"github.com/joho/godotenv"
	"gitlab.com/emilshal/loadone-lambda/internal/handler"
	"gitlab.com/emilshal/loadone-lambda/internal/s3client"
	config "gitlab.com/emilshal/loadone-lambda/pkg"
)

func main() {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("Panic in main: %v\n", r)
		}
	}()

	// Load .env first for local runs; no-op on Lambda
	_ = godotenv.Load()

	// Now read env into your config
	config.LoadConfig()

	// S3 client is fine to pre-init; it's lightweight
	s3client.InitializeS3()

	// IMPORTANT: DO NOT InitializeDB() here.
	// Let the handler do it only when needed (and after it checks dry_run).
	lambda.Start(handler.LambdaHandler)
}
