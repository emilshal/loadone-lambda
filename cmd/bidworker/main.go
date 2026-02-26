package main

import (
	"log"

	"github.com/aws/aws-lambda-go/lambda"
	"github.com/joho/godotenv"
	"github.com/sirupsen/logrus"
	"gitlab.com/emilshal/loadone-lambda/internal/bidworker"
)

func main() {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("panic in bidworker main: %v", r)
		}
	}()

	_ = godotenv.Load()
	logrus.Info("Starting Load One bid worker Lambda")
	lambda.Start(bidworker.LambdaHandler)
}
