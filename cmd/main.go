package main

import (
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/stebinsabu13/lambda/pkg/handlers"
)

func main() {
	lambda.Start(handlers.HandleUploadProduct)
}
