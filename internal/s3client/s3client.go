package s3client

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
)

var S3Client *s3.S3

const (
	S3Bucket  = "hfieldlambdalogs" // Hardcoded S3 bucket name
	S3LogPath = "parser_log/"      // Hardcoded S3 path for logs
)

// InitializeS3 initializes the S3 client
func InitializeS3() {
	sess := session.Must(session.NewSession(&aws.Config{
		Region: aws.String("us-east-1"), // Replace with your region
	}))
	S3Client = s3.New(sess)
}

// UploadLogToS3 uploads a local log file to the specified S3 bucket and path
func UploadLogToS3(filePath string) error {
	// Construct S3 key with timestamp
	timestamp := time.Now().Format("2006-01-02-15-04-05")
	key := fmt.Sprintf("%s%s_handler_logs_%s.log", S3LogPath, "lambda", timestamp)

	// Read the log file
	file, err := os.Open(filePath)
	if err != nil {
		return fmt.Errorf("failed to open log file: %v", err)
	}
	defer file.Close()

	fileContent, err := ioutil.ReadAll(file)
	if err != nil {
		return fmt.Errorf("failed to read log file content: %v", err)
	}

	// Upload to S3
	_, err = S3Client.PutObject(&s3.PutObjectInput{
		Bucket: aws.String(S3Bucket),
		Key:    aws.String(key),
		Body:   bytes.NewReader(fileContent),
	})
	if err != nil {
		return fmt.Errorf("failed to upload log to S3: %v", err)
	}

	// Construct and log the full S3 URL
	s3URL := fmt.Sprintf("https://%s.s3.%s.amazonaws.com/%s", S3Bucket, "us-east-1", key)
	log.Printf("Log uploaded to S3: %s/%s\n", S3Bucket, key)
	log.Printf("Log uploaded to S3: %s\n", s3URL)
	return nil
}
