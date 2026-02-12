package config

import (
	"os"

	"github.com/sirupsen/logrus"
)

type Config struct {
	ServerIP       string
	ServerPort     string
	LogFile        string
	AWSRegion      string
	AWSAccessKey   string
	AWSSecretKey   string
	AWSSQSQueueURL string
	MySQLDSN       string
}

var AppConfig Config

func LoadConfig() {

	AppConfig = Config{
		ServerIP:       getEnv("SERVER_IP", "127.0.0.1"),
		ServerPort:     getEnv("SERVER_PORT", "54321"),
		LogFile:        getEnv("LOG_FILE", "logs/app.log"),
		AWSRegion:      getEnv("AWS_REGION", "us-east-1"),
		AWSAccessKey:   getEnv("CUSTOM_AWS_ACCESS_KEY", ""),
		AWSSecretKey:   getEnv("CUSTOM_AWS_SECRET_KEY", ""),
		AWSSQSQueueURL: getEnv("AWS_SQS_QUEUE_URL", ""),
		MySQLDSN:       getEnv("MYSQL_DSN", ""),
	}
	logrus.Infof("Loaded configuration: %+v", AppConfig)

}

// Helper function to read an environment variable or return a default value
func getEnv(key, defaultValue string) string {
	if value, exists := os.LookupEnv(key); exists {
		return value
	}
	return defaultValue
}
