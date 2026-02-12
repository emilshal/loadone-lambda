package log

import (
	"io"
	"os"
	"path/filepath"

	"github.com/sirupsen/logrus"
)

func InitLogger() {
	// Ensure the logs directory exists in the project root
	logDir := filepath.Join("..", "..", "logs")
	if _, err := os.Stat(logDir); os.IsNotExist(err) {
		err := os.Mkdir(logDir, 0755)
		if err != nil {
			logrus.Warn("Failed to create logs directory, using default stderr")
		}
	}

	// Set the output to the log file in the project root
	logFile := filepath.Join(logDir, "app.log")
	file, err := os.OpenFile(logFile, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	if err != nil {
		logrus.Warn("Failed to log to file, using default stderr")
		logrus.SetOutput(os.Stdout) // Fallback to stdout if file creation fails
	} else {
		// Log to both file and terminal
		multiWriter := io.MultiWriter(file, os.Stdout)
		logrus.SetOutput(multiWriter)
	}

	// Set the log level (optional, default is Info)
	logrus.SetLevel(logrus.InfoLevel)

	// Set log format (optional)
	logrus.SetFormatter(&logrus.TextFormatter{
		FullTimestamp: true,
	})
}
