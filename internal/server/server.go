package server

import (
	"gitlab.com/emilshal/loadone-lambda/internal/log"
	"gitlab.com/emilshal/loadone-lambda/internal/middleware"
	"gitlab.com/emilshal/loadone-lambda/internal/routes"
	config "gitlab.com/emilshal/loadone-lambda/pkg"
	"github.com/gofiber/fiber/v2"
	"github.com/sirupsen/logrus"
)

func SetupAndRun() {
	// Load configuration
	config.LoadConfig()

	// Initialize the logger
	log.InitLogger()

	// Create a new Fiber app
	app := fiber.New()

	// Apply the CORS middleware from the middleware package
	app.Use(middleware.CORS())

	// Set up routes
	routes.Setup(app)

	// Start the server on the specified IP and port
	logrus.Infof("Starting server on %s:%s", config.AppConfig.ServerIP, config.AppConfig.ServerPort)
	if err := app.Listen(config.AppConfig.ServerIP + ":" + config.AppConfig.ServerPort); err != nil {
		logrus.Fatal(err)
	}
}
