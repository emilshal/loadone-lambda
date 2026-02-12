package routes

import (
	"gitlab.com/emilshal/loadone-lambda/internal/handler"
	"github.com/aws/aws-lambda-go/events"
	"github.com/gofiber/fiber/v2"
	"github.com/sirupsen/logrus"
)

func Setup(app *fiber.App) {
	// Root route
	app.Get("/", func(c *fiber.Ctx) error {
		logrus.Info("Root route accessed")
		return c.SendString("Welcome to the Fiber server!")
	})

	// Mailgun route
	app.Post("/mailgun", func(c *fiber.Ctx) error {
		ctx := c.Context()
		request := events.APIGatewayProxyRequest{
			Body: string(c.Body()),
			// Add other necessary fields from c to request
		}
		response, err := handler.LambdaHandler(ctx, request)
		if err != nil {
			return err
		}
		return c.Status(response.StatusCode).SendString(response.Body)
	})
}
