package middleware

import (
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/fiber/v2/middleware/cors"
)

var lastRequestTime time.Time
var firstRequest bool = true
var requestCounter int = 0
var requestsData []map[string]string

// func RequestThrottle() fiber.Handler {
// 	return func(c *fiber.Ctx) error {
// 		// If this is the first request, initialize the counter and timestamp
// 		if firstRequest {
// 			firstRequest = false
// 			lastRequestTime = time.Now()
// 			return c.Next()
// 		}

// 		// Increment the request counter
// 		requestCounter++

// 		// If less than 10 requests, allow and store the data
// 		if requestCounter <= 10 {
// 			lastRequestTime = time.Now() // Update the last request time
// 			storeRequestData(c)          // Function to store request data
// 			return c.Next()
// 		}

// 		// Check if 5 minutes have passed since the 10th request
// 		if time.Since(lastRequestTime) < 5*time.Minute {
// 			logrus.Warn("Request received before the 5-minute wait period")
// 			return c.Status(fiber.StatusTooManyRequests).SendString("Please wait 5 minutes between requests.")
// 		}

// 		// Reset the counter after the wait period
// 		requestCounter = 1           // Reset to 1 for the current request
// 		requestsData = nil           // Clear stored data
// 		lastRequestTime = time.Now() // Update the last request time

// 		storeRequestData(c) // Store the current request's data
// 		return c.Next()
// 	}
// }

func CORS() fiber.Handler {
	return cors.New(cors.Config{
		AllowOrigins: "*", // Allow all origins, customize as needed
		AllowHeaders: "Origin, Content-Type, Accept, Authorization",
		AllowMethods: "GET,POST,HEAD,PUT,DELETE,PATCH",
	})
}

// Helper function to store request data
// func storeRequestData(c *fiber.Ctx) {
// 	formData, err := url.ParseQuery(string(c.Body()))
// 	if err != nil {
// 		logrus.Error("Error parsing form data from request body: ", err)
// 		return
// 	}

// 	// Extract specific fields
// 	subject := formData.Get("subject")
// 	bodyPlain := formData.Get("body-plain")
// 	bodyHTML := formData.Get("body-html")

// 	// Store the data
// 	requestData := map[string]string{
// 		"subject":   subject,
// 		"bodyPlain": bodyPlain,
// 		"bodyHTML":  bodyHTML,
// 	}
// 	requestsData = append(requestsData, requestData)

// 	// Save the data to file once we reach 10 requests
// 	if requestCounter == 10 {
// 		saveAllRequestsData() // Function to save all data
// 	}
// }

// // Helper function to save all requests data to a JSON file
// func saveAllRequestsData() {
// 	if len(requestsData) > 0 {
// 		if err := handler.SaveToJSONFile(requestsData); err != nil {
// 			logrus.Error("Error saving requests data to JSON file: ", err)
// 		} else {
// 			logrus.Info("Successfully saved all 10 requests data to JSON file.")
// 		}
// 	}
// }
