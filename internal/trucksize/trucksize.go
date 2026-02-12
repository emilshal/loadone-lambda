package trucksize

import (
	"log"
	"regexp"
	"strconv"
	"strings"
)

// TruckType represents a type of truck with an ID and Name.
type TruckType struct {
	ID   int
	Name string
}

// Constants for TruckType IDs.
const (
	SMALL_STRAIGHT = iota + 1
	LARGE_STRAIGHT
	SPRINTER
)

// AllowedTruckSizes is the list of valid truck sizes.
var AllowedTruckSizes = []string{"SMALL STRAIGHT", "LARGE STRAIGHT"}

// extractTruckTypeKeyword extracts truck type keywords like "53 FT" from a string.
func extractTruckTypeKeyword(field string) string {
	re := regexp.MustCompile(`\b\d+\s*FT\b`)
	match := re.FindString(field)
	return match
}

// removeEverythingButNumbers removes all non-numeric characters from a string.
func removeEverythingButNumbers(s string) string {
	re := regexp.MustCompile(`\D`)
	return re.ReplaceAllString(s, "")
}

// checkIfSprinterRequired checks if a Sprinter van is required based on notes and subject.
func checkIfSprinterRequired(notes, subject string) bool {
	lowerNotes := strings.ToLower(notes)
	lowerSubject := strings.ToLower(subject)
	return strings.Contains(lowerNotes, "sprinter") || strings.Contains(lowerSubject, "sprinter")
}

// convertToKnownType tries to convert extracted fields into a known truck type.
func convertToKnownType(fields []string) (*TruckType, bool) {
	smallStraightMaxSize := 26 // Define the maximum size for a SMALL STRAIGHT truck.

	for _, field := range fields {
		extracted := extractTruckTypeKeyword(field)
		if extracted == "" {
			continue
		}

		ftSizeStr := removeEverythingButNumbers(extracted)
		if ftSizeStr == "" {
			continue
		}

		ftSize, err := strconv.Atoi(ftSizeStr)
		if err != nil {
			continue
		}

		if ftSize <= smallStraightMaxSize {
			// SMALL STRAIGHT
			return &TruckType{
				ID:   SMALL_STRAIGHT,
				Name: "SMALL STRAIGHT",
			}, true
		} else {
			// LARGE STRAIGHT
			return &TruckType{
				ID:   LARGE_STRAIGHT,
				Name: "LARGE STRAIGHT",
			}, true
		}
	}
	return nil, false
}

// contains checks if a string is in a slice.
func contains(slice []string, item string) bool {
	for _, s := range slice {
		if strings.EqualFold(s, item) {
			return true
		}
	}
	return false
}

// DetermineSuggestedTruckSize determines the suggested truck size based on input data.
// It updates parsedKeyValue["suggested_truck_size"] accordingly.
func DetermineSuggestedTruckSize(parsedKeyValue map[string]string, args map[string]string) {
	var truckTypeConverterOptions []string

	// Collect potential truck type information from notes and subject.
	if notes, ok := parsedKeyValue["notes"]; ok && notes != "" {
		truckTypeConverterOptions = append(truckTypeConverterOptions, notes)
	}
	if subject, ok := args["subject"]; ok && subject != "" {
		truckTypeConverterOptions = append(truckTypeConverterOptions, subject)
	}

	var convertedTruckType *TruckType
	if len(truckTypeConverterOptions) > 0 {
		// Attempt to convert to a known truck type.
		convertedType, ok := convertToKnownType(truckTypeConverterOptions)
		if ok {
			// Check if Sprinter is required.
			requiresSprinter := checkIfSprinterRequired(parsedKeyValue["notes"], args["subject"])
			if requiresSprinter {
				convertedTruckType = &TruckType{
					ID:   SPRINTER,
					Name: "SPRINTER",
				}
			} else {
				convertedTruckType = convertedType
			}
		}
	}

	// Set or update suggested_truck_size based on allowed truck sizes and conversion results.
	suggestedTruckSize, hasSuggestedTruckSize := parsedKeyValue["suggested_truck_size"]

	if hasSuggestedTruckSize {
		if !contains(AllowedTruckSizes, suggestedTruckSize) {
			if convertedTruckType == nil {
				// Handle special cases for van types.
				lowerSize := strings.ToLower(suggestedTruckSize)
				if lowerSize == "cargo van" || lowerSize == "van" || lowerSize == "cube van" || lowerSize == "sprinter van" {
					log.Printf("TRUCK_SIZE_CARGO_VAN: %+v", parsedKeyValue)
					parsedKeyValue["suggested_truck_size"] = "SPRINTER"
				} else {
					parsedKeyValue["suggested_truck_size"] = "SMALL STRAIGHT"
				}
			} else {
				parsedKeyValue["suggested_truck_size"] = convertedTruckType.Name
			}
		} else {
			if convertedTruckType != nil && !strings.EqualFold(convertedTruckType.Name, suggestedTruckSize) {
				parsedKeyValue["suggested_truck_size"] = convertedTruckType.Name
			}
		}
	} else {
		if convertedTruckType != nil {
			parsedKeyValue["suggested_truck_size"] = convertedTruckType.Name
		} else {
			parsedKeyValue["suggested_truck_size"] = "SMALL STRAIGHT"
		}
	}

	// Optionally, log the final suggested truck size.
	log.Printf("Final suggested_truck_size: %s", parsedKeyValue["suggested_truck_size"])
}
