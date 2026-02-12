package parser

import (
	"html"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/sirupsen/logrus"
	models "gitlab.com/emilshal/loadone-lambda/internal/model"
)

// Public API -------------------------------------------------------------

type LoadOneParser struct{}

// Result returns your models + a few extras useful for the handler/UI.
type Result struct {
	Order        models.Order
	Location     models.OrderLocation
	Item         models.OrderItem
	ExternalLink string // CTA link like "Click here to provide your all-in rate"
	// NEW: pass the exact strings to the handler/SQS
	PickupDateStr   string
	DeliveryDateStr string
}

// Parse prefers bodyPlain; falls back to a normalized version of bodyHTML.
// It returns fully-populated models.Order, models.OrderLocation, and models.OrderItem.
func (p *LoadOneParser) Parse(bodyHTML, bodyPlain string) (*Result, error) {
	plain := strings.TrimSpace(bodyPlain)
	htmlText := normalizeHTMLToText(strings.TrimSpace(bodyHTML))

	rPlain := parseAll(plain)
	rHTML := parseAll(htmlText)

	best := chooseBetter(rPlain, rHTML)

	// prefer HTML 'Note' cell if present
	if n := extractNotesFromHTML(bodyHTML); n != "" {
		// set on both so chooseBetter doesn't flip-flop
		rPlain.Order.Notes = n
		rHTML.Order.Notes = n
	}
	// --- Preserve external link robustly ---
	if best.ExternalLink == "" {
		// 1) try raw HTML anchors
		if u := extractExternalLinkFromHTML(bodyHTML); u != "" {
			best.ExternalLink = u
		} else {
			// 2) try plain text again (CTA line with URL in parentheses)
			if u := extractExternalLink(plain); u != "" {
				best.ExternalLink = u
			}
		}
	}

	finalizeLabelsAndDefaults(&best)
	return &best, nil
}

// Internal pipeline ------------------------------------------------------

type scratch struct {
	// Order-ish
	orderNumber        string
	estMiles           int
	suggestedTruckSize string
	originalTruckSize  string
	notes              string
	extLink            string
	carrierPay         int     // mapped to Order.CarrierPay
	carrierPayRate     float64 // mapped to Order.CarrierPayRate

	// Items
	length, width, height float64
	weight                float64
	pieces                int
	stackable             bool
	hazardous             bool

	// Locations
	puCity, puStateCode, puZip string
	drCity, drStateCode, drZip string
	puDateStr                  string
	drDateStr                  string
}

func parseAll(src string) Result {
	src = normalizeNewlines(src)

	sc := &scratch{}
	// Order number
	sc.orderNumber = extractOrderNumber(src)
	// Miles
	sc.estMiles = extractEstimatedMiles(src)
	// Truck size
	sc.suggestedTruckSize, sc.originalTruckSize = extractTruckSizePair(src)
	// Notes
	sc.notes = extractNotes(src)
	// External CTA link
	sc.extLink = extractExternalLink(src)
	// Pay
	sc.carrierPay = extractCarrierPay(src)
	sc.carrierPayRate = extractCarrierPayRate(src)
	// Freight block
	sc.pieces = extractPieces(src)
	sc.weight = extractWeightLbs(src)
	sc.length, sc.width, sc.height = extractDimensions(src)
	sc.stackable = extractStackable(src)
	sc.hazardous = extractHazardous(src)
	// Locations
	sc.puCity, sc.puStateCode, sc.puZip = extractPickupLocation(src)
	sc.drCity, sc.drStateCode, sc.drZip = extractDeliveryLocation(src)
	// Times

	sc.puDateStr = extractPickupDateStr(src)
	sc.drDateStr = extractDeliveryDateStr(src)

	return toResult(sc)
}

func toResult(sc *scratch) Result {
	var r Result
	r.PickupDateStr = sc.puDateStr
	r.DeliveryDateStr = sc.drDateStr
	truckTypeID, normalizedTruckSize := mapTruckTypeID(sc.suggestedTruckSize)
	suggestedTruckSize := sc.suggestedTruckSize
	if normalizedTruckSize != "" {
		suggestedTruckSize = normalizedTruckSize
	}
	// ---- Order
	r.Order = models.Order{
		OrderNumber:        sc.orderNumber,
		SuggestedTruckSize: suggestedTruckSize,
		Notes:              sc.notes,
		EstimatedMiles:     sc.estMiles,
		TruckTypeID:        truckTypeID,
		OriginalTruckSize:  sc.originalTruckSize,
		OrderTypeID:        7,                 // as per your convention
		CarrierPay:         sc.carrierPay,     // column: pays
		CarrierPayRate:     sc.carrierPayRate, // column: pays_rate
	}

	// ---- Location
	puStateName := stateCodeToName(sc.puStateCode)
	drStateName := stateCodeToName(sc.drStateCode)

	r.Location = models.OrderLocation{
		PickupCity:        sc.puCity,
		PickupStateCode:   sc.puStateCode,
		PickupState:       puStateName,
		PickupPostalCode:  sc.puZip,
		PickupCountryCode: "US",
		PickupCountryName: "USA",

		DeliveryCity:        sc.drCity,
		DeliveryStateCode:   sc.drStateCode,
		DeliveryState:       drStateName,
		DeliveryPostalCode:  sc.drZip,
		DeliveryCountryCode: "US",
		DeliveryCountryName: "USA",

		EstimatedMiles: float64(sc.estMiles),
	}

	// Labels (zip, city, stateCode, country)
	r.Location.PickupLabel = buildLabel(sc.puZip, sc.puCity, sc.puStateCode, "USA")
	r.Location.DeliveryLabel = buildLabel(sc.drZip, sc.drCity, sc.drStateCode, "USA")

	// ---- Item
	r.Item = models.OrderItem{
		Length:    sc.length,
		Width:     sc.width,
		Height:    sc.height,
		Weight:    sc.weight,
		Pieces:    sc.pieces,
		Stackable: sc.stackable,
		Hazardous: sc.hazardous,
	}

	// Extra
	r.ExternalLink = sc.extLink

	return r
}

func extractPickupDateStr(src string) string {
	// Prefer the line right after PU (matches both 10/28/2025 10:00 EST and 2025-10-28 10:00 EST)
	re := regexp.MustCompile(`(?i)PU[^\n]*\n\s*([0-9]{1,2}/[0-9]{1,2}/[0-9]{4}\s+[0-9]{1,2}:[0-9]{2}\s+[A-Z]{2,4}|[0-9]{4}-[0-9]{2}-[0-9]{2}\s+[0-9]{2}:[0-9]{2}\s+[A-Z]{2,4})(?:\s*$begin:math:text$[^)]*$end:math:text$)?`)
	if m := re.FindStringSubmatch(src); len(m) > 1 {
		return strings.TrimSpace(m[1])
	}
	// Fallback: first datetime anywhere
	re2 := regexp.MustCompile(`(?i)([0-9]{1,2}/[0-9]{1,2}/[0-9]{4}\s+[0-9]{1,2}:[0-9]{2}\s+[A-Z]{2,4}|[0-9]{4}-[0-9]{2}-[0-9]{2}\s+[0-9]{2}:[0-9]{2}\s+[A-Z]{2,4})`)
	if m := re2.FindStringSubmatch(src); len(m) > 1 {
		return strings.TrimSpace(m[1])
	}
	return ""
}

func extractDeliveryDateStr(src string) string {
	// Prefer the line right after DR
	re := regexp.MustCompile(`(?i)\bDR[^\n]*\n\s*([0-9]{1,2}/[0-9]{1,2}/[0-9]{4}\s+[0-9]{1,2}:[0-9]{2}\s+[A-Z]{2,4}|[0-9]{4}-[0-9]{2}-[0-9]{2}\s+[0-9]{2}:[0-9]{2}\s+[A-Z]{2,4})(?:\s*$begin:math:text$[^)]*$end:math:text$)?`)
	if m := re.FindStringSubmatch(src); len(m) > 1 {
		return strings.TrimSpace(m[1])
	}
	// Fallback: second datetime anywhere
	re2 := regexp.MustCompile(`(?i)([0-9]{1,2}/[0-9]{1,2}/[0-9]{4}\s+[0-9]{1,2}:[0-9]{2}\s+[A-Z]{2,4}|[0-9]{4}-[0-9]{2}-[0-9]{2}\s+[0-9]{2}:[0-9]{2}\s+[A-Z]{2,4})`)
	all := re2.FindAllStringSubmatch(src, -1)
	if len(all) > 1 {
		return strings.TrimSpace(all[1][1])
	}
	return ""
}

func stripParenNote(s string) string {
	return strings.TrimSpace(regexp.MustCompile(`\s*$begin:math:text$[^)]+$end:math:text$\s*$`).ReplaceAllString(s, ""))
}

func chooseBetter(a, b Result) Result {
	score := func(r Result) int {
		s := 0
		if r.Order.OrderNumber != "" {
			s += 3
		}
		if r.Location.PickupCity != "" && r.Location.PickupStateCode != "" && r.Location.PickupPostalCode != "" {
			s += 3
		}
		if r.Location.DeliveryCity != "" && r.Location.DeliveryStateCode != "" && r.Location.DeliveryPostalCode != "" {
			s += 3
		}
		if !r.Order.PickupDate.IsZero() {
			s += 2
		}
		if !r.Order.DeliveryDate.IsZero() {
			s += 2
		}
		if r.Item.Pieces > 0 {
			s++
		}
		if r.Item.Weight > 0 {
			s++
		}
		if r.Item.Length > 0 {
			s++
		}
		if r.Item.Width > 0 {
			s++
		}
		if r.Item.Height > 0 {
			s++
		}
		if r.Order.SuggestedTruckSize != "" {
			s++
		}
		if r.PickupDateStr != "" {
			s += 2
		}
		if r.DeliveryDateStr != "" {
			s += 2
		}
		return s
	}
	if score(a) >= score(b) {
		return a
	}
	return b
}

func finalizeLabelsAndDefaults(r *Result) {
	// Already filled labels/country; nothing more for now.
}

// Extractors -------------------------------------------------------------

func extractOrderNumber(src string) string {
	re := regexp.MustCompile(`(?i)Quote\s*#\s*([A-Za-z0-9\-]+)`)
	if m := re.FindStringSubmatch(src); len(m) > 1 {
		return strings.TrimSpace(m[1])
	}
	return ""
}

// Miles: e.g. "Stops (241 miles)" or "Stops (1,200 mi)"
func extractEstimatedMiles(src string) int {
	re := regexp.MustCompile(`(?i)\bStops\s*\(\s*([0-9,]+)\s*miles?\b`)
	if m := re.FindStringSubmatch(src); len(m) > 1 {
		return mustAtoi(m[1])
	}
	// Fallbacks sometimes say "Distance: 666 mi"
	re2 := regexp.MustCompile(`(?i)\bDistance\s*:\s*([0-9,]+)\s*mi(?:les)?\b`)
	if m := re2.FindStringSubmatch(src); len(m) > 1 {
		return mustAtoi(m[1])
	}
	return 0
}

// Truck size from "Freight (TRACTOR)" or "Requested Vehicle Class: Tractor Trailer"
func extractTruckSizePair(src string) (suggested, original string) {
	// 1) Freight (TRACTOR)
	re := regexp.MustCompile(`(?i)\bFreight\s*\(\s*([A-Z0-9 /+\-]+?)\s*\)`)
	if m := re.FindStringSubmatch(src); len(m) > 1 {
		ts := strings.TrimSpace(m[1])
		return ts, ts
	}
	// 2) Requested Vehicle Class: Tractor Trailer
	re2 := regexp.MustCompile(`(?i)\bRequested\s+Vehicle\s+Class\s*:\s*([A-Za-z0-9 /+\-]+)`)
	if m := re2.FindStringSubmatch(src); len(m) > 1 {
		ts := strings.TrimSpace(m[1])
		return ts, ts
	}
	// 3) We call this vehicle class: Tractor Trailer
	re3 := regexp.MustCompile(`(?i)\bWe\s+call\s+this\s+vehicle\s+class\s*:\s*([A-Za-z0-9 /+\-]+)`)
	if m := re3.FindStringSubmatch(src); len(m) > 1 {
		ts := strings.TrimSpace(m[1])
		return ts, ts
	}
	return "", ""
}

func extractNotes(src string) string {
	// Strict single-line text capture (avoid grabbing the next row)
	if m := regexp.MustCompile(`(?im)^\s*Note\s*:?\s*([^\n]*)$`).FindStringSubmatch(src); len(m) == 2 {
		return strings.TrimSpace(m[1])
	}
	return ""
}
func extractExternalLink(src string) string {
	re, err := regexp.Compile(`(?i)Click here to provide your all-in rate.*?(https?://\S+)`)
	if err == nil {
		if m := re.FindStringSubmatch(src); len(m) > 1 {
			return trimTrailingPunct(m[1])
		}
	}
	re2, err2 := regexp.Compile(`https?://\S+`)
	if err2 == nil {
		if m := re2.FindStringSubmatch(src); len(m) > 0 {
			return trimTrailingPunct(m[0])
		}
	}
	return ""
}

func extractCarrierPay(src string) int {
	// Example patterns (customize as you see them):
	// "Carrier Pay: $1,250" or "Pay: $900"
	re := regexp.MustCompile(`(?i)(Carrier\s*Pay|Pay)\s*:\s*\$?\s*([0-9,]+)`)
	if m := re.FindStringSubmatch(src); len(m) > 2 {
		return mustAtoi(m[2])
	}
	return 0
}

func extractCarrierPayRate(src string) float64 {
	// Example: "Rate: $2.10/mi" or "$1.75 per mile"
	re := regexp.MustCompile(`(?i)(Rate|per\s*mile)\s*[:\-]?\s*\$?\s*([0-9]+(?:\.[0-9]+)?)\s*(/mi|per\s*mile)?`)
	if m := re.FindStringSubmatch(src); len(m) > 2 {
		return mustAtof(m[2])
	}
	return 0
}

func extractPieces(src string) int {
	re := regexp.MustCompile(`(?i)Pieces:\s*([0-9,]+)`)
	if m := re.FindStringSubmatch(src); len(m) > 1 {
		return mustAtoi(m[1])
	}
	return 0
}

func extractWeightLbs(src string) float64 {
	// your current
	if m := regexp.MustCompile(`(?i)Weight:\s*([0-9,]+(?:\.[0-9]+)?)\s*lb?s?`).FindStringSubmatch(src); len(m) > 1 {
		return mustAtof(m[1])
	}
	// simple fallback (just in case spacing/tagging gets weird)
	if m := regexp.MustCompile(`(?i)\bWeight\s*:\s*([0-9][0-9,\.]*)\s*lb?s?\b`).FindStringSubmatch(src); len(m) == 2 {
		return mustAtof(m[1])
	}
	return 0
}

// Place near other HTML helpers
func extractNotesFromHTML(htmlSrc string) string {
	re := regexp.MustCompile(`(?is)<td[^>]*>\s*Note\s*</td>\s*<td[^>]*>\s*([^<]*)\s*</td>`)
	if m := re.FindStringSubmatch(htmlSrc); len(m) == 2 {
		return strings.TrimSpace(html.UnescapeString(m[1]))
	}
	return ""
}

func extractDimensions(src string) (float64, float64, float64) {
	// existing line-first approach
	lineRe := regexp.MustCompile(`(?i)Dims[^:]*:\s*([^\n]+)`)
	if m := lineRe.FindStringSubmatch(src); len(m) == 2 {
		line := m[1]
		line = regexp.MustCompile(`(?is)<[^>]+>`).ReplaceAllString(line, "")
		line = strings.ReplaceAll(line, "×", "x")
		line = strings.TrimSpace(regexp.MustCompile(`\s+`).ReplaceAllString(line, " "))
		if mm := regexp.MustCompile(`(?i)\b([0-9]+(?:\.[0-9]+)?)\s*x\s*([0-9]+(?:\.[0-9]+)?)\s*x\s*([0-9]+(?:\.[0-9]+)?)\b`).FindStringSubmatch(line); len(mm) == 4 {
			return mustAtof(mm[1]), mustAtof(mm[2]), mustAtof(mm[3])
		}
	}

	// global fallback (handles tags or missing spaces)
	if mm := regexp.MustCompile(`(?is)\bDims\b.*?([0-9]+(?:\.[0-9]+)?)\D*[x×]\D*([0-9]+(?:\.[0-9]+)?)\D*[x×]\D*([0-9]+(?:\.[0-9]+)?)`).FindStringSubmatch(src); len(mm) == 4 {
		return mustAtof(mm[1]), mustAtof(mm[2]), mustAtof(mm[3])
	}

	// ultra-loose fallback anywhere (no "Dims" required)
	if mm := regexp.MustCompile(`(?is)\b([0-9]+(?:\.[0-9]+)?)\D*[x×]\D*([0-9]+(?:\.[0-9]+)?)\D*[x×]\D*([0-9]+(?:\.[0-9]+)?)\b`).FindStringSubmatch(src); len(mm) == 4 {
		return mustAtof(mm[1]), mustAtof(mm[2]), mustAtof(mm[3])
	}

	return 0, 0, 0
}

func extractStackable(src string) bool {
	re := regexp.MustCompile(`(?i)Stackable:\s*(Yes|No)`)
	if m := re.FindStringSubmatch(src); len(m) > 1 {
		return strings.EqualFold(strings.TrimSpace(m[1]), "Yes")
	}
	return false
}

func extractHazardous(src string) bool {
	// Explicit "Hazardous: Yes/No" or "Haz: Yes/No"
	explicit := regexp.MustCompile(`(?i)\bHaz(?:ard(?:ous)?)?\s*:\s*(Yes|No)\b`)
	if m := explicit.FindStringSubmatch(src); len(m) == 2 {
		return strings.EqualFold(strings.TrimSpace(m[1]), "Yes")
	}

	// Heuristics: catch common phrasing in notes/body like
	// "HazMat", "Haz Mat", "UN 3268", any "UN ####", "Class 9", etc.
	keywords := regexp.MustCompile(`(?i)\bHaz\s*Mat\b|\bHazmat\b|\bHazard(?:ous)?\b|\bUN\s*\d{4}\b|\bClass\s*[1-9]\b`)
	return keywords.MatchString(src)
}

func extractPickupLocation(src string) (city, stateCode, zip string) {
	re := regexp.MustCompile(`(?i)PU\s+([A-Za-z .'\-]+)\s+([A-Z]{2})\s*,\s*([0-9]{5}(?:-[0-9]{4})?)`)
	if m := re.FindStringSubmatch(src); len(m) == 4 {
		return cleanName(m[1]), strings.ToUpper(m[2]), m[3]
	}
	return "", "", ""
}

// --- Delivery location ---------------------------------------------------
// Same hardening as pickup; comma optional before ZIP.
func extractDeliveryLocation(src string) (city, stateCode, zip string) {
	re := regexp.MustCompile(`(?i)\bDR\s+([A-Za-z .'\-]+)\s+([A-Z]{2})\s*,?\s*([0-9]{5}(?:-[0-9]{4})?)`)
	if m := re.FindStringSubmatch(src); len(m) == 4 {
		return cleanName(m[1]), strings.ToUpper(m[2]), m[3]
	}
	// Full Circle tabular/plain fallback: "... McKinney TX 75069 USA"
	re2 := regexp.MustCompile(`(?i)\b([A-Za-z .'\-]+)\s+([A-Z]{2})\s+([0-9]{5}(?:-[0-9]{4})?)\s+USA`)
	if m := re2.FindStringSubmatch(src); len(m) == 4 {
		return cleanName(m[1]), strings.ToUpper(m[2]), m[3]
	}
	return "", "", ""
}

func extractPickupDate(src string) time.Time {
	// next line after PU with a US datetime and EST/EDT
	re := regexp.MustCompile(`(?i)PU[^\n]*\n\s*([0-9]{1,2}/[0-9]{1,2}/[0-9]{4}\s+[0-9]{1,2}:[0-9]{2}\s+[A-Z]{2,4})`)
	if m := re.FindStringSubmatch(src); len(m) > 1 {
		if t, ok := parseUSNYToUTC(m[1]); ok {
			return t
		}
	}
	// fallback: first datetime anywhere
	return firstDateAnywhere(src)
}

func extractDeliveryDate(src string) time.Time {
	// next line after DR with a US datetime and EST/EDT
	re := regexp.MustCompile(`(?i)DR[^\n]*\n\s*([0-9]{1,2}/[0-9]{1,2}/[0-9]{4}\s+[0-9]{1,2}:[0-9]{2}\s+[A-Z]{2,4})`)
	if m := re.FindStringSubmatch(src); len(m) > 1 {
		if t, ok := parseUSNYToUTC(m[1]); ok {
			return t
		}
	}
	// fallback: second datetime anywhere
	return secondDateAnywhere(src)
}

// Date helpers -----------------------------------------------------------

func parseUSNYToUTC(s string) (time.Time, bool) {
	// input examples: "10/28/2025 10:00 EST" or "... EDT"
	s = strings.TrimSpace(s)
	fields := strings.Fields(s)
	if len(fields) < 3 {
		return time.Time{}, false
	}
	dat := fields[0]
	tim := fields[1]
	tz := strings.ToUpper(fields[2])

	layout := "01/02/2006 15:04 -0700"
	offset := "-0500" // EST
	if tz == "EDT" {
		offset = "-0400"
	}
	combined := dat + " " + tim + " " + offset

	tt, err := time.Parse(layout, combined)
	if err != nil {
		return time.Time{}, false
	}
	// tt is parsed in that fixed offset; convert to UTC
	return tt.UTC(), true
}

// ParseUSNYToUTC exposes the internal EST/EDT parser so other packages can
// convert LoadOne timestamps into a canonical time.Time.
func ParseUSNYToUTC(s string) (time.Time, bool) {
	return parseUSNYToUTC(s)
}

func firstDateAnywhere(src string) time.Time {
	all := allDates(src)
	if len(all) > 0 {
		if t, ok := parseUSNYToUTC(all[0]); ok {
			return t
		}
	}
	return time.Time{}
}

func secondDateAnywhere(src string) time.Time {
	all := allDates(src)
	if len(all) > 1 {
		if t, ok := parseUSNYToUTC(all[1]); ok {
			return t
		}
	}
	return time.Time{}
}

func allDates(src string) []string {
	re := regexp.MustCompile(`(?i)([0-9]{1,2}/[0-9]{1,2}/[0-9]{4}\s+[0-9]{1,2}:[0-9]{2}\s+(?:EDT|EST))`)
	return re.FindAllString(src, -1)
}

// Utilities --------------------------------------------------------------

func normalizeNewlines(s string) string {
	s = strings.ReplaceAll(s, "\r\n", "\n")
	s = strings.ReplaceAll(s, "\u00a0", " ")
	return dedupeSpaces(s)
}

func normalizeHTMLToText(htmlSrc string) string {
	if htmlSrc == "" {
		return ""
	}
	t := html.UnescapeString(htmlSrc)

	// 1) Drop <script>...</script> and <style>...</style> (no backrefs, no \s)
	t = regexp.MustCompile(`(?is)<script\b[^>]*>.*?</script>`).ReplaceAllString(t, "")
	t = regexp.MustCompile(`(?is)<style\b[^>]*>.*?</style>`).ReplaceAllString(t, "")

	// 2) Turn common block/line-breaking tags into newlines
	//    Match both opening and closing tags (</p>, <br>, <td>, etc.)
	t = regexp.MustCompile(`(?i)</?(br|p|div|li|tr|td|th)\b[^>]*>`).ReplaceAllString(t, "\n")

	// 3) Strip any remaining tags
	t = regexp.MustCompile(`(?s)<[^>]+>`).ReplaceAllString(t, "")

	t = strings.TrimSpace(t)
	return normalizeNewlines(t)
}

func dedupeSpaces(s string) string {
	// tidy multiple spaces while keeping newlines
	lines := strings.Split(s, "\n")
	for i := range lines {
		lines[i] = regexp.MustCompile(`\s+`).ReplaceAllString(strings.TrimSpace(lines[i]), " ")
	}
	return strings.Join(lines, "\n")
}

func cleanName(s string) string {
	s = strings.TrimSpace(s)
	s = regexp.MustCompile(`\s+`).ReplaceAllString(s, " ")
	return s
}

func trimTrailingPunct(u string) string {
	return strings.TrimRight(u, ").,;")
}

func mustAtoi(s string) int {
	s = strings.ReplaceAll(s, ",", "")
	n, _ := strconv.Atoi(s)
	return n
}

func mustAtof(s string) float64 {
	s = strings.ReplaceAll(s, ",", "")
	f, _ := strconv.ParseFloat(s, 64)
	return f
}

func buildLabel(zip, city, stateCode, country string) string {
	// if zip missing, use "City, ST, Country"
	if strings.TrimSpace(zip) == "" {
		return strings.TrimSpace(strings.Join([]string{
			city, strings.ToUpper(stateCode), country,
		}, ", "))
	}
	return strings.TrimSpace(strings.Join([]string{
		zip, city, strings.ToUpper(stateCode), country,
	}, ", "))
}

// Mapping helpers --------------------------------------------------------

func mapTruckTypeID(size string) (int, string) {
	lowerTruckSize := strings.ToLower(strings.TrimSpace(size))
	var (
		truckTypeID  int
		newTruckSize string
		matchReason  string
	)

	contains53 := false
	if lowerTruckSize != "" {
		contains53 = word53Regex.MatchString(lowerTruckSize) ||
			strings.Contains(lowerTruckSize, "53ft") ||
			strings.Contains(lowerTruckSize, "53 ft") ||
			strings.Contains(lowerTruckSize, "53-ft") ||
			strings.Contains(lowerTruckSize, "53'") ||
			strings.Contains(lowerTruckSize, "53\"")
	}

	if lowerTruckSize != "" {
		switch {
		case strings.Contains(lowerTruckSize, "large straight"):
			truckTypeID = 2
			newTruckSize = "Large Straight"
			matchReason = "word match 'large straight'"
		case strings.Contains(lowerTruckSize, "small straight"):
			truckTypeID = 1
			newTruckSize = "Small Straight"
			matchReason = "word match 'small straight'"
		case strings.Contains(lowerTruckSize, "semi") || strings.Contains(lowerTruckSize, "tractor") || contains53:
			truckTypeID = 4
			newTruckSize = "Semi Truck"
			matchReason = "word match 'semi/tractor/53'"
		}
	}

	truckLength := 0.0
	if truckTypeID == 0 && lowerTruckSize != "" {
		if matches := truckNumberRegex.FindStringSubmatch(lowerTruckSize); len(matches) > 0 {
			if extracted, err := strconv.ParseFloat(matches[0], 64); err == nil {
				truckLength = extracted
				matchReason = "numeric fallback"
				logrus.Infof("mapTruckTypeID: Extracted %.2f (assumed inches) from name '%s'", truckLength, size)
			}
		}

		if truckLength == 0 &&
			strings.Contains(lowerTruckSize, "cargo van") &&
			strings.Contains(lowerTruckSize, "small") {
			truckLength = 120
			matchReason = "default length for small cargo van"
			logrus.Infof("mapTruckTypeID: Defaulting truckLength to 120 (assumed inches) for small cargo van (no numeric found)")
		}

		if truckLength > 0 {
			switch {
			case truckLength <= 156:
				newTruckSize = "Sprinter"
				truckTypeID = 3
			case truckLength >= 168 && truckLength <= 192:
				newTruckSize = "Small Straight"
				truckTypeID = 1
			case truckLength >= 204 && truckLength <= 312:
				newTruckSize = "Large Straight"
				truckTypeID = 2
			case truckLength >= 324 && truckLength <= 636:
				newTruckSize = "Semi Truck"
				truckTypeID = 4
			default:
				newTruckSize = "Semi Truck"
				truckTypeID = 4
			}
		}
	}

	if strings.Contains(lowerTruckSize, "van") && !anyDigitRegex.MatchString(lowerTruckSize) {
		truckTypeID = 3
		newTruckSize = "Sprinter"
		matchReason = "van keyword without digits"
	}

	if strings.Contains(lowerTruckSize, "sprinter") {
		truckTypeID = 3
		newTruckSize = "Sprinter"
		matchReason = "sprinter keyword"
	}

	if truckTypeID == 0 {
		truckTypeID = 4
		if newTruckSize == "" {
			newTruckSize = "Semi Truck"
		}
		if matchReason == "" {
			matchReason = "default fallback"
		}
	}

	logrus.Infof(
		"mapTruckTypeID: Final assigned TruckSize=%s (ID=%d) with length=%.2f (input='%s', reason=%s)",
		newTruckSize, truckTypeID, truckLength, size, matchReason,
	)

	return truckTypeID, newTruckSize
}

func stateCodeToName(code string) string {
	code = strings.ToUpper(strings.TrimSpace(code))
	if name, ok := usStates[code]; ok {
		return name
	}
	return code // fallback to the code itself if unknown
}

var usStates = map[string]string{
	"AL": "Alabama", "AK": "Alaska", "AZ": "Arizona", "AR": "Arkansas",
	"CA": "California", "CO": "Colorado", "CT": "Connecticut", "DE": "Delaware",
	"FL": "Florida", "GA": "Georgia", "HI": "Hawaii", "ID": "Idaho",
	"IL": "Illinois", "IN": "Indiana", "IA": "Iowa", "KS": "Kansas",
	"KY": "Kentucky", "LA": "Louisiana", "ME": "Maine", "MD": "Maryland",
	"MA": "Massachusetts", "MI": "Michigan", "MN": "Minnesota", "MS": "Mississippi",
	"MO": "Missouri", "MT": "Montana", "NE": "Nebraska", "NV": "Nevada",
	"NH": "New Hampshire", "NJ": "New Jersey", "NM": "New Mexico", "NY": "New York",
	"NC": "North Carolina", "ND": "North Dakota", "OH": "Ohio", "OK": "Oklahoma",
	"OR": "Oregon", "PA": "Pennsylvania", "RI": "Rhode Island", "SC": "South Carolina",
	"SD": "South Dakota", "TN": "Tennessee", "TX": "Texas", "UT": "Utah",
	"VT": "Vermont", "VA": "Virginia", "WA": "Washington", "WV": "West Virginia",
	"WI": "Wisconsin", "WY": "Wyoming",
}

var (
	truckNumberRegex = regexp.MustCompile(`\d+`)
	anyDigitRegex    = regexp.MustCompile(`[0-9]`)
	word53Regex      = regexp.MustCompile(`\b53\b`)
)

// extractOfferedByName tries HTML first, then plain text.
// Returns "" if not found.
func ExtractOfferedByName(bodyHTML, bodyPlain string) string {
	// HTML table pattern: <td>Offered By</td><td>Gabriel Feliciano</td>
	reHTML := regexp.MustCompile(`(?i)<td[^>]*>\s*Offered\s*By\s*</td>\s*<td[^>]*>\s*([^<]+)\s*</td>`)
	if m := reHTML.FindStringSubmatch(bodyHTML); len(m) == 2 {
		return strings.TrimSpace(html.UnescapeString(m[1]))
	}

	// Plain text fallback: "Offered By Gabriel Feliciano"
	reText := regexp.MustCompile(`(?i)\bOffered\s*By\s+([^\r\n]+)`)
	if m := reText.FindStringSubmatch(bodyPlain); len(m) == 2 {
		return strings.TrimSpace(m[1])
	}

	return ""
}

// --- NEW: pull first anchor href from raw HTML (prefers the CTA if present)
func extractExternalLinkFromHTML(htmlSrc string) string {
	// Try to find the specific CTA button first
	reCTA := regexp.MustCompile(`(?is)<a[^>]+href=["'](https?://[^"']+)["'][^>]*>\s*Click\s*here\s*to\s*provide\s*your\s*all-?in\s*rate\s*</a>`)
	if m := reCTA.FindStringSubmatch(htmlSrc); len(m) == 2 {
		return trimTrailingPunct(m[1])
	}
	// Otherwise grab the first href in the HTML
	reAny := regexp.MustCompile(`(?is)<a[^>]+href=["'](https?://[^"']+)["']`)
	if m := reAny.FindStringSubmatch(htmlSrc); len(m) == 2 {
		return trimTrailingPunct(m[1])
	}
	return ""
}
