package loadoneapi

import (
	"fmt"
	"net/url"
	"regexp"
	"strings"
)

var uuidRe = regexp.MustCompile(`(?i)\b[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\b`)

// ExtractAccessKeyFromURL tries a few safe, local heuristics.
// It does not follow tracking redirects. If the link is a SendGrid wrapper,
// the caller may need to resolve the redirect target first.
func ExtractAccessKeyFromURL(raw string) (string, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return "", ErrAccessKeyNotFound
	}

	if m := uuidRe.FindString(raw); m != "" {
		return m, nil
	}

	u, err := url.Parse(raw)
	if err != nil {
		return "", fmt.Errorf("parse tracking URL: %w", err)
	}

	q := u.Query()
	if v := strings.TrimSpace(q.Get("accessKey")); v != "" {
		return v, nil
	}
	if v := strings.TrimSpace(q.Get("accesskey")); v != "" {
		return v, nil
	}

	for _, seg := range strings.Split(u.Path, "/") {
		if uuidRe.MatchString(seg) {
			return uuidRe.FindString(seg), nil
		}
	}

	return "", ErrAccessKeyNotFound
}
