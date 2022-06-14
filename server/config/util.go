// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package config

import (
	"net/url"
	"strings"
)

// parseUrls parse a string into multiple urls.
func parseUrls(s string) ([]url.URL, error) {
	items := strings.Split(s, ",")
	urls := make([]url.URL, 0, len(items))
	for _, item := range items {
		u, err := url.Parse(item)
		if err != nil {
			return nil, err
		}

		urls = append(urls, *u)
	}

	return urls, nil
}
