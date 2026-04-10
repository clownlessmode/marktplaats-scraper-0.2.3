package marktplaats

import (
	"fmt"
	"strings"
)

// Category is parent or leaf category (Python mpscraper.Category).
type Category struct {
	ID  int
	URL string
}

func categoryURLWithPage(categoryURL string, pageNumber int) string {
	if !strings.HasSuffix(categoryURL, "/") {
		categoryURL += "/"
	}
	return fmt.Sprintf("%sp/%d/%s", categoryURL, pageNumber, SortOptions)
}
