package marktplaats

// Mirrors scraper-python/mpscraper/mpscraper.py constants.
const (
	BaseURL     = "https://marktplaats.nl"
	SortOptions = "#sortBy:SORT_INDEX|sortOrder:DECREASING"

	NextDataScriptID = "__NEXT_DATA__"
	ContentID        = "content"
	SelectCategoryID = "categoryId"
	ListingRootID    = "listing-root"
	ListingConfigVar = "__CONFIG__"

	AllCategoriesID = 0

	// Sponsored listing itemId prefix — skip (Python MARKTPLAATS_ADVERTISEMENT_PREFIX).
	AdItemIDPrefix = "a"
)
