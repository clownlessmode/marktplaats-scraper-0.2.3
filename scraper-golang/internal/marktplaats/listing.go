package marktplaats

// Listing mirrors scraper-python/mpscraper/listing.py (Listing dataclass).
type Listing struct {
	ItemID       string
	SellerID     string
	ParentCatID  int
	ChildCatID   int
	Verticals    []string
	AdType       string
	Title        string
	Description  string
	PriceType    string
	PriceCents   int
	Types        []string
	Services     []string
	ListingURL   string
	ImageURLs    []string
	CityName     string
	CountryCode  string
	ListedTS     string
	CrawledTS    string
	ViewCount    int
	Favorited    int
	SellerName   string
	Latitude     float64
	Longitude    float64
	DistanceM    int
	CountryName  string
	PriorityProd string
	Traits       []string
	CatSpecDesc  string
	Reserved     bool
	NapAvail     bool
	Urgency      bool
	Verified     bool
	SellerWebURL string
	AttributesJSON string
}
