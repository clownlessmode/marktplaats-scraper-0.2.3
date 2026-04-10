package marktplaats

// ListingAuditRow одна строка ленты: что увидели на выдаче и прошло ли по правилам.
type ListingAuditRow struct {
	ItemID      string
	Title       string
	ListingURL  string
	PriceCents  int
	ListedTSRaw string // date из JSON или since со страницы — для ListingTimeSummary снаружи
	Passed      bool
	Status      string // коротко по-русски
}
