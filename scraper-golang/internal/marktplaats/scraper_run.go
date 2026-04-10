package marktplaats

import (
	"errors"
	"fmt"
	"strings"
	"sync"

	"github.com/playwright-community/playwright-go"
	"github.com/tidwall/gjson"

	"github.com/marktplaats-scraper/scraper-golang/internal/prettylog"
)

func listingURLFromRow(g gjson.Result, base string) string {
	site := base
	if site == "" {
		site = BaseURL
	}
	return strings.TrimRight(site, "/") + g.Get("vipUrl").String()
}

// GetListingsOptions mirrors Python get_listings kwargs.
type GetListingsOptions struct {
	OnBatch     func([]Listing)
	MaxAgeHours *float64 // slow path: ErrListingTooOld → caller wraps CategoryStaleError
	SkipCount   bool
	ExistingIDs map[string]struct{}
	// ItemIDsMu + общая ExistingIDs: параллельные воркеры используют одну карту id (без копии).
	ItemIDsMu *sync.Mutex
	// AuditMu защищает AuditTrail при параллельном скрапе.
	AuditMu *sync.Mutex
	// AuditTrail если не nil — дополняется каждой просмотренной строкой выдачи (время, проходит/нет).
	AuditTrail *[]ListingAuditRow
}

func appendListingAudit(opt *GetListingsOptions, row ListingAuditRow) {
	if opt == nil || opt.AuditTrail == nil {
		return
	}
	if opt.AuditMu != nil {
		opt.AuditMu.Lock()
		defer opt.AuditMu.Unlock()
	}
	*opt.AuditTrail = append(*opt.AuditTrail, row)
}

// GetParentCategories loads homepage and parses categories (HTML then __CONFIG__).
func (s *Scraper) GetParentCategories() ([]Category, error) {
	if err := s.navigate(s.BaseURL); err != nil {
		return nil, err
	}
	html, err := s.HTMLContent()
	if err != nil {
		return nil, err
	}
	cats, err := ParentCategoriesFromHTML(html, s.BaseURL)
	if err != nil {
		return nil, err
	}
	if len(cats) > 0 {
		return dedupeCategories(cats), nil
	}
	cfg, err := EvalWindowConfig(s.Page)
	if err != nil {
		return nil, err
	}
	cats, err = ParentCategoriesFromConfigJSON(cfg, s.BaseURL)
	if err != nil {
		return nil, err
	}
	return dedupeCategories(cats), nil
}

func dedupeCategories(in []Category) []Category {
	seen := map[int]struct{}{}
	var out []Category
	for _, c := range in {
		if _, ok := seen[c.ID]; ok {
			continue
		}
		seen[c.ID] = struct{}{}
		out = append(out, c)
	}
	return out
}

// GetSubcategories loads parent category page and returns leaf categories.
func (s *Scraper) GetSubcategories(parent Category) ([]Category, error) {
	if err := s.navigate(parent.URL); err != nil {
		return nil, err
	}
	idSel := "#" + fmt.Sprint(parent.ID)
	_, _ = s.Page.WaitForSelector(idSel, playwright.PageWaitForSelectorOptions{
		Timeout: playwright.Float(s.TimeoutMS),
		State:   playwright.WaitForSelectorStateAttached,
	})
	html, err := s.HTMLContent()
	if err != nil {
		return nil, err
	}
	return SubcategoriesFromHTML(html, parent)
}

// ListingsCount fetches category page and parses total from counter (Python listings_count).
func (s *Scraper) ListingsCount(cat Category) (int, error) {
	if err := s.navigate(cat.URL); err != nil {
		return 0, err
	}
	_, _ = s.Page.WaitForSelector("#"+ContentID, playwright.PageWaitForSelectorOptions{
		Timeout: playwright.Float(s.TimeoutMS),
		State:   playwright.WaitForSelectorStateAttached,
	})
	html, err := s.HTMLContent()
	if err != nil {
		return 0, err
	}
	return ListingsCountFromHTML(html)
}

// GetListings walks subcategories / parent, paginates search pages, builds listings (Python get_listings).
func (s *Scraper) GetListings(parent Category, limit int, opt GetListingsOptions) ([]Listing, error) {
	mu := opt.ItemIDsMu
	var itemIDs map[string]struct{}
	if mu != nil {
		if opt.ExistingIDs == nil {
			return nil, errors.New("marktplaats: ItemIDsMu требует ненулевой ExistingIDs (общая карта item_id)")
		}
		itemIDs = opt.ExistingIDs
	} else {
		itemIDs = make(map[string]struct{})
		if opt.ExistingIDs != nil {
			for k := range opt.ExistingIDs {
				itemIDs[k] = struct{}{}
			}
		}
	}

	var listingsCount int
	var err error
	if opt.SkipCount && limit > 0 {
		listingsCount = limit
	} else {
		listingsCount, err = s.ListingsCount(parent)
		if err != nil {
			prettylog.Pagef("подсчёт объявлений: запасной режим · %v", err)
			if limit > 0 {
				listingsCount = limit
			} else {
				listingsCount = 100
			}
		}
	}

	targetLimit := limit
	if limit > listingsCount || limit == 0 {
		targetLimit = listingsCount
	}
	maxListings := targetLimit
	if maxListings < 1 {
		maxListings = 1
	}

	var categories []Category
	sub, err := s.GetSubcategories(parent)
	if err != nil || len(sub) == 0 {
		categories = []Category{parent}
	} else {
		categories = sub
	}

	var out []Listing

outer:
	for _, cat := range categories {
		categoryID := cat.ID
		categoryURL := cat.URL
		pageNum := 1
		catSlug := slugFromURL(categoryURL)

		for len(out) < maxListings {
			s.sleepAdaptive()
			u := categoryURLWithPage(categoryURL, pageNum)
			if err := s.navigate(u); err != nil {
				return out, err
			}
			if err := s.waitNextData(); err != nil {
				prettylog.Pagef("__NEXT_DATA__ · категория %s страница %d · %v", catSlug, pageNum, err)
			}
			nd, errND := s.NextDataText()
			if errND != nil {
				return out, errND
			}
			if strings.TrimSpace(nd) == "" {
				html, _ := s.HTMLContent()
				if PageLooksBlocked(html) {
					return out, ErrForbidden
				}
				return out, ErrNextDataMissing
			}

			props := gjson.Get(nd, "props.pageProps")
			if props.Get("errorStatusCode").Exists() {
				code := int(props.Get("errorStatusCode").Int())
				if code == 403 {
					return out, ErrForbidden
				}
				prettylog.Pagef("категория «%s» недоступна · HTTP %d", catSlug, code)
				break
			}

			arr := props.Get("searchRequestAndResponse.listings").Array()
			if len(arr) == 0 {
				break
			}

			var pageBatch []Listing
			for _, row := range arr {
				if len(out) >= maxListings {
					break outer
				}

				raw := []byte(row.Raw)
				gRow := gjson.ParseBytes(raw)
				itemID := gRow.Get("itemId").String()
				titleRow := FormatText(gRow.Get("title").String())
				dateRow := gRow.Get("date").String()
				priceRow := int(gRow.Get("priceInfo.priceCents").Int())
				urlRow := listingURLFromRow(gRow, s.BaseURL)

				if len(itemID) > 0 && itemID[0:1] == AdItemIDPrefix {
					appendListingAudit(&opt, ListingAuditRow{
						ItemID: itemID, Title: titleRow, ListingURL: urlRow, PriceCents: priceRow,
						ListedTSRaw: dateRow, Passed: false, Status: "реклама (префикс a)",
					})
					continue
				}
				if mu != nil {
					mu.Lock()
				}
				_, dup := itemIDs[itemID]
				if dup {
					appendListingAudit(&opt, ListingAuditRow{
						ItemID: itemID, Title: titleRow, ListingURL: urlRow, PriceCents: priceRow,
						ListedTSRaw: dateRow, Passed: false, Status: "уже было (дубликат / в базе)",
					})
					if limit == listingsCount && listingsCount > 0 {
						maxListings--
						if maxListings < 1 {
							maxListings = 1
						}
					}
					if mu != nil {
						mu.Unlock()
					}
					continue
				}
				if mu != nil {
					mu.Unlock()
				}

				childID := int(gRow.Get("categoryId").Int())
				if len(categories) > 1 && childID != categoryID {
					return out, &UnexpectedCategoryIDError{Got: childID, Want: categoryID}
				}

				var listing Listing
				var le error
				if s.Fast {
					listing, le = ListingFastFromSearchItem(raw, parent, s.BaseURL, utcISO8601(), opt.MaxAgeHours)
					if le != nil {
						var too *ListingTooOldDetails
						if errors.As(le, &too) {
							// В fast, как раньше: старое объявление пропускаем, категорию не рвём (в slow — CategoryStale).
							appendListingAudit(&opt, ListingAuditRow{
								ItemID: too.ItemID, Title: too.Title, ListingURL: too.ListingURL, PriceCents: priceRow,
								ListedTSRaw: too.ListedTS, Passed: false,
								Status: fmt.Sprintf("старше порога (%.1f ч > %.1f ч), пропуск", too.AgeHours, too.MaxHours),
							})
							continue
						}
						appendListingAudit(&opt, ListingAuditRow{
							ItemID: itemID, Title: titleRow, ListingURL: urlRow, PriceCents: priceRow,
							ListedTSRaw: dateRow, Passed: false, Status: fmt.Sprintf("ошибка: %v", le),
						})
						prettylog.Pagef("разбор объявления %q · %v", itemID, le)
						continue
					}
				} else {
					listing, le = s.listingSlowFromSearchItem(raw, parent, opt.MaxAgeHours)
					if le != nil {
						var too *ListingTooOldDetails
						if errors.As(le, &too) {
							appendListingAudit(&opt, ListingAuditRow{
								ItemID: too.ItemID, Title: too.Title, ListingURL: too.ListingURL, PriceCents: priceRow,
								ListedTSRaw: too.ListedTS, Passed: false,
								Status: fmt.Sprintf("старше порога (%.1f ч > %.1f ч)", too.AgeHours, too.MaxHours),
							})
							return out, &CategoryStaleError{Listings: out, Msg: le.Error()}
						}
						appendListingAudit(&opt, ListingAuditRow{
							ItemID: itemID, Title: titleRow, ListingURL: urlRow, PriceCents: priceRow,
							ListedTSRaw: dateRow, Passed: false, Status: fmt.Sprintf("ошибка: %v", le),
						})
						prettylog.Pagef("пропуск объявления %q · %v", itemID, le)
						maxListings--
						if maxListings < 1 {
							maxListings = 1
						}
						continue
					}
				}

				if mu != nil {
					mu.Lock()
				}
				if _, taken := itemIDs[itemID]; taken {
					if mu != nil {
						mu.Unlock()
					}
					continue
				}
				itemIDs[itemID] = struct{}{}
				if mu != nil {
					mu.Unlock()
				}

				appendListingAudit(&opt, ListingAuditRow{
					ItemID: listing.ItemID, Title: listing.Title, ListingURL: listing.ListingURL, PriceCents: listing.PriceCents,
					ListedTSRaw: listing.ListedTS, Passed: true, Status: "принято в итог",
				})

				out = append(out, listing)
				pageBatch = append(pageBatch, listing)

				if targetLimit > 0 && len(out) >= targetLimit {
					if opt.OnBatch != nil && len(pageBatch) > 0 {
						opt.OnBatch(pageBatch)
					}
					break outer
				}
			}
			if opt.OnBatch != nil && len(pageBatch) > 0 {
				opt.OnBatch(pageBatch)
			}
			pageNum++
		}
	}

	return out, nil
}

func slugFromURL(u string) string {
	u = strings.TrimRight(u, "/")
	if i := strings.LastIndex(u, "/"); i >= 0 {
		return u[i+1:]
	}
	return u
}
