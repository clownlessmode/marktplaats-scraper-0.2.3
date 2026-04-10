package marktplaats

import (
	"strings"
	"time"

	"github.com/PuerkitoBio/goquery"
	"github.com/playwright-community/playwright-go"
	"github.com/tidwall/gjson"
)

// listingSlowFromSearchItem mirrors Python slow path: search row + listing page + __CONFIG__.
func (s *Scraper) listingSlowFromSearchItem(searchRaw []byte, parent Category, maxAgeHours *float64) (Listing, error) {
	if !gjson.ValidBytes(searchRaw) {
		return Listing{}, ErrNextDataMissing
	}
	g := gjson.ParseBytes(searchRaw)
	itemID := g.Get("itemId").String()
	title := FormatText(g.Get("title").String())
	vip := g.Get("vipUrl").String()
	site := s.BaseURL
	if site == "" {
		site = BaseURL
	}
	listingURL := strings.TrimRight(site, "/") + vip
	childID := int(g.Get("categoryId").Int())

	var imgs []string
	for _, pic := range g.Get("pictures").Array() {
		u := pic.Get("extraExtraLargeUrl").String()
		if u == "" {
			u = pic.Get("largeUrl").String()
		}
		if u == "" {
			u = pic.Get("mediumUrl").String()
		}
		if u != "" {
			imgs = append(imgs, u)
		}
	}

	countryCode := g.Get("location.countryAbbreviation").String()
	cityName := g.Get("location.cityName").String()
	var verticals []string
	for _, v := range g.Get("verticals").Array() {
		if t := v.String(); t != "" {
			verticals = append(verticals, t)
		}
	}
	sellerID := g.Get("sellerInformation.sellerId").String()

	s.sleepAdaptive()
	if err := s.navigate(listingURL); err != nil {
		return Listing{}, err
	}
	_, _ = s.Page.WaitForSelector("#"+ListingRootID, playwright.PageWaitForSelectorOptions{
		Timeout: playwright.Float(s.TimeoutMS),
		State:   playwright.WaitForSelectorStateAttached,
	})

	html, err := s.HTMLContent()
	if err != nil {
		return Listing{}, err
	}
	desc, types, services, err := parseDetailDescriptionAndAttributes(html)
	if err != nil {
		desc = ""
	}

	cfgStr, err := evalListingConfigJSON(s.Page)
	if err != nil {
		cfgStr = ""
	}
	adType := gjson.Get(cfgStr, "listing.adType").String()
	priceType := gjson.Get(cfgStr, "listing.priceInfo.priceType").String()
	priceCents := int(gjson.Get(cfgStr, "listing.priceInfo.priceCents").Int())
	viewCount := int(gjson.Get(cfgStr, "listing.stats.viewCount").Int())
	fav := int(gjson.Get(cfgStr, "listing.stats.favoritedCount").Int())
	listedTS := gjson.Get(cfgStr, "listing.stats.since").String()
	if listedTS != "" {
		listedTS = strings.TrimSpace(listedTS)
	}

	if maxAgeHours != nil && *maxAgeHours > 0 {
		if h, ok := AgeHoursSinceListed(listedTS, time.Now().UTC()); ok && h > *maxAgeHours {
			return Listing{}, &ListingTooOldDetails{
				ItemID: itemID, Title: title, ListingURL: listingURL, ListedTS: listedTS,
				AgeHours: h, MaxHours: *maxAgeHours,
			}
		}
	}

	return Listing{
		ItemID:      itemID,
		SellerID:    sellerID,
		ParentCatID: parent.ID,
		ChildCatID:  childID,
		Verticals:   verticals,
		AdType:      adType,
		Title:       title,
		Description: FormatText(desc),
		PriceType:   priceType,
		PriceCents:  priceCents,
		Types:       types,
		Services:    services,
		ListingURL:  listingURL,
		ImageURLs:   imgs,
		CityName:    cityName,
		CountryCode: countryCode,
		ListedTS:    listedTS,
		CrawledTS:   utcISO8601(),
		ViewCount:   viewCount,
		Favorited:   fav,
	}, nil
}

func evalListingConfigJSON(page playwright.Page) (string, error) {
	v, err := page.Evaluate(`() => {
		const c = window.__CONFIG__;
		return c ? JSON.stringify(c) : "";
	}`)
	if err != nil {
		return "", err
	}
	s, _ := v.(string)
	return s, nil
}

func parseDetailDescriptionAndAttributes(html string) (desc string, types []string, services []string, err error) {
	doc, err := goquery.NewDocumentFromReader(strings.NewReader(html))
	if err != nil {
		return "", nil, nil, err
	}
	desc = strings.TrimSpace(doc.Find("div.Description-description").First().Text())
	desc = FormatText(desc)

	doc.Find("div.Attributes-item").Each(func(i int, sel *goquery.Selection) {
		label := strings.ToLower(strings.TrimSpace(sel.Find("strong.Attributes-label").First().Text()))
		val := strings.TrimSpace(sel.Find("span.Attributes-value").First().Text())
		if val == "" {
			return
		}
		vals := map[string]struct{}{}
		for _, p := range strings.Split(val, ", ") {
			p = strings.TrimSpace(p)
			if p != "" {
				vals[p] = struct{}{}
			}
		}
		if len(vals) == 0 {
			vals[val] = struct{}{}
		}
		switch label {
		case "type", "soort":
			for v := range vals {
				types = append(types, v)
			}
		case "service", "dienst", "delivery", "bezorging":
			for v := range vals {
				services = append(services, v)
			}
		}
	})
	return desc, types, services, nil
}
