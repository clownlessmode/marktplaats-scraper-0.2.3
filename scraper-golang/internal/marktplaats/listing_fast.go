package marktplaats

import (
	"encoding/json"
	"strings"
	"time"

	"github.com/tidwall/gjson"
)

// ListingFastFromSearchItem mirrors Python __listing_from_res_listing (fast path).
// maxAgeHours: если не nil и > 0, поле date из поиска сравнивается с порогом (как max_age_hours в Python для детальной страницы).
func ListingFastFromSearchItem(raw []byte, parent Category, siteBase string, crawledISO string, maxAgeHours *float64) (Listing, error) {
	if !gjson.ValidBytes(raw) {
		return Listing{}, ErrNextDataMissing
	}
	g := gjson.ParseBytes(raw)
	itemID := g.Get("itemId").String()
	title := FormatText(g.Get("title").String())
	vip := g.Get("vipUrl").String()
	if siteBase == "" {
		siteBase = BaseURL
	}
	listingURL := strings.TrimRight(siteBase, "/") + vip

	desc := g.Get("description").String()
	if desc == "" {
		desc = g.Get("categorySpecificDescription").String()
	}
	desc = FormatText(desc)

	priceType := g.Get("priceInfo.priceType").String()
	priceCents := int(g.Get("priceInfo.priceCents").Int())

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
		s := v.String()
		if s != "" {
			verticals = append(verticals, s)
		}
	}

	sellerID := g.Get("sellerInformation.sellerId").String()
	sellerName := g.Get("sellerInformation.sellerName").String()
	verified := g.Get("sellerInformation.isVerified").Bool()
	sellerWeb := g.Get("sellerInformation.sellerWebsiteUrl").String()

	listedTS := g.Get("date").String()
	childID := int(g.Get("categoryId").Int())

	var types, services []string
	var attrsCombined []map[string]any
	for _, attr := range g.Get("attributes").Array() {
		attrsCombined = append(attrsCombined, jsonRawToMap(attr.Raw))
		key := strings.ToLower(attr.Get("key").String())
		val := attr.Get("value").String()
		if val == "" {
			continue
		}
		switch key {
		case "type", "soort":
			types = append(types, val)
		case "service", "dienst", "delivery", "bezorging":
			services = append(services, val)
		}
	}
	for _, attr := range g.Get("extendedAttributes").Array() {
		attrsCombined = append(attrsCombined, jsonRawToMap(attr.Raw))
		key := strings.ToLower(attr.Get("key").String())
		val := attr.Get("value").String()
		if val == "" {
			continue
		}
		switch key {
		case "type", "soort":
			types = append(types, val)
		case "service", "dienst", "delivery", "bezorging":
			services = append(services, val)
		}
	}

	lat := g.Get("location.latitude").Float()
	lon := g.Get("location.longitude").Float()
	dist := int(g.Get("location.distanceMeters").Int())
	cname := g.Get("location.countryName").String()

	attrsJSON := ""
	if len(attrsCombined) > 0 {
		b, _ := json.Marshal(attrsCombined)
		attrsJSON = string(b)
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
		ItemID:         itemID,
		SellerID:       sellerID,
		ParentCatID:    parent.ID,
		ChildCatID:     childID,
		Verticals:      verticals,
		AdType:         priceType,
		Title:          title,
		Description:    desc,
		PriceType:      priceType,
		PriceCents:     priceCents,
		Types:          types,
		Services:       services,
		ListingURL:     listingURL,
		ImageURLs:      imgs,
		CityName:       cityName,
		CountryCode:    countryCode,
		ListedTS:       listedTS,
		CrawledTS:      crawledISO,
		ViewCount:      0,
		Favorited:      0,
		SellerName:     sellerName,
		Latitude:       lat,
		Longitude:      lon,
		DistanceM:      dist,
		CountryName:    cname,
		PriorityProd:   g.Get("priorityProduct").String(),
		Traits:         stringsToSlice(g.Get("traits")),
		CatSpecDesc:    FormatText(g.Get("categorySpecificDescription").String()),
		Reserved:       g.Get("reserved").Bool(),
		NapAvail:       g.Get("napAvailable").Bool(),
		Urgency:        g.Get("urgencyFeatureActive").Bool(),
		Verified:       verified,
		SellerWebURL:   sellerWeb,
		AttributesJSON: attrsJSON,
	}, nil
}

func stringsToSlice(r gjson.Result) []string {
	var out []string
	for _, x := range r.Array() {
		if s := x.String(); s != "" {
			out = append(out, s)
		}
	}
	return out
}

func jsonRawToMap(raw string) map[string]any {
	var m map[string]any
	_ = json.Unmarshal([]byte(raw), &m)
	if m == nil {
		m = map[string]any{}
	}
	return m
}

func utcISO8601() string {
	return time.Now().UTC().Format(time.RFC3339Nano)
}
