package clientbot

import (
	"encoding/csv"
	"regexp"
	"strconv"
	"strings"

	"github.com/marktplaats-scraper/scraper-golang/internal/marktplaats"
)

// Колонки как LISTINGS_CSV_COL_MAP в telegram_bot/database.py
var listingsColMap = map[string]string{
	"название товара": "title", "название": "title", "title": "title",
	"ник продавца": "seller_name", "имя продавца": "seller_name", "имя": "seller_name",
	"seller_name": "seller_name", "seller": "seller_name", "продавец": "seller_name", "shop_name": "seller_name",
	"ссылка на товар": "listing_url", "ссылка на объявление": "listing_url",
	"listing_url": "listing_url", "url": "listing_url",
	"цена": "price", "price": "price", "price_label": "price",
	"город": "city_name", "локация": "city_name", "местоположение": "city_name",
	"location_label": "city_name", "city": "city_name", "city_name": "city_name",
	"описание": "description", "description": "description",
	"никнейм": "seller_name", "nickname": "seller_name",
}

var listingURLBlacklist = []string{"продавца", "магазин", "фото", "seller_url", "photo"}

var listingURLDomains = []string{"marktplaats", "2dehands", "poshmark"}

var reItemID = regexp.MustCompile(`/m(\d+)(?:-|$|/)`)
var rePoshmarkListingID = regexp.MustCompile(`(?i)-([0-9a-f]{24})(?:\?.*)?$`)

func itemIDFromURL(url string) string {
	if url == "" {
		return ""
	}
	if m := reItemID.FindStringSubmatch(url); len(m) >= 2 {
		return "m" + m[1]
	}
	if strings.Contains(strings.ToLower(url), "poshmark.com") {
		if m := rePoshmarkListingID.FindStringSubmatch(strings.TrimSpace(url)); len(m) >= 2 {
			return "p" + strings.ToLower(m[1])
		}
	}
	return ""
}

func parsePriceToCents(s string) int {
	s = strings.TrimSpace(s)
	if s == "" {
		return 0
	}
	low := strings.ToLower(s)
	if low == "bieden" || low == "gratis" || low == "zie omschrijving" || low == "-" {
		return 0
	}
	s = strings.ReplaceAll(s, "€", "")
	s = strings.ReplaceAll(s, "$", "")
	s = strings.ReplaceAll(s, "\u00a0", "")
	s = strings.TrimSpace(s)
	if strings.Contains(s, ".") && strings.Contains(s, ",") {
		s = strings.ReplaceAll(s, ".", "")
		s = strings.ReplaceAll(s, ",", ".")
	} else if strings.Contains(s, ",") {
		s = strings.ReplaceAll(s, ",", ".")
	}
	f, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return 0
	}
	return int(f * 100)
}

func matchListingColumns(header []string) map[string]int {
	colIdx := make(map[string]int)
	// Экспорт 2dehands: shop_name и seller_id раньше перехватывали seller_name; photo_label — title.
	for i, h := range header {
		switch h {
		case "seller_name":
			colIdx["seller_name"] = i
		case "title":
			colIdx["title"] = i
		case "listing_url":
			colIdx["listing_url"] = i
		}
	}
	if _, ok := colIdx["listing_url"]; !ok {
		for i, h := range header {
			if h == "url" {
				colIdx["listing_url"] = i
				break
			}
		}
	}
	// Экспорт Poshmark: колонка «Никнейм» приоритетнее «Имя» для seller_name.
	for i, h := range header {
		if h == "никнейм" {
			colIdx["seller_name"] = i
			break
		}
	}
	// точное совпадение
	for i, h := range header {
		for key, dbKey := range listingsColMap {
			if key == h {
				if _, ok := colIdx[dbKey]; !ok {
					colIdx[dbKey] = i
				}
				break
			}
		}
	}
	// частичное
	for i, h := range header {
		for key, dbKey := range listingsColMap {
			if _, ok := colIdx[dbKey]; ok {
				continue
			}
			if strings.Contains(h, key) {
				if dbKey == "listing_url" {
					skip := false
					for _, bl := range listingURLBlacklist {
						if strings.Contains(h, bl) {
							skip = true
							break
						}
					}
					if skip {
						continue
					}
				}
				colIdx[dbKey] = i
				break
			}
		}
	}
	return colIdx
}

// ParseListingsCSV как parse_listings_csv в Python → marktplaats.Listing для рассылки.
func ParseListingsCSV(content string) []marktplaats.Listing {
	content = strings.TrimSpace(content)
	if content == "" {
		return nil
	}
	for _, delim := range []rune{',', ';'} {
		r := csv.NewReader(strings.NewReader(content))
		r.Comma = delim
		r.TrimLeadingSpace = true
		rows, err := r.ReadAll()
		if err != nil || len(rows) < 2 {
			continue
		}
		header := make([]string, len(rows[0]))
		for i, h := range rows[0] {
			header[i] = strings.ToLower(strings.TrimSpace(strings.TrimPrefix(h, "\ufeff")))
		}
		colIdx := matchListingColumns(header)
		if _, ok := colIdx["seller_name"]; !ok {
			continue
		}
		if _, ok := colIdx["listing_url"]; !ok {
			continue
		}
		maxIdx := 0
		for _, idx := range colIdx {
			if idx > maxIdx {
				maxIdx = idx
			}
		}
		var out []marktplaats.Listing
		for _, row := range rows[1:] {
			if len(row) <= maxIdx {
				continue
			}
			seller := strings.TrimSpace(row[colIdx["seller_name"]])
			url := strings.TrimSpace(row[colIdx["listing_url"]])
			if seller == "" || url == "" {
				continue
			}
			low := strings.ToLower(url)
			okDomain := false
			for _, d := range listingURLDomains {
				if strings.Contains(low, d) {
					okDomain = true
					break
				}
			}
			if !okDomain {
				continue
			}
			title := "Товар"
			if i, ok := colIdx["title"]; ok {
				if t := strings.TrimSpace(row[i]); t != "" {
					title = t
				}
			}
			priceStr := ""
			if i, ok := colIdx["price"]; ok {
				priceStr = strings.TrimSpace(row[i])
			}
			city := ""
			if i, ok := colIdx["city_name"]; ok {
				city = strings.TrimSpace(row[i])
			}
			desc := ""
			if i, ok := colIdx["description"]; ok {
				desc = strings.TrimSpace(row[i])
			}
			out = append(out, marktplaats.Listing{
				ItemID:      itemIDFromURL(url),
				Title:       title,
				SellerName:  seller,
				ListingURL:  url,
				PriceCents:  parsePriceToCents(priceStr),
				CityName:    city,
				Description: desc,
			})
		}
		if len(out) > 0 {
			return out
		}
	}
	return nil
}
