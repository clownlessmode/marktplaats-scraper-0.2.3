// Package listingsdb — SQLite совместимая с scraper-python/telegram_bot.database (таблица listings).
package listingsdb

import (
	"database/sql"
	"os"
	"path/filepath"
	"strings"

	_ "modernc.org/sqlite"

	"github.com/marktplaats-scraper/scraper-golang/internal/marktplaats"
)

// Open открывает БД, создаёт каталог и схему при необходимости.
func Open(path string) (*sql.DB, error) {
	path, err := filepath.Abs(path)
	if err != nil {
		return nil, err
	}
	dir := filepath.Dir(path)
	if dir != "" && dir != "." {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return nil, err
		}
	}
	db, err := sql.Open("sqlite", "file:"+filepath.ToSlash(path)+"?mode=rwc")
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	if err := db.Ping(); err != nil {
		_ = db.Close()
		return nil, err
	}
	if err := initSchema(db); err != nil {
		_ = db.Close()
		return nil, err
	}
	return db, nil
}

func initSchema(db *sql.DB) error {
	stmts := []string{
		`CREATE TABLE IF NOT EXISTS listings (
			item_id TEXT PRIMARY KEY,
			seller_id TEXT,
			parent_category_id INTEGER,
			child_category_id INTEGER,
			category_verticals TEXT,
			ad_type TEXT,
			title TEXT,
			description TEXT,
			price_type TEXT,
			price_cents INTEGER,
			types TEXT,
			services TEXT,
			listing_url TEXT,
			image_urls TEXT,
			city_name TEXT,
			country_code TEXT,
			listed_timestamp TEXT,
			crawled_timestamp TEXT,
			view_count INTEGER,
			favorited_count INTEGER,
			seller_name TEXT,
			latitude REAL,
			longitude REAL,
			distance_meters INTEGER,
			country_name TEXT,
			priority_product TEXT,
			traits TEXT,
			category_specific_description TEXT,
			reserved INTEGER,
			nap_available INTEGER,
			urgency_feature_active INTEGER,
			is_verified INTEGER,
			seller_website_url TEXT,
			attributes_json TEXT
		)`,
		`CREATE INDEX IF NOT EXISTS idx_listings_date ON listings(listed_timestamp)`,
		`CREATE INDEX IF NOT EXISTS idx_listings_price ON listings(price_cents)`,
		`CREATE INDEX IF NOT EXISTS idx_listings_city ON listings(city_name)`,
	}
	for _, q := range stmts {
		if _, err := db.Exec(q); err != nil {
			return err
		}
	}
	// Миграции как в Python: ADD COLUMN, если таблица старая.
	alters := []string{
		`ALTER TABLE listings ADD COLUMN seller_name TEXT`,
		`ALTER TABLE listings ADD COLUMN latitude REAL`,
		`ALTER TABLE listings ADD COLUMN longitude REAL`,
		`ALTER TABLE listings ADD COLUMN distance_meters INTEGER`,
		`ALTER TABLE listings ADD COLUMN country_name TEXT`,
		`ALTER TABLE listings ADD COLUMN priority_product TEXT`,
		`ALTER TABLE listings ADD COLUMN traits TEXT`,
		`ALTER TABLE listings ADD COLUMN category_specific_description TEXT`,
		`ALTER TABLE listings ADD COLUMN reserved INTEGER`,
		`ALTER TABLE listings ADD COLUMN nap_available INTEGER`,
		`ALTER TABLE listings ADD COLUMN urgency_feature_active INTEGER`,
		`ALTER TABLE listings ADD COLUMN is_verified INTEGER`,
		`ALTER TABLE listings ADD COLUMN seller_website_url TEXT`,
		`ALTER TABLE listings ADD COLUMN attributes_json TEXT`,
	}
	for _, q := range alters {
		if _, err := db.Exec(q); err != nil && !isDupColumn(err) {
			return err
		}
	}
	return initMailSchema(db)
}

// ClearListings удаляет все строки из listings (для dev: чистый прогон без «дубликатов в базе»).
// Таблицы почты (emails, email_templates, …) не трогаются.
func ClearListings(db *sql.DB) error {
	_, err := db.Exec(`DELETE FROM listings`)
	return err
}

func isDupColumn(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "duplicate column")
}

func joinPipe(ss []string) string {
	if len(ss) == 0 {
		return ""
	}
	return strings.Join(ss, "|")
}

func boolInt(b bool) int {
	if b {
		return 1
	}
	return 0
}

func sanitize(s string) string {
	return strings.ToValidUTF8(s, "")
}

// Upsert вставляет или заменяет объявление (INSERT OR REPLACE), как save_listing_to_db в fetch_listings.py.
func Upsert(db *sql.DB, l marktplaats.Listing) error {
	_, err := db.Exec(`
INSERT OR REPLACE INTO listings (
	item_id, seller_id, parent_category_id, child_category_id, category_verticals, ad_type,
	title, description, price_type, price_cents, types, services, listing_url, image_urls,
	city_name, country_code, listed_timestamp, crawled_timestamp, view_count, favorited_count,
	seller_name, latitude, longitude, distance_meters, country_name, priority_product, traits,
	category_specific_description, reserved, nap_available, urgency_feature_active, is_verified,
	seller_website_url, attributes_json
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		sanitize(l.ItemID),
		nullIfEmpty(sanitize(l.SellerID)),
		l.ParentCatID,
		l.ChildCatID,
		nullIfEmpty(sanitize(joinPipe(l.Verticals))),
		nullIfEmpty(sanitize(l.AdType)),
		nullIfEmpty(sanitize(l.Title)),
		nullIfEmpty(sanitize(l.Description)),
		nullIfEmpty(sanitize(l.PriceType)),
		l.PriceCents,
		nullIfEmpty(sanitize(joinPipe(l.Types))),
		nullIfEmpty(sanitize(joinPipe(l.Services))),
		nullIfEmpty(sanitize(l.ListingURL)),
		nullIfEmpty(sanitize(joinPipe(l.ImageURLs))),
		nullIfEmpty(sanitize(l.CityName)),
		nullIfEmpty(sanitize(l.CountryCode)),
		nullIfEmpty(sanitize(l.ListedTS)),
		nullIfEmpty(sanitize(l.CrawledTS)),
		l.ViewCount,
		l.Favorited,
		nullIfEmpty(sanitize(l.SellerName)),
		l.Latitude,
		l.Longitude,
		l.DistanceM,
		nullIfEmpty(sanitize(l.CountryName)),
		nullIfEmpty(sanitize(l.PriorityProd)),
		nullIfEmpty(sanitize(joinPipe(l.Traits))),
		nullIfEmpty(sanitize(l.CatSpecDesc)),
		boolInt(l.Reserved),
		boolInt(l.NapAvail),
		boolInt(l.Urgency),
		boolInt(l.Verified),
		nullIfEmpty(sanitize(l.SellerWebURL)),
		nullIfEmpty(sanitize(l.AttributesJSON)),
	)
	return err
}

func nullIfEmpty(s string) any {
	if s == "" {
		return nil
	}
	return s
}

// LoadItemIDs все item_id из listings (для пропуска уже сохранённых).
func LoadItemIDs(db *sql.DB) (map[string]struct{}, error) {
	rows, err := db.Query(`SELECT item_id FROM listings`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make(map[string]struct{})
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		if id != "" {
			out[id] = struct{}{}
		}
	}
	return out, rows.Err()
}
