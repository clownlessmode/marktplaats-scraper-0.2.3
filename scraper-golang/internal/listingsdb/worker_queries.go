package listingsdb

import (
	"database/sql"
)

// RegisterPendingUser INSERT OR IGNORE authorized=0 (как register_pending_user в Python).
func RegisterPendingUser(db *sql.DB, userID int64) error {
	_, err := db.Exec(`
		INSERT OR IGNORE INTO users (user_id, authorized, created_at, shift_active) VALUES (?, 0, ?, 0)`,
		userID, nowISO())
	return err
}

// IsAuthorized как в Python is_authorized: authorized=1 (блок проверяйте отдельно IsBlocked).
func IsAuthorized(db *sql.DB, userID int64) bool {
	var auth int
	err := db.QueryRow(`SELECT COALESCE(authorized,0) FROM users WHERE user_id = ?`, userID).Scan(&auth)
	if err != nil {
		return false
	}
	return auth == 1
}

// IsBlocked пользователь в blocked_users.
func IsBlocked(db *sql.DB, userID int64) bool {
	var one int
	err := db.QueryRow(`SELECT 1 FROM blocked_users WHERE user_id = ? LIMIT 1`, userID).Scan(&one)
	return err == nil
}

// SetShiftActive обновляет shift_active.
func SetShiftActive(db *sql.DB, userID int64, active bool) error {
	v := 0
	if active {
		v = 1
	}
	_, err := db.Exec(`UPDATE users SET shift_active = ? WHERE user_id = ?`, v, userID)
	return err
}

// IsShiftActive флаг смены (если строки в users нет — false).
func IsShiftActive(db *sql.DB, userID int64) bool {
	var v sql.NullInt64
	err := db.QueryRow(`SELECT shift_active FROM users WHERE user_id = ?`, userID).Scan(&v)
	if err != nil || !v.Valid {
		return false
	}
	return v.Int64 != 0
}

// WorkerListingToday строка для «товары сегодня».
type WorkerListingToday struct {
	ItemID      string
	ReceivedAt  string
	Title       string
	PriceCents  sql.NullInt64
	ListingURL  string
	CityName    string
}

// WorkerListingsToday товары из worker_listings за сегодня (UTC date) с JOIN listings.
func WorkerListingsToday(db *sql.DB, userID int64) ([]WorkerListingToday, error) {
	rows, err := db.Query(`
		SELECT wl.item_id, COALESCE(wl.received_at,''), COALESCE(l.title,''), l.price_cents,
		       COALESCE(l.listing_url,''), COALESCE(l.city_name,'')
		FROM worker_listings wl
		LEFT JOIN listings l ON l.item_id = wl.item_id
		WHERE wl.user_id = ? AND date(wl.received_at) = date('now')
		ORDER BY wl.received_at DESC`,
		userID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []WorkerListingToday
	for rows.Next() {
		var r WorkerListingToday
		if err := rows.Scan(&r.ItemID, &r.ReceivedAt, &r.Title, &r.PriceCents, &r.ListingURL, &r.CityName); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}
