package listingsdb

import "database/sql"

// initMailSchema — таблицы как в telegram_bot/database.py (почта, шаблоны, rotation_state).
func initMailSchema(db *sql.DB) error {
	stmts := []string{
		`CREATE TABLE IF NOT EXISTS rotation_state (
			key TEXT PRIMARY KEY,
			value TEXT
		)`,
		`CREATE TABLE IF NOT EXISTS email_templates (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			name TEXT NOT NULL,
			body TEXT NOT NULL,
			created_at TEXT,
			user_id INTEGER
		)`,
		`CREATE TABLE IF NOT EXISTS emails (
			user_id INTEGER NOT NULL,
			email TEXT NOT NULL,
			password TEXT,
			created_at TEXT,
			blocked INTEGER DEFAULT 0,
			PRIMARY KEY (user_id, email)
		)`,
	}
	for _, q := range stmts {
		if _, err := db.Exec(q); err != nil {
			return err
		}
	}
	// Миграция: user_id у шаблонов (старые БД)
	if _, err := db.Exec(`ALTER TABLE email_templates ADD COLUMN user_id INTEGER`); err != nil && !isDupColumn(err) {
		return err
	}
	if _, err := db.Exec(`ALTER TABLE email_templates ADD COLUMN subject_template TEXT DEFAULT ''`); err != nil && !isDupColumn(err) {
		return err
	}
	// --- Telegram-боты (как telegram_bot/database.init_db) ---
	botStmts := []string{
		`CREATE TABLE IF NOT EXISTS users (
			user_id INTEGER PRIMARY KEY,
			authorized INTEGER DEFAULT 0,
			created_at TEXT
		)`,
		`CREATE TABLE IF NOT EXISTS blocked_users (
			user_id INTEGER PRIMARY KEY,
			blocked_at TEXT
		)`,
		`CREATE TABLE IF NOT EXISTS worker_listings (
			item_id TEXT,
			user_id INTEGER,
			received_at TEXT,
			PRIMARY KEY (item_id, user_id)
		)`,
		`CREATE INDEX IF NOT EXISTS idx_worker_listings_user ON worker_listings(user_id, received_at)`,
	}
	for _, q := range botStmts {
		if _, err := db.Exec(q); err != nil {
			return err
		}
	}
	if _, err := db.Exec(`ALTER TABLE users ADD COLUMN shift_active INTEGER DEFAULT 0`); err != nil && !isDupColumn(err) {
		return err
	}
	return nil
}
