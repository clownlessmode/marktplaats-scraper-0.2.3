package listingsdb

import (
	"database/sql"
	"fmt"
	"strconv"
	"strings"
	"time"
)

// --- Пользователи / модерация (админ-бот) ---

type PendingUser struct {
	UserID    int64
	CreatedAt string
}

// PendingUsers ожидают подтверждения (authorized=0, не в blocked).
func PendingUsers(db *sql.DB) ([]PendingUser, error) {
	rows, err := db.Query(`
		SELECT user_id, COALESCE(created_at,'') FROM users
		WHERE authorized = 0 AND user_id NOT IN (SELECT user_id FROM blocked_users)
		ORDER BY created_at DESC`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []PendingUser
	for rows.Next() {
		var u PendingUser
		if err := rows.Scan(&u.UserID, &u.CreatedAt); err != nil {
			return nil, err
		}
		out = append(out, u)
	}
	return out, rows.Err()
}

func nowISO() string {
	return time.Now().UTC().Format(time.RFC3339Nano)
}

// AuthorizeUser одобрить воркера.
func AuthorizeUser(db *sql.DB, userID int64) error {
	_, err := db.Exec(`
		INSERT OR REPLACE INTO users (user_id, authorized, created_at, shift_active) VALUES (?, 1, ?, 0)`,
		userID, nowISO())
	return err
}

// BlockUser в blocked_users + authorized=0.
func BlockUser(db *sql.DB, userID int64) error {
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	if _, err := tx.Exec(`INSERT OR REPLACE INTO blocked_users (user_id, blocked_at) VALUES (?, ?)`, userID, nowISO()); err != nil {
		return err
	}
	if _, err := tx.Exec(`UPDATE users SET authorized = 0 WHERE user_id = ?`, userID); err != nil {
		return err
	}
	return tx.Commit()
}

// UnblockUser снять блок (пользователь снова может подать заявку).
func UnblockUser(db *sql.DB, userID int64) error {
	_, err := db.Exec(`DELETE FROM blocked_users WHERE user_id = ?`, userID)
	return err
}

// DeleteUser удаляет воркера из users, blocked, worker_listings.
func DeleteUser(db *sql.DB, userID int64) (deleted bool, err error) {
	tx, err := db.Begin()
	if err != nil {
		return false, err
	}
	defer func() { _ = tx.Rollback() }()
	if _, err := tx.Exec(`DELETE FROM blocked_users WHERE user_id = ?`, userID); err != nil {
		return false, err
	}
	if _, err := tx.Exec(`DELETE FROM worker_listings WHERE user_id = ?`, userID); err != nil {
		return false, err
	}
	res, err := tx.Exec(`DELETE FROM users WHERE user_id = ?`, userID)
	if err != nil {
		return false, err
	}
	n, _ := res.RowsAffected()
	if err := tx.Commit(); err != nil {
		return false, err
	}
	return n > 0, nil
}

// WorkerRow авторизованный воркер (без blocked).
type WorkerRow struct {
	UserID      int64
	CreatedAt   string
	ShiftActive bool
}

// AllWorkers (user_id, created_at, shift).
func AllWorkers(db *sql.DB) ([]WorkerRow, error) {
	rows, err := db.Query(`
		SELECT user_id, COALESCE(created_at,''), COALESCE(shift_active,0) FROM users
		WHERE authorized = 1 AND user_id NOT IN (SELECT user_id FROM blocked_users)
		ORDER BY created_at DESC`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []WorkerRow
	for rows.Next() {
		var w WorkerRow
		var shift int
		if err := rows.Scan(&w.UserID, &w.CreatedAt, &shift); err != nil {
			return nil, err
		}
		w.ShiftActive = shift != 0
		out = append(out, w)
	}
	return out, rows.Err()
}

// WorkerStat как в Python get_workers_with_stats.
type WorkerStat struct {
	UserID         int64
	CreatedAt      string
	ShiftActive    bool
	ListingsToday  int
	LastListingAt  string
}

// WorkersWithStats список воркеров с подсчётом объявлений за сегодня (UTC, SQLite date('now')).
func WorkersWithStats(db *sql.DB) ([]WorkerStat, error) {
	ws, err := AllWorkers(db)
	if err != nil {
		return nil, err
	}
	today := map[int64]int{}
	lastAt := map[int64]string{}
	rows, err := db.Query(`
		SELECT user_id, COUNT(*) FROM worker_listings
		WHERE date(received_at) = date('now') GROUP BY user_id`)
	if err == nil {
		for rows.Next() {
			var uid int64
			var c int
			if err := rows.Scan(&uid, &c); err != nil {
				rows.Close()
				return nil, err
			}
			today[uid] = c
		}
		rows.Close()
	}
	rows2, err := db.Query(`SELECT user_id, MAX(received_at) FROM worker_listings GROUP BY user_id`)
	if err == nil {
		for rows2.Next() {
			var uid int64
			var ts sql.NullString
			if err := rows2.Scan(&uid, &ts); err != nil {
				rows2.Close()
				return nil, err
			}
			if ts.Valid && ts.String != "" {
				s := ts.String
				if len(s) > 16 {
					s = s[:16]
				}
				lastAt[uid] = strings.ReplaceAll(s, "T", " ")
			}
		}
		rows2.Close()
	}
	out := make([]WorkerStat, 0, len(ws))
	for _, w := range ws {
		la := lastAt[w.UserID]
		if la == "" {
			la = "—"
		}
		out = append(out, WorkerStat{
			UserID:        w.UserID,
			CreatedAt:     w.CreatedAt,
			ShiftActive:   w.ShiftActive,
			ListingsToday: today[w.UserID],
			LastListingAt: la,
		})
	}
	return out, nil
}

// BlockedUser запись blocked_users.
type BlockedUser struct {
	UserID    int64
	BlockedAt string
}

func BlockedUsersList(db *sql.DB) ([]BlockedUser, error) {
	rows, err := db.Query(`SELECT user_id, COALESCE(blocked_at,'') FROM blocked_users ORDER BY blocked_at DESC`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []BlockedUser
	for rows.Next() {
		var b BlockedUser
		if err := rows.Scan(&b.UserID, &b.BlockedAt); err != nil {
			return nil, err
		}
		out = append(out, b)
	}
	return out, rows.Err()
}

// --- Почты (таблица emails) ---

type EmailAccount struct {
	Email     string
	Password  string
	CreatedAt string
	Blocked   bool
}

// AddEmailsBatch вставка пачки; только строки с @ в email.
func AddEmailsBatch(db *sql.DB, ownerUserID int64, pairs [][2]string) (added, skipped int) {
	for _, p := range pairs {
		email := strings.TrimSpace(strings.ToLower(p[0]))
		pass := strings.TrimSpace(p[1])
		if email == "" || !strings.Contains(email, "@") {
			continue
		}
		_, err := db.Exec(
			`INSERT INTO emails (user_id, email, password, created_at, blocked) VALUES (?, ?, ?, ?, 0)`,
			ownerUserID, email, pass, nowISO())
		if err != nil {
			if strings.Contains(strings.ToLower(err.Error()), "unique") {
				skipped++
			}
			continue
		}
		added++
	}
	return added, skipped
}

// ListEmails страница почт воркера.
func ListEmails(db *sql.DB, ownerUserID int64, limit, offset int) ([]EmailAccount, error) {
	rows, err := db.Query(`
		SELECT email, COALESCE(password,''), COALESCE(created_at,''), COALESCE(blocked,0)
		FROM emails WHERE user_id = ? ORDER BY created_at DESC LIMIT ? OFFSET ?`,
		ownerUserID, limit, offset)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []EmailAccount
	for rows.Next() {
		var e EmailAccount
		var bl int
		if err := rows.Scan(&e.Email, &e.Password, &e.CreatedAt, &bl); err != nil {
			return nil, err
		}
		e.Blocked = bl != 0
		out = append(out, e)
	}
	return out, rows.Err()
}

// EmailsTotalCount все почты воркера (включая blocked).
func EmailsTotalCount(db *sql.DB, ownerUserID int64) (int, error) {
	var n int
	err := db.QueryRow(`SELECT COUNT(*) FROM emails WHERE user_id = ?`, ownerUserID).Scan(&n)
	return n, err
}

// DeleteEmail удаляет строку; true если была удалена.
func DeleteEmail(db *sql.DB, ownerUserID int64, email string) (bool, error) {
	email = strings.TrimSpace(strings.ToLower(email))
	res, err := db.Exec(`DELETE FROM emails WHERE email = ? AND user_id = ?`, email, ownerUserID)
	if err != nil {
		return false, err
	}
	n, _ := res.RowsAffected()
	return n > 0, nil
}

// UnblockEmail снять blocked.
func UnblockEmail(db *sql.DB, ownerUserID int64, email string) (bool, error) {
	email = strings.TrimSpace(strings.ToLower(email))
	res, err := db.Exec(`UPDATE emails SET blocked = 0 WHERE email = ? AND user_id = ?`, email, ownerUserID)
	if err != nil {
		return false, err
	}
	n, _ := res.RowsAffected()
	return n > 0, nil
}

// LastUsedEmail из rotation_state.
func LastUsedEmail(db *sql.DB, ownerUserID int64) string {
	key := fmt.Sprintf("last_used_email_%d", ownerUserID)
	var v sql.NullString
	_ = db.QueryRow(`SELECT value FROM rotation_state WHERE key = ?`, key).Scan(&v)
	if v.Valid {
		return v.String
	}
	return ""
}

// RandomActiveEmail случайная незаблокированная почта воркера.
func RandomActiveEmail(db *sql.DB, ownerUserID int64) (email, password string, ok bool) {
	err := db.QueryRow(`
		SELECT email, COALESCE(password,'') FROM emails
		WHERE user_id = ? AND COALESCE(blocked,0) = 0 ORDER BY RANDOM() LIMIT 1`, ownerUserID).Scan(&email, &password)
	if err != nil {
		return "", "", false
	}
	return email, password, true
}

// AllEmailsForTest все почты воркера для прогона теста (включая blocked).
func AllEmailsForTest(db *sql.DB, ownerUserID int64) ([]struct{ Email, Password string }, error) {
	rows, err := db.Query(`SELECT email, COALESCE(password,'') FROM emails WHERE user_id = ? ORDER BY created_at DESC`, ownerUserID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []struct{ Email, Password string }
	for rows.Next() {
		var e, p string
		if err := rows.Scan(&e, &p); err != nil {
			return nil, err
		}
		out = append(out, struct{ Email, Password string }{e, p})
	}
	return out, rows.Err()
}

// --- Шаблоны ---

type TemplateRow struct {
	ID        int64
	Name      string
	Body      string
	CreatedAt string
}

func ListEmailTemplates(db *sql.DB, ownerUserID int64) ([]TemplateRow, error) {
	rows, err := db.Query(`
		SELECT id, name, body, COALESCE(created_at,'') FROM email_templates WHERE user_id = ? ORDER BY id`, ownerUserID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []TemplateRow
	for rows.Next() {
		var t TemplateRow
		if err := rows.Scan(&t.ID, &t.Name, &t.Body, &t.CreatedAt); err != nil {
			return nil, err
		}
		out = append(out, t)
	}
	return out, rows.Err()
}

// AddEmailTemplate вставка; возвращает id.
func AddEmailTemplate(db *sql.DB, ownerUserID int64, name, body string) (int64, error) {
	res, err := db.Exec(
		`INSERT INTO email_templates (name, body, created_at, user_id) VALUES (?, ?, ?, ?)`,
		strings.TrimSpace(name), strings.TrimSpace(body), nowISO(), ownerUserID)
	if err != nil {
		return 0, err
	}
	id, err := res.LastInsertId()
	return id, err
}

// GetEmailTemplate (name, body), ok.
func GetEmailTemplate(db *sql.DB, ownerUserID, templateID int64) (name, body string, ok bool) {
	err := db.QueryRow(
		`SELECT name, body FROM email_templates WHERE id = ? AND user_id = ?`, templateID, ownerUserID).Scan(&name, &body)
	if err != nil {
		return "", "", false
	}
	return name, body, true
}

// UpdateEmailTemplate обновить имя и тело.
func UpdateEmailTemplate(db *sql.DB, ownerUserID, templateID int64, name, body string) (bool, error) {
	res, err := db.Exec(
		`UPDATE email_templates SET name = ?, body = ? WHERE id = ? AND user_id = ?`,
		strings.TrimSpace(name), strings.TrimSpace(body), templateID, ownerUserID)
	if err != nil {
		return false, err
	}
	n, _ := res.RowsAffected()
	return n > 0, nil
}

// DeleteEmailTemplate удаление шаблона.
func DeleteEmailTemplate(db *sql.DB, ownerUserID, templateID int64) (bool, error) {
	res, err := db.Exec(`DELETE FROM email_templates WHERE id = ? AND user_id = ?`, templateID, ownerUserID)
	if err != nil {
		return false, err
	}
	n, _ := res.RowsAffected()
	return n > 0, nil
}

// ClearActiveTemplateIf удаляет active_template_id_{user} если он равен templateID.
func ClearActiveTemplateIf(db *sql.DB, ownerUserID, templateID int64) error {
	cur, ok := ActiveTemplateID(db, ownerUserID)
	if !ok || cur != templateID {
		return nil
	}
	key := fmt.Sprintf("active_template_id_%d", ownerUserID)
	_, err := db.Exec(`DELETE FROM rotation_state WHERE key = ?`, key)
	return err
}

// SetActiveTemplateID установить активный шаблон.
func SetActiveTemplateID(db *sql.DB, ownerUserID, templateID int64) error {
	key := fmt.Sprintf("active_template_id_%d", ownerUserID)
	_, err := db.Exec(`INSERT OR REPLACE INTO rotation_state (key, value) VALUES (?, ?)`, key, strconv.FormatInt(templateID, 10))
	return err
}

// ClearActiveTemplateID сброс активного шаблона.
func ClearActiveTemplateID(db *sql.DB, ownerUserID int64) error {
	key := fmt.Sprintf("active_template_id_%d", ownerUserID)
	_, err := db.Exec(`DELETE FROM rotation_state WHERE key = ?`, key)
	return err
}
