package listingsdb

import (
	"database/sql"
	"fmt"
	"strconv"
	"strings"
)

// FormatTemplate подставляет {key} как в Python format_template.
func FormatTemplate(body string, vars map[string]string) string {
	out := body
	for k, v := range vars {
		out = strings.ReplaceAll(out, "{"+k+"}", v)
	}
	return out
}

// ActiveEmails возвращает активные почты воркера, ORDER BY email.
func ActiveEmails(db *sql.DB, userID int64) ([]struct{ Email, Password string }, error) {
	rows, err := db.Query(
		`SELECT email, COALESCE(password,'') FROM emails WHERE user_id = ? AND COALESCE(blocked,0) = 0 ORDER BY email`,
		userID,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var list []struct{ Email, Password string }
	for rows.Next() {
		var e, p string
		if err := rows.Scan(&e, &p); err != nil {
			return nil, err
		}
		list = append(list, struct{ Email, Password string }{strings.TrimSpace(strings.ToLower(e)), p})
	}
	return list, rows.Err()
}

// NextEmailForListing round-robin по активным почтам (как get_next_email_for_listing).
func NextEmailForListing(db *sql.DB, userID int64) (email, password string, err error) {
	list, err := ActiveEmails(db, userID)
	if err != nil || len(list) == 0 {
		return "", "", err
	}
	key := fmt.Sprintf("last_email_for_listing_%d", userID)
	var last string
	_ = db.QueryRow(`SELECT value FROM rotation_state WHERE key = ?`, key).Scan(&last)
	last = strings.TrimSpace(strings.ToLower(last))
	nextIdx := 0
	for i, pair := range list {
		if pair.Email == last {
			nextIdx = (i + 1) % len(list)
			break
		}
	}
	return list[nextIdx].Email, list[nextIdx].Password, nil
}

// SetLastEmailForListing записывает последнюю почту для round-robin.
func SetLastEmailForListing(db *sql.DB, userID int64, email string) error {
	key := fmt.Sprintf("last_email_for_listing_%d", userID)
	_, err := db.Exec(`INSERT OR REPLACE INTO rotation_state (key, value) VALUES (?, ?)`, key, strings.TrimSpace(strings.ToLower(email)))
	return err
}

// ActiveTemplateID ID активного шаблона воркера из rotation_state (устарело: рассылка крутит шаблоны по кругу).
func ActiveTemplateID(db *sql.DB, userID int64) (int64, bool) {
	key := fmt.Sprintf("active_template_id_%d", userID)
	var v sql.NullString
	if err := db.QueryRow(`SELECT value FROM rotation_state WHERE key = ?`, key).Scan(&v); err != nil || !v.Valid || v.String == "" {
		return 0, false
	}
	id, err := strconv.ParseInt(strings.TrimSpace(v.String), 10, 64)
	if err != nil {
		return 0, false
	}
	return id, true
}

// OrderedTemplateIDs id шаблонов воркера по возрастанию (порядок round-robin).
func OrderedTemplateIDs(db *sql.DB, userID int64) ([]int64, error) {
	rows, err := db.Query(`SELECT id FROM email_templates WHERE user_id = ? ORDER BY id`, userID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var ids []int64
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	return ids, rows.Err()
}

// NextTemplateForListing — следующий шаблон по кругу для рассылки (как NextEmailForListing).
func NextTemplateForListing(db *sql.DB, userID int64) (templateID int64, ok bool) {
	ids, err := OrderedTemplateIDs(db, userID)
	if err != nil || len(ids) == 0 {
		return 0, false
	}
	key := fmt.Sprintf("last_template_for_listing_%d", userID)
	var lastStr sql.NullString
	_ = db.QueryRow(`SELECT value FROM rotation_state WHERE key = ?`, key).Scan(&lastStr)
	lastID, _ := strconv.ParseInt(strings.TrimSpace(lastStr.String), 10, 64)
	nextIdx := 0
	if lastID > 0 {
		for i, id := range ids {
			if id == lastID {
				nextIdx = (i + 1) % len(ids)
				break
			}
		}
	}
	return ids[nextIdx], true
}

// SetLastTemplateForListing записывает последний использованный шаблон для round-robin.
func SetLastTemplateForListing(db *sql.DB, userID, templateID int64) error {
	key := fmt.Sprintf("last_template_for_listing_%d", userID)
	_, err := db.Exec(`INSERT OR REPLACE INTO rotation_state (key, value) VALUES (?, ?)`, key, strconv.FormatInt(templateID, 10))
	return err
}

// Template возвращает (name, subject_template, body) шаблона воркера.
func Template(db *sql.DB, templateID, userID int64) (name, subject, body string, err error) {
	err = db.QueryRow(
		`SELECT name, COALESCE(subject_template,''), body FROM email_templates WHERE id = ? AND user_id = ?`,
		templateID, userID,
	).Scan(&name, &subject, &body)
	if err == sql.ErrNoRows {
		return "", "", "", nil
	}
	return name, subject, body, err
}

// EmailTemplatesCount число шаблонов воркера.
func EmailTemplatesCount(db *sql.DB, userID int64) (int, error) {
	var n int
	err := db.QueryRow(`SELECT COUNT(*) FROM email_templates WHERE user_id = ?`, userID).Scan(&n)
	return n, err
}

// SetLastUsedEmail rotation_state last_used_email_{userID}.
func SetLastUsedEmail(db *sql.DB, userID int64, email string) error {
	key := fmt.Sprintf("last_used_email_%d", userID)
	_, err := db.Exec(`INSERT OR REPLACE INTO rotation_state (key, value) VALUES (?, ?)`, key, strings.TrimSpace(strings.ToLower(email)))
	return err
}

// MarkEmailBlocked blocked=1 для пары user_id + email.
func MarkEmailBlocked(db *sql.DB, userID int64, email string) error {
	_, err := db.Exec(`UPDATE emails SET blocked = 1 WHERE email = ? AND user_id = ?`, strings.TrimSpace(strings.ToLower(email)), userID)
	return err
}

// ActiveEmailsCount число незаблокированных почт воркера.
func ActiveEmailsCount(db *sql.DB, userID int64) (int, error) {
	var n int
	err := db.QueryRow(`SELECT COUNT(*) FROM emails WHERE user_id = ? AND COALESCE(blocked,0) = 0`, userID).Scan(&n)
	return n, err
}
