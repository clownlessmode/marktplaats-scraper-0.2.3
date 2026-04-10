package adminbot

import (
	"encoding/csv"
	"strings"
)

// ParseEmailLine одна строка email:password / email;password / tab.
func ParseEmailLine(line string) (email, password string, ok bool) {
	line = strings.TrimSpace(line)
	if line == "" || strings.HasPrefix(line, "#") {
		return "", "", false
	}
	for _, sep := range []string{":", ";", "\t"} {
		if strings.Contains(line, sep) {
			parts := strings.SplitN(line, sep, 2)
			if len(parts) >= 2 && strings.Contains(parts[0], "@") {
				return strings.TrimSpace(strings.ToLower(parts[0])), strings.TrimSpace(parts[1]), true
			}
		}
	}
	if strings.Contains(line, "@") {
		return strings.ToLower(strings.TrimSpace(line)), "", true
	}
	return "", "", false
}

// ParseEmailsText многострочный ввод.
func ParseEmailsText(text string) [][2]string {
	var out [][2]string
	for _, line := range strings.Split(text, "\n") {
		e, p, ok := ParseEmailLine(line)
		if ok {
			out = append(out, [2]string{e, p})
		}
	}
	return out
}

// ParseEmailsCSV содержимое CSV (колонки email/почта/mail и password/пароль).
func ParseEmailsCSV(content string) [][2]string {
	r := csv.NewReader(strings.NewReader(content))
	r.TrimLeadingSpace = true
	rows, err := r.ReadAll()
	if err != nil || len(rows) == 0 {
		return nil
	}
	header := rows[0]
	if len(header) == 1 && strings.Contains(header[0], ";") {
		r = csv.NewReader(strings.NewReader(content))
		r.Comma = ';'
		rows, err = r.ReadAll()
		if err != nil || len(rows) == 0 {
			return nil
		}
		header = rows[0]
	}
	low := make([]string, len(header))
	for i, h := range header {
		low[i] = strings.ToLower(strings.TrimSpace(h))
	}
	emailCol, passCol := -1, -1
	for i, h := range low {
		switch h {
		case "email", "почта", "mail", "логин", "login", "username":
			emailCol = i
		case "password", "пароль", "pass", "pwd", "apppassword":
			passCol = i
		}
	}
	if emailCol < 0 {
		for i, h := range low {
			if strings.Contains(h, "@") {
				emailCol = i
				break
			}
		}
	}
	if emailCol < 0 {
		return nil
	}
	if passCol < 0 {
		passCol = emailCol + 1
		if passCol >= len(low) {
			passCol = emailCol
		}
	}
	var out [][2]string
	for _, row := range rows[1:] {
		if len(row) <= emailCol {
			continue
		}
		email := strings.TrimSpace(strings.ToLower(row[emailCol]))
		pass := ""
		if passCol < len(row) {
			pass = strings.TrimSpace(row[passCol])
		}
		if email != "" && strings.Contains(email, "@") {
			out = append(out, [2]string{email, pass})
		}
	}
	return out
}
