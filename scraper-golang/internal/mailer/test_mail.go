package mailer

import (
	"database/sql"
	"fmt"
	"mime"
	"net/smtp"
	"strings"

	"github.com/marktplaats-scraper/scraper-golang/internal/listingsdb"
)

// SendTestEmail тестовое письмо на TestRecipient() (или to если не пусто). Как send_test_email в Python.
func SendTestEmail(db *sql.DB, senderEmail, senderPassword string, userID int64, to string) bool {
	senderEmail = strings.TrimSpace(strings.ToLower(senderEmail))
	to = strings.TrimSpace(to)
	if to == "" || !strings.Contains(to, "@") {
		to = TestRecipient()
	}
	userName := "User"
	if i := strings.Index(senderEmail, "@"); i > 0 {
		userName = senderEmail[:i]
	}
	subject := mime.QEncoding.Encode("utf-8", "Тест почты — Marktplaats Scraper")
	body := "Это тестовое письмо. Почта работает."
	fromLine := fmt.Sprintf("%s <%s>", mime.QEncoding.Encode("utf-8", userName), senderEmail)
	var msg strings.Builder
	msg.WriteString("From: ")
	msg.WriteString(fromLine)
	msg.WriteString("\r\nTo: ")
	msg.WriteString(to)
	msg.WriteString("\r\nSubject: ")
	msg.WriteString(subject)
	msg.WriteString("\r\nMIME-Version: 1.0\r\nContent-Type: text/plain; charset=UTF-8\r\nContent-Transfer-Encoding: 8bit\r\n\r\n")
	msg.WriteString(body)

	addr := "smtp.gmail.com:587"
	auth := smtp.PlainAuth("", senderEmail, senderPassword, "smtp.gmail.com")
	err := smtp.SendMail(addr, auth, senderEmail, []string{to}, []byte(msg.String()))
	if err != nil {
		s := err.Error()
		low := strings.ToLower(s)
		if strings.Contains(low, "535") || strings.Contains(low, "authentication") ||
			(strings.Contains(low, "auth") && strings.Contains(low, "failed")) {
			_ = listingsdb.MarkEmailBlocked(db, userID, senderEmail)
			NotifyAdminBlocked(senderEmail, s)
			return false
		}
		if isNetworkLikeError(err) {
			return false
		}
		_ = listingsdb.MarkEmailBlocked(db, userID, senderEmail)
		NotifyAdminBlocked(senderEmail, s)
		return false
	}
	_ = listingsdb.SetLastUsedEmail(db, userID, senderEmail)
	_, _ = listingsdb.UnblockEmail(db, userID, senderEmail)
	return true
}

// TestAllEmailsForAdmin проверяет все почты владельца adminUserID; возвращает счётчики и список неуспешных.
func TestAllEmailsForAdmin(db *sql.DB, adminUserID int64) (okN, failN int, failedEmails []string) {
	pairs, err := listingsdb.AllEmailsForTest(db, adminUserID)
	if err != nil {
		return 0, 0, nil
	}
	for _, p := range pairs {
		if SendTestEmail(db, p.Email, p.Password, adminUserID, "") {
			okN++
		} else {
			failN++
			failedEmails = append(failedEmails, p.Email)
		}
	}
	return okN, failN, failedEmails
}
