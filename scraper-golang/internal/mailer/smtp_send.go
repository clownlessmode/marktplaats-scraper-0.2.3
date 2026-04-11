package mailer

import (
	"database/sql"
	"fmt"
	"mime"
	"net/smtp"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/marktplaats-scraper/scraper-golang/internal/listingsdb"
	"github.com/marktplaats-scraper/scraper-golang/internal/marktplaats"
	"github.com/marktplaats-scraper/scraper-golang/internal/prettylog"
)

// SendResult исход отправки одного письма.
type SendResult struct {
	OK         bool
	Recipient  string
	NotExists  bool
	SkipReason string
}

func truncateRunes(s string, max int) string {
	if utf8.RuneCountInString(s) <= max {
		return s
	}
	return string([]rune(s)[:max])
}

func categoryLine(l marktplaats.Listing) string {
	if len(l.Verticals) == 0 {
		return ""
	}
	return strings.Join(l.Verticals, ", ")
}

func templateVars(l marktplaats.Listing, senderEmail string) map[string]string {
	userName := "User"
	if i := strings.Index(senderEmail, "@"); i > 0 {
		userName = senderEmail[:i]
	}
	desc := l.Description
	r := []rune(desc)
	if len(r) > 500 {
		desc = string(r[:500])
	}
	return map[string]string{
		"url":         l.ListingURL,
		"title":       l.Title,
		"price":       fmt.Sprintf("€%.2f", float64(l.PriceCents)/100),
		"price_cents": fmt.Sprintf("%d", l.PriceCents),
		"seller_name": l.SellerName,
		"city":        l.CityName,
		"category":    categoryLine(l),
		"description": desc,
		"user_name":   userName,
		"item_id":     l.ItemID,
	}
}

func encodeSubjectLine(s string) string {
	s = strings.TrimSpace(strings.ReplaceAll(strings.ReplaceAll(s, "\r\n", " "), "\n", " "))
	raw := truncateRunes(s, 200)
	return mime.QEncoding.Encode("utf-8", raw)
}

func buildSubject(l marktplaats.Listing, subjectTpl string, vars map[string]string) string {
	subjectTpl = strings.TrimSpace(subjectTpl)
	if subjectTpl == "" || subjectTpl == "-" {
		return truncateRunes(strings.TrimSpace(l.Title), 200)
	}
	s := listingsdb.FormatTemplate(subjectTpl, vars)
	s = strings.TrimSpace(strings.ReplaceAll(strings.ReplaceAll(s, "\r\n", " "), "\n", " "))
	if s == "" {
		return truncateRunes(strings.TrimSpace(l.Title), 200)
	}
	return truncateRunes(s, 200)
}

func encodePhrase(s string) string {
	return mime.QEncoding.Encode("utf-8", s)
}

// SendSellerEmail одно письмо (как send_seller_email в email_sender.py).
func SendSellerEmail(db *sql.DB, l marktplaats.Listing, senderEmail, senderPassword string, userID int64, skipRCPTVerify bool) SendResult {
	senderEmail = strings.TrimSpace(strings.ToLower(senderEmail))
	recipientReal := BuildSellerRecipient(l.SellerName)
	recipient := recipientReal
	if DevEnvironment() {
		recipient = testRecipient()
	}

	if !skipRCPTVerify && !DevEnvironment() {
		if RecipientCheckExists(recipientReal) == "NOT_EXISTS" {
			return SendResult{NotExists: true}
		}
	}

	tplID, ok := listingsdb.NextTemplateForListing(db, userID)
	if !ok {
		return SendResult{SkipReason: "нет шаблонов писем (добавьте хотя бы один в боте)"}
	}
	_, subjectTpl, bodyTpl, err := listingsdb.Template(db, tplID, userID)
	if err != nil {
		return SendResult{SkipReason: fmt.Sprintf("шаблон: %v", err)}
	}
	if strings.TrimSpace(bodyTpl) == "" {
		return SendResult{SkipReason: "тело шаблона пусто"}
	}

	vars := templateVars(l, senderEmail)
	body := listingsdb.FormatTemplate(bodyTpl, vars)
	subjRaw := buildSubject(l, subjectTpl, vars)
	userName := vars["user_name"]
	fromLine := fmt.Sprintf("%s <%s>", encodePhrase(userName), senderEmail)

	var msg strings.Builder
	msg.WriteString("From: ")
	msg.WriteString(fromLine)
	msg.WriteString("\r\nTo: ")
	msg.WriteString(recipient)
	msg.WriteString("\r\nSubject: ")
	msg.WriteString(encodeSubjectLine(subjRaw))
	msg.WriteString("\r\nMIME-Version: 1.0\r\nContent-Type: text/plain; charset=UTF-8\r\nContent-Transfer-Encoding: 8bit\r\n\r\n")
	msg.WriteString(body)

	addr := "smtp.gmail.com:587"
	auth := smtp.PlainAuth("", senderEmail, senderPassword, "smtp.gmail.com")
	err = smtp.SendMail(addr, auth, senderEmail, []string{recipient}, []byte(msg.String()))
	if err != nil {
		s := err.Error()
		low := strings.ToLower(s)
		if strings.Contains(low, "535") || strings.Contains(low, "authentication") ||
			(strings.Contains(low, "auth") && strings.Contains(low, "failed")) {
			_ = listingsdb.MarkEmailBlocked(db, userID, senderEmail)
			NotifyAdminBlocked(senderEmail, s)
			return SendResult{SkipReason: "SMTP авторизация: " + s}
		}
		if isNetworkLikeError(err) {
			return SendResult{SkipReason: "сеть (почта не блокировалась): " + s}
		}
		_ = listingsdb.MarkEmailBlocked(db, userID, senderEmail)
		NotifyAdminBlocked(senderEmail, s)
		return SendResult{SkipReason: "SMTP: " + s}
	}

	_ = listingsdb.SetLastUsedEmail(db, userID, senderEmail)
	_ = listingsdb.SetLastEmailForListing(db, userID, senderEmail)
	_ = listingsdb.SetLastTemplateForListing(db, userID, tplID)
	return SendResult{OK: true, Recipient: recipient}
}

// TrySendListingEmail round-robin почта + отправка (как try_send_listing_email).
func TrySendListingEmail(db *sql.DB, l marktplaats.Listing, userID int64, skipRCPTVerify bool) SendResult {
	email, pass, err := listingsdb.NextEmailForListing(db, userID)
	if err != nil || email == "" {
		return SendResult{SkipReason: "нет активных почт у воркера в БД"}
	}
	return SendSellerEmail(db, l, email, pass, userID, skipRCPTVerify)
}

// BulkSendStats счётчики пачки.
type BulkSendStats struct {
	OK, Fail, NotExists int
}

// BulkSendListings рассылка по списку (как send_bulk_listing_emails).
func BulkSendListings(db *sql.DB, userID int64, listings []marktplaats.Listing, delay time.Duration, skipRCPTVerify bool) BulkSendStats {
	var st BulkSendStats
	if len(listings) == 0 {
		return st
	}
	nMail, _ := listingsdb.ActiveEmailsCount(db, userID)
	nTpl, _ := listingsdb.EmailTemplatesCount(db, userID)
	if nTpl == 0 || nMail == 0 {
		st.Fail = len(listings)
		if nMail == 0 {
			prettylog.Warn("рассылка: нет активных почт у воркера в таблице emails", "")
		} else {
			prettylog.Warn("рассылка: нет шаблонов писем (добавьте шаблоны в боте)", "")
		}
		return st
	}
	var loggedErr bool
	for i, item := range listings {
		res := TrySendListingEmail(db, item, userID, skipRCPTVerify)
		switch {
		case res.OK:
			st.OK++
		case res.NotExists:
			st.NotExists++
		default:
			st.Fail++
			if res.SkipReason != "" && !loggedErr {
				prettylog.Warnf("пример ошибки отправки: %s", res.SkipReason)
				loggedErr = true
			}
		}
		if delay > 0 && i < len(listings)-1 {
			time.Sleep(delay)
		}
	}
	return st
}
