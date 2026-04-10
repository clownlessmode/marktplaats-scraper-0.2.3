package mailer

import (
	"fmt"
	"net"
	"strings"
	"time"
)

// BuildSellerRecipient {sanitized_seller_name}@gmail.com как в Python _build_seller_email.
func BuildSellerRecipient(sellerName string) string {
	var b strings.Builder
	for _, r := range strings.ToLower(sellerName) {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') {
			b.WriteRune(r)
			if b.Len() >= 64 {
				break
			}
		}
	}
	local := b.String()
	if local == "" {
		local = "seller"
	}
	return local + "@gmail.com"
}

// RecipientCheckExists SMTP RCPT на MX (как check_email_exists: EXISTS / NOT_EXISTS / UNKNOWN).
func RecipientCheckExists(emailAddr string) string {
	emailAddr = strings.TrimSpace(strings.ToLower(emailAddr))
	if emailAddr == "" || !strings.Contains(emailAddr, "@") {
		return "NOT_EXISTS"
	}
	domain := emailAddr[strings.LastIndex(emailAddr, "@")+1:]
	mxs, err := net.LookupMX(domain)
	if err != nil || len(mxs) == 0 {
		return "UNKNOWN"
	}
	best := mxs[0].Host
	if best == "" {
		return "UNKNOWN"
	}
	best = strings.TrimSuffix(best, ".")
	addr := net.JoinHostPort(best, "25")
	conn, err := net.DialTimeout("tcp", addr, 10*time.Second)
	if err != nil {
		return "UNKNOWN"
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(12 * time.Second))
	read := func() string {
		buf := make([]byte, 1024)
		n, _ := conn.Read(buf)
		return string(buf[:n])
	}
	write := func(s string) error {
		_, err := conn.Write([]byte(s))
		return err
	}
	_ = read()
	if write("EHLO marktplaats-scraper.local\r\n") != nil {
		return "UNKNOWN"
	}
	_ = read()
	if write("MAIL FROM:<verify@example.com>\r\n") != nil {
		return "UNKNOWN"
	}
	_ = read()
	if write(fmt.Sprintf("RCPT TO:<%s>\r\n", emailAddr)) != nil {
		return "UNKNOWN"
	}
	resp := read()
	if strings.Contains(resp, "250") || strings.Contains(resp, "251") {
		return "EXISTS"
	}
	if strings.Contains(resp, "550") || strings.Contains(resp, "551") || strings.Contains(resp, "553") || strings.Contains(resp, "554") {
		return "NOT_EXISTS"
	}
	return "UNKNOWN"
}
