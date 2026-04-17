package proxy

import (
	"fmt"
	"net"
	"net/url"
	"strings"

	"github.com/playwright-community/playwright-go"
)

// Entry is a normalized proxy for Playwright and pre-flight checks.
type Entry struct {
	Scheme   string // socks5, socks5h, http, https (https = TLS to proxy, then CONNECT)
	Server   string // e.g. socks5://host:9999 or http://host:8080 (no userinfo)
	Username string
	Password string
}

// ParseLine parses one non-empty line from proxies.txt.
// Supported: socks5://user:pass@host:port, http(s)://..., host:port (treated as HTTP).
// https:// — «secure web proxy»: соединение с прокси по TLS (часто порт 443); http:// — обычный HTTP CONNECT.
func ParseLine(line string) (Entry, error) {
	line = strings.TrimSpace(line)
	if line == "" {
		return Entry{}, fmt.Errorf("пустая строка")
	}
	raw := line
	if !strings.Contains(raw, "://") {
		raw = "http://" + raw
	}
	u, err := url.Parse(raw)
	if err != nil {
		return Entry{}, err
	}
	scheme := strings.ToLower(u.Scheme)
	switch scheme {
	case "socks5", "socks5h":
	case "http", "https":
	default:
		return Entry{}, fmt.Errorf("неподдерживаемая схема %q (нужны socks5://, http:// или host:port)", u.Scheme)
	}
	if u.Host == "" {
		return Entry{}, fmt.Errorf("не указан хост")
	}
	hostport := u.Host
	if _, _, err := net.SplitHostPort(hostport); err != nil {
		switch scheme {
		case "socks5", "socks5h":
			hostport = net.JoinHostPort(hostport, "1080")
		case "https":
			hostport = net.JoinHostPort(hostport, "443")
		default:
			hostport = net.JoinHostPort(hostport, "80")
		}
	}
	user := ""
	pass := ""
	if u.User != nil {
		user = u.User.Username()
		pass, _ = u.User.Password()
	}
	server := fmt.Sprintf("%s://%s", scheme, hostport)
	return Entry{
		Scheme:   scheme,
		Server:   server,
		Username: user,
		Password: pass,
	}, nil
}

// Mask returns a safe string for logs (no password).
func (e Entry) Mask() string {
	auth := "без_логина"
	if e.Username != "" {
		auth = "есть_логин"
	}
	return fmt.Sprintf("%s (%s)", e.Server, auth)
}

// Playwright returns launch options proxy; nil if this entry is zero (unused).
func (e Entry) Playwright() *playwright.Proxy {
	p := &playwright.Proxy{Server: e.Server}
	if e.Username != "" {
		p.Username = playwright.String(e.Username)
		p.Password = playwright.String(e.Password)
	}
	return p
}
