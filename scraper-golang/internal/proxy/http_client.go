package proxy

import (
	"context"
	"crypto/tls"
	"net"
	"net/http"
	"net/url"
	"time"

	xproxy "golang.org/x/net/proxy"
)

// HTTPClient возвращает клиент с прямым доступом или через тот же прокси, что и Playwright (HTTP / SOCKS5).
// entry == nil — без прокси.
func HTTPClient(entry *Entry, timeout time.Duration) (*http.Client, error) {
	if timeout <= 0 {
		timeout = 90 * time.Second
	}
	if entry == nil || entry.Server == "" {
		return &http.Client{
			Timeout: timeout,
			Transport: &http.Transport{
				TLSClientConfig: &tls.Config{MinVersion: tls.VersionTLS12},
			},
		}, nil
	}

	tr := &http.Transport{
		TLSClientConfig: &tls.Config{MinVersion: tls.VersionTLS12},
	}

	switch entry.Scheme {
	case "socks5", "socks5h":
		u, err := url.Parse(entry.Server)
		if err != nil {
			return nil, err
		}
		if entry.Username != "" {
			u.User = url.UserPassword(entry.Username, entry.Password)
		}
		dialer, err := xproxy.FromURL(u, xproxy.Direct)
		if err != nil {
			return nil, err
		}
		if cd, ok := dialer.(xproxy.ContextDialer); ok {
			tr.DialContext = func(ctx context.Context, network, addr string) (net.Conn, error) {
				return cd.DialContext(ctx, network, addr)
			}
		} else {
			tr.DialContext = func(ctx context.Context, network, addr string) (net.Conn, error) {
				return dialer.Dial(network, addr)
			}
		}
	default:
		pu, err := url.Parse(entry.Server)
		if err != nil {
			return nil, err
		}
		if entry.Username != "" {
			pu.User = url.UserPassword(entry.Username, entry.Password)
		}
		tr.Proxy = http.ProxyURL(pu)
	}

	return &http.Client{Transport: tr, Timeout: timeout}, nil
}
