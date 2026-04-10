package proxy

import (
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"time"

	"golang.org/x/net/proxy"
)

// Check verifies that the proxy can reach probeURL (GET, status < 500, TLS OK for HTTPS).
func Check(ctx context.Context, e Entry, probeURL string) error {
	probe, err := url.Parse(probeURL)
	if err != nil {
		return err
	}
	if probe.Scheme != "http" && probe.Scheme != "https" {
		return fmt.Errorf("probe URL должен быть http или https")
	}

	switch e.Scheme {
	case "socks5", "socks5h":
		return checkSOCKS(ctx, e, probe)
	default:
		return checkHTTPProxy(ctx, e, probe)
	}
}

func probeTCPAddr(u *url.URL) string {
	host := u.Hostname()
	port := u.Port()
	if port == "" {
		if u.Scheme == "https" {
			port = "443"
		} else {
			port = "80"
		}
	}
	return net.JoinHostPort(host, port)
}

func checkHTTPProxy(ctx context.Context, e Entry, probe *url.URL) error {
	pu, err := url.Parse(e.Server)
	if err != nil {
		return err
	}
	if e.Username != "" {
		pu.User = url.UserPassword(e.Username, e.Password)
	}
	tr := &http.Transport{
		Proxy:           http.ProxyURL(pu),
		TLSClientConfig: &tls.Config{MinVersion: tls.VersionTLS12},
	}
	defer tr.CloseIdleConnections()
	client := &http.Client{
		Transport: tr,
		Timeout:   25 * time.Second,
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, probe.String(), nil)
	if err != nil {
		return err
	}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 64*1024))
	if resp.StatusCode >= 500 {
		return fmt.Errorf("HTTP-код ответа %d", resp.StatusCode)
	}
	return nil
}

func checkSOCKS(ctx context.Context, e Entry, probe *url.URL) error {
	su, err := url.Parse(e.Server)
	if err != nil {
		return err
	}
	// FromURL needs userinfo on the URL for SOCKS auth.
	if e.Username != "" {
		su.User = url.UserPassword(e.Username, e.Password)
	}
	dialer, err := proxy.FromURL(su, proxy.Direct)
	if err != nil {
		return err
	}
	cd, ok := dialer.(proxy.ContextDialer)
	if !ok {
		return fmt.Errorf("SOCKS-диалер без поддержки context")
	}
	addr := probeTCPAddr(probe)
	conn, err := cd.DialContext(ctx, "tcp", addr)
	if err != nil {
		return err
	}
	defer conn.Close()

	if probe.Scheme == "https" {
		tlsConn := tls.Client(conn, &tls.Config{
			ServerName:         probe.Hostname(),
			MinVersion:         tls.VersionTLS12,
			InsecureSkipVerify: false,
		})
		defer tlsConn.Close()
		if err := tlsConn.HandshakeContext(ctx); err != nil {
			return err
		}
		return nil
	}

	deadline, ok := ctx.Deadline()
	if ok {
		_ = conn.SetDeadline(deadline)
	}
	req := fmt.Sprintf("GET %s HTTP/1.1\r\nHost: %s\r\nConnection: close\r\n\r\n", probe.RequestURI(), probe.Hostname())
	if _, err := io.WriteString(conn, req); err != nil {
		return err
	}
	buf := make([]byte, 16)
	if _, err := conn.Read(buf); err != nil && err != io.EOF {
		return err
	}
	return nil
}
