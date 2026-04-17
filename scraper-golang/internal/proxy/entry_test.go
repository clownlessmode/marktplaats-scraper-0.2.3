package proxy

import "testing"

func TestParseLineHTTPSPort443(t *testing.T) {
	e, err := ParseLine("https://user:secret@190.2.137.56:443")
	if err != nil {
		t.Fatal(err)
	}
	if e.Scheme != "https" {
		t.Fatalf("Scheme: got %q, want https", e.Scheme)
	}
	if e.Server != "https://190.2.137.56:443" {
		t.Fatalf("Server: got %q", e.Server)
	}
	if e.Username != "user" || e.Password != "secret" {
		t.Fatalf("credentials")
	}
}

func TestParseLineHTTPPort443(t *testing.T) {
	e, err := ParseLine("http://user:secret@190.2.137.56:443")
	if err != nil {
		t.Fatal(err)
	}
	if e.Scheme != "http" {
		t.Fatalf("Scheme: got %q, want http", e.Scheme)
	}
	if e.Server != "http://190.2.137.56:443" {
		t.Fatalf("Server: got %q", e.Server)
	}
}

func TestParseLineHTTPSDefaultPort(t *testing.T) {
	e, err := ParseLine("https://u:p@proxy.example.com")
	if err != nil {
		t.Fatal(err)
	}
	if e.Server != "https://proxy.example.com:443" {
		t.Fatalf("Server: got %q, want default :443", e.Server)
	}
}
