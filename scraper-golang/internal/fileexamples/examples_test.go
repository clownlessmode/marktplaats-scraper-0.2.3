package fileexamples

import (
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/marktplaats-scraper/scraper-golang/internal/adminbot"
	"github.com/marktplaats-scraper/scraper-golang/internal/clientbot"
)

// scraperGolangRoot = …/scraper-golang (родитель internal/fileexamples).
func scraperGolangRoot() string {
	_, f, _, _ := runtime.Caller(0)
	return filepath.Clean(filepath.Join(filepath.Dir(f), "..", ".."))
}

func readExample(t *testing.T, name string) string {
	t.Helper()
	p := filepath.Join(scraperGolangRoot(), "files_examples", name)
	b, err := os.ReadFile(p)
	if err != nil {
		t.Fatalf("read %s: %v", p, err)
	}
	return string(b)
}

func TestExampleListingsMarktplaatsV1(t *testing.T) {
	raw := readExample(t, "listings_marktplaats_export_v1.csv")
	got := clientbot.ParseListingsCSV(raw)
	if len(got) < 20 {
		t.Fatalf("expected many rows, got %d", len(got))
	}
	first := got[0]
	if !strings.Contains(first.ListingURL, "marktplaats.nl") {
		t.Fatalf("url: %s", first.ListingURL)
	}
	if first.SellerName == "" {
		t.Fatal("empty seller")
	}
	if !strings.Contains(first.Title, "Feestelijk") && first.Title == "" {
		t.Fatalf("title: %q", first.Title)
	}
	if first.ItemID != "m2369188486" {
		t.Fatalf("item_id %q", first.ItemID)
	}
	if first.PriceCents != 6000 {
		t.Fatalf("price cents %d", first.PriceCents)
	}
}

func TestExampleListingsMarktplaatsV2(t *testing.T) {
	raw := readExample(t, "listings_marktplaats_export_v2.csv")
	got := clientbot.ParseListingsCSV(raw)
	if len(got) < 25 {
		t.Fatalf("got %d rows", len(got))
	}
	u := got[0].ListingURL
	if !strings.Contains(u, "marktplaats") {
		t.Fatalf("url %s", u)
	}
	if got[0].SellerName == "" {
		t.Fatal("seller")
	}
}

func TestExampleListingsMarktplaatsV3(t *testing.T) {
	raw := readExample(t, "listings_marktplaats_export_v3.csv")
	got := clientbot.ParseListingsCSV(raw)
	if len(got) < 25 {
		t.Fatalf("got %d rows", len(got))
	}
	if !strings.Contains(got[0].ListingURL, "marktplaats.nl") {
		t.Fatal(got[0].ListingURL)
	}
}

func TestExampleListingsMarktplaatsRU(t *testing.T) {
	raw := readExample(t, "listings_marktplaats_export_ru.csv")
	got := clientbot.ParseListingsCSV(raw)
	if len(got) < 25 {
		t.Fatalf("got %d rows", len(got))
	}
	first := got[0]
	if !strings.Contains(first.ListingURL, "marktplaats.nl") {
		t.Fatal(first.ListingURL)
	}
	if !strings.Contains(first.Title, "Nintendo") {
		t.Fatalf("first title %q", first.Title)
	}
	if first.SellerName != "Kei Trekker" {
		t.Fatalf("seller %q", first.SellerName)
	}
}

func TestExampleCsvFilePoshmark(t *testing.T) {
	raw := readExample(t, "csv_file.csv")
	got := clientbot.ParseListingsCSV(raw)
	if len(got) < 15 {
		t.Fatalf("got %d rows", len(got))
	}
	first := got[0]
	if !strings.Contains(first.ListingURL, "poshmark.com/listing/") {
		t.Fatalf("url %s", first.ListingURL)
	}
	if first.SellerName != "kiehnabagail" {
		t.Fatalf("никнейм как seller: %q", first.SellerName)
	}
	if !strings.Contains(first.Title, "AirPods Pro") {
		t.Fatalf("title %q", first.Title)
	}
	if first.ItemID != "p69d767f15919e0cb8669d03b" {
		t.Fatalf("item_id %q", first.ItemID)
	}
	if first.PriceCents != 9300 {
		t.Fatalf("price cents %d", first.PriceCents)
	}
	if !strings.Contains(first.Description, "noise cancellation") {
		t.Fatalf("description missing expected text")
	}
}

func TestExampleListings2dehands(t *testing.T) {
	raw := readExample(t, "listings_2dehands_export.csv")
	got := clientbot.ParseListingsCSV(raw)
	if len(got) < 30 {
		t.Fatalf("got %d rows", len(got))
	}
	u := got[0].ListingURL
	if !strings.Contains(u, "2dehands") {
		t.Fatalf("url %s", u)
	}
	if got[0].SellerName == "" {
		t.Fatal("seller")
	}
	if got[0].ItemID != "m2364851996" {
		t.Fatalf("item_id %q", got[0].ItemID)
	}
}

func TestExampleEmailsGmailSample(t *testing.T) {
	raw := readExample(t, "emails_gmail_sample.csv")
	got := adminbot.ParseEmailsCSV(raw)
	if len(got) != 2 {
		t.Fatalf("got %#v", got)
	}
	if got[0][0] != "worker.demo@gmail.com" || got[0][1] != "demo-app-password-16chars" {
		t.Fatalf("row0 %#v", got[0])
	}
	if got[1][0] != "second.account@gmail.com" {
		t.Fatalf("row1 %#v", got[1])
	}
}
