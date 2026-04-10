package clientbot

import (
	"testing"

	"github.com/marktplaats-scraper/scraper-golang/internal/marktplaats"
)

func TestParseListingsCSV(t *testing.T) {
	csv := "Ник Продавца,Ссылка на товар,Название\nJan,https://www.marktplaats.nl/v/phones/m123-iphone.html,iPhone\n"
	got := ParseListingsCSV(csv)
	if len(got) != 1 {
		t.Fatalf("got %d rows", len(got))
	}
	var want marktplaats.Listing
	want.SellerName = "Jan"
	want.ListingURL = "https://www.marktplaats.nl/v/phones/m123-iphone.html"
	want.Title = "iPhone"
	want.ItemID = "m123"
	if got[0].SellerName != want.SellerName || got[0].ListingURL != want.ListingURL || got[0].Title != want.Title {
		t.Fatalf("%+v", got[0])
	}
	if got[0].ItemID != want.ItemID {
		t.Fatalf("item_id %q want %q", got[0].ItemID, want.ItemID)
	}
}

func TestItemIDFromURL(t *testing.T) {
	if itemIDFromURL("https://x.nl/m999888777-foo") != "m999888777" {
		t.Fatal(itemIDFromURL("https://x.nl/m999888777-foo"))
	}
	u := "https://poshmark.com/listing/Apple-AirPods-Pro-69d767f15919e0cb8669d03b"
	if got := itemIDFromURL(u); got != "p69d767f15919e0cb8669d03b" {
		t.Fatalf("poshmark id %q", got)
	}
}
