package adminbot

import (
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	tgbotapi "github.com/go-telegram-bot-api/telegram-bot-api/v5"

	"github.com/marktplaats-scraper/scraper-golang/internal/listingsdb"
)

func TestNewBotWithFakeTelegram(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(fakeTelegramHandler))
	t.Cleanup(srv.Close)
	cl := &http.Client{Timeout: 5 * time.Second}

	dbPath := filepath.Join(t.TempDir(), "n.sqlite")
	db, err := listingsdb.Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })

	cfg := Config{
		AdminBotToken:  testAdminToken,
		AdminChatID:    testAdminChat,
		ClientBotToken: "",
		APIEndpoint:    srv.URL + "/bot%s/%s",
	}
	b, err := NewBot(db, cfg, cl)
	if err != nil {
		t.Fatal(err)
	}
	if b.api.Self.UserName != "testadminbot" {
		t.Fatalf("Self: %+v", b.api.Self)
	}
	if b.client != nil {
		t.Fatal("expected no client bot")
	}
}

func TestNewBotWithClientBot(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(fakeTelegramHandler))
	t.Cleanup(srv.Close)
	cl := &http.Client{Timeout: 5 * time.Second}

	dbPath := filepath.Join(t.TempDir(), "c.sqlite")
	db, err := listingsdb.Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })

	cfg := Config{
		AdminBotToken:  testAdminToken,
		AdminChatID:    testAdminChat,
		ClientBotToken: testClientToken,
		APIEndpoint:    srv.URL + "/bot%s/%s",
	}
	b, err := NewBot(db, cfg, cl)
	if err != nil {
		t.Fatal(err)
	}
	if b.client == nil {
		t.Fatal("expected client bot")
	}
}

func TestNewBotSameClientTokenSkipped(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(fakeTelegramHandler))
	t.Cleanup(srv.Close)
	cl := &http.Client{Timeout: 5 * time.Second}
	dbPath := filepath.Join(t.TempDir(), "s.sqlite")
	db, err := listingsdb.Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })

	cfg := Config{
		AdminBotToken:  testAdminToken,
		AdminChatID:    testAdminChat,
		ClientBotToken: testAdminToken,
		APIEndpoint:    srv.URL + "/bot%s/%s",
	}
	b, err := NewBot(db, cfg, cl)
	if err != nil {
		t.Fatal(err)
	}
	if b.client != nil {
		t.Fatal("same token should not create second bot")
	}
}

func TestCallbackNilMessageIgnored(t *testing.T) {
	b, _, done := newTestBot(t, false)
	defer done()
	b.handleUpdate(tgbotapi.Update{
		CallbackQuery: &tgbotapi.CallbackQuery{
			ID:      "x",
			Data:    "admin_main",
			Message: nil,
		},
	})
}
