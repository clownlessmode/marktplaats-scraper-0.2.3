package adminbot

import (
	"path/filepath"
	"strings"
	"testing"

	"github.com/marktplaats-scraper/scraper-golang/internal/listingsdb"
)

func TestMainPanelHTML(t *testing.T) {
	t.Parallel()
	if mainPanelHTML() == "" {
		t.Fatal("empty")
	}
}

func TestTemplatesListHTMLEmpty(t *testing.T) {
	t.Parallel()
	p := filepath.Join(t.TempDir(), "db.sqlite")
	db, err := listingsdb.Open(p)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	h := templatesListHTML(db, 1)
	if h == "" || !substringsAll(h, "Шаблоны", "Нет") {
		t.Fatal(h)
	}
}

func TestTemplatesListHTMLWithRows(t *testing.T) {
	p := filepath.Join(t.TempDir(), "db.sqlite")
	db, err := listingsdb.Open(p)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	_, err = listingsdb.AddEmailTemplate(db, 99, "T1", "body {title} here")
	if err != nil {
		t.Fatal(err)
	}
	h := templatesListHTML(db, 99)
	if !substringsAll(h, "T1", "body") {
		t.Fatal(h)
	}
	kb := templatesListKB(db, 99)
	if len(kb.InlineKeyboard) < 2 {
		t.Fatal("expected rows")
	}
}

func TestEmailsMenuKB(t *testing.T) {
	p := filepath.Join(t.TempDir(), "db.sqlite")
	db, err := listingsdb.Open(p)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	_, _ = listingsdb.AddEmailsBatch(db, 7, [][2]string{{"z@gmail.com", "p"}})
	kb := emailsMenuKB(db, 7)
	if len(kb.InlineKeyboard) < 4 {
		t.Fatalf("rows %d", len(kb.InlineKeyboard))
	}
}

func substringsAll(s string, subs ...string) bool {
	for _, x := range subs {
		if !strings.Contains(s, x) {
			return false
		}
	}
	return true
}
