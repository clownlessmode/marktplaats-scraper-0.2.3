package adminbot

import (
	"strings"
	"testing"
)

func TestEmailCallbackSafeRoundtrip(t *testing.T) {
	t.Parallel()
	for _, em := range []string{
		"simple@gmail.com",
		"user_name@domain.co.uk",
		"weird__user@test.com",
		"a:b@c.com", // ':' in local part after decode path
	} {
		enc := emailToCallbackSafe(em)
		dec := decodeEmailFromCallback(enc)
		want := strings.ToLower(strings.TrimSpace(em))
		if dec != want {
			t.Errorf("%q enc=%q dec=%q want %q", em, enc, dec, want)
		}
	}
}

func TestKbMainHasExpectedCallbacks(t *testing.T) {
	t.Parallel()
	kb := kbMain()
	if len(kb.InlineKeyboard) < 5 {
		t.Fatalf("expected >=5 rows, got %d", len(kb.InlineKeyboard))
	}
	data := kb.InlineKeyboard[0][0].CallbackData
	if data == nil || *data != "admin_pending" {
		t.Fatalf("first row callback: %v", data)
	}
}
