package adminbot

import (
	"database/sql"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	tgbotapi "github.com/go-telegram-bot-api/telegram-bot-api/v5"

	"github.com/marktplaats-scraper/scraper-golang/internal/listingsdb"
)

const (
	testAdminToken  = "ADMIN_TEST_TOKEN"
	testClientToken = "CLIENT_TEST_TOKEN"
	testAdminChat   = int64(424242)
)

func fakeTelegramHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method", http.StatusMethodNotAllowed)
		return
	}
	// Реальный API: /bot<token>/<method> (без "/" между "bot" и токеном).
	path := strings.TrimPrefix(r.URL.Path, "/")
	if !strings.HasPrefix(path, "bot") {
		http.NotFound(w, r)
		return
	}
	rest := path[len("bot"):]
	i := strings.LastIndex(rest, "/")
	if i <= 0 || i >= len(rest)-1 {
		http.NotFound(w, r)
		return
	}
	method := rest[i+1:]
	_ = r.ParseForm()
	switch method {
	case "getMe":
		fmt.Fprintf(w, `{"ok":true,"result":{"id":1,"is_bot":true,"first_name":"Admin","username":"testadminbot"}}`)
	case "sendMessage", "editMessageText":
		fmt.Fprintf(w, `{"ok":true,"result":{"message_id":10,"date":1,"chat":{"id":%d,"type":"private"},"text":"ok"}}`, testAdminChat)
	case "answerCallbackQuery":
		fmt.Fprintf(w, `{"ok":true,"result":true}`)
	case "getFile":
		fmt.Fprintf(w, `{"ok":true,"result":{"file_id":"f1","file_path":"files/x.csv"}}`)
	case "sendDocument":
		_, _ = io.Copy(io.Discard, r.Body)
		_ = r.Body.Close()
		fmt.Fprintf(w, `{"ok":true,"result":{"message_id":11,"date":1,"chat":{"id":%d,"type":"private"},"document":{"file_id":"d1","file_name":"emails_export.csv"}}}`, testAdminChat)
	default:
		fmt.Fprintf(w, `{"ok":true,"result":{}}`)
	}
}

func newTestBot(t *testing.T, withClient bool) (*Bot, *sql.DB, func()) {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(fakeTelegramHandler))
	t.Cleanup(srv.Close)

	endpoint := srv.URL + "/bot%s/%s"
	client := &http.Client{Timeout: 10 * time.Second}

	adminAPI, err := tgbotapi.NewBotAPIWithClient(testAdminToken, endpoint, client)
	if err != nil {
		t.Fatal(err)
	}
	var clientAPI *tgbotapi.BotAPI
	if withClient {
		clientAPI, err = tgbotapi.NewBotAPIWithClient(testClientToken, endpoint, client)
		if err != nil {
			t.Fatal(err)
		}
	}

	dbPath := filepath.Join(t.TempDir(), "bot.sqlite")
	db, err := listingsdb.Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	cfg := Config{
		AdminBotToken:  testAdminToken,
		AdminChatID:    testAdminChat,
		ClientBotToken: "",
	}
	if withClient {
		cfg.ClientBotToken = testClientToken
	}
	b := &Bot{
		db:      db,
		api:     adminAPI,
		client:  clientAPI,
		cfg:     cfg,
		ownerID: testAdminChat,
	}
	return b, db, func() { _ = db.Close() }
}

func cbQ(chatID int64, msgID int, data string) tgbotapi.Update {
	return tgbotapi.Update{
		CallbackQuery: &tgbotapi.CallbackQuery{
			ID:   "cq1",
			Data: data,
			Message: &tgbotapi.Message{
				MessageID: msgID,
				Chat:      &tgbotapi.Chat{ID: chatID},
			},
		},
	}
}

func msgText(chatID int64, text string) tgbotapi.Update {
	return tgbotapi.Update{
		Message: &tgbotapi.Message{
			Chat: &tgbotapi.Chat{ID: chatID},
			Text: text,
		},
	}
}

func msgCommandStart(chatID int64) tgbotapi.Update {
	return tgbotapi.Update{
		Message: &tgbotapi.Message{
			Chat: &tgbotapi.Chat{ID: chatID},
			Entities: []tgbotapi.MessageEntity{
				{Type: "bot_command", Offset: 0, Length: 6}, // "/start"
			},
			Text: "/start",
		},
	}
}

func authorized(db *sql.DB, uid int64) int {
	var v int
	_ = db.QueryRow(`SELECT COALESCE(authorized,0) FROM users WHERE user_id = ?`, uid).Scan(&v)
	return v
}

func TestHandleUpdateIgnoresNonAdmin(t *testing.T) {
	b, db, done := newTestBot(t, false)
	defer done()
	b.handleUpdate(cbQ(999001, 1, "admin_main"))
	b.handleUpdate(msgText(999001, "hello"))
	var n int
	_ = db.QueryRow(`SELECT COUNT(*) FROM users`).Scan(&n)
	if n != 0 {
		t.Fatalf("unexpected users %d", n)
	}
}

func TestCallbackAdminMainAndPending(t *testing.T) {
	b, db, done := newTestBot(t, false)
	defer done()
	_, _ = db.Exec(`INSERT INTO users (user_id, authorized, created_at) VALUES (77, 0, '2020-01-01')`)

	b.handleUpdate(cbQ(testAdminChat, 5, "admin_main"))
	b.handleUpdate(cbQ(testAdminChat, 5, "admin_pending"))
	if authorized(db, 77) != 0 {
		t.Fatal("should still be pending")
	}
}

func TestCallbackApproveRejectBlockUnblockDelete(t *testing.T) {
	b, db, done := newTestBot(t, true)
	defer done()
	_, _ = db.Exec(`INSERT INTO users (user_id, authorized, created_at) VALUES (101, 0, '2020-01-01')`)

	b.handleUpdate(cbQ(testAdminChat, 1, "approve_101"))
	if authorized(db, 101) != 1 {
		t.Fatalf("approve: got authorized=%d", authorized(db, 101))
	}

	_, _ = db.Exec(`INSERT INTO users (user_id, authorized, created_at) VALUES (102, 1, '2020-01-02')`)
	b.handleUpdate(cbQ(testAdminChat, 1, "block_102"))
	var inBlocked int
	_ = db.QueryRow(`SELECT COUNT(*) FROM blocked_users WHERE user_id = 102`).Scan(&inBlocked)
	if inBlocked != 1 {
		t.Fatal("not blocked")
	}

	b.handleUpdate(cbQ(testAdminChat, 1, "unblock_102"))
	_ = db.QueryRow(`SELECT COUNT(*) FROM blocked_users WHERE user_id = 102`).Scan(&inBlocked)
	if inBlocked != 0 {
		t.Fatal("still blocked")
	}

	b.handleUpdate(cbQ(testAdminChat, 1, "reject_103"))
	_ = db.QueryRow(`SELECT COUNT(*) FROM blocked_users WHERE user_id = 103`).Scan(&inBlocked)
	if inBlocked != 1 {
		t.Fatal("reject should block new id")
	}

	_, _ = db.Exec(`INSERT OR REPLACE INTO users (user_id, authorized, created_at) VALUES (104, 1, '2020-01-03')`)
	b.handleUpdate(cbQ(testAdminChat, 1, "delete_104"))
	var c int
	_ = db.QueryRow(`SELECT COUNT(*) FROM users WHERE user_id = 104`).Scan(&c)
	if c != 0 {
		t.Fatal("user should be deleted")
	}

	b.handleUpdate(cbQ(testAdminChat, 1, "delete_999999"))
}

func TestCallbackWorkersScreens(t *testing.T) {
	b, db, done := newTestBot(t, false)
	defer done()
	_, _ = db.Exec(`INSERT INTO users (user_id, authorized, created_at, shift_active) VALUES (201, 1, '2020-01-01', 1)`)
	_, _ = db.Exec(`INSERT INTO worker_listings (item_id, user_id, received_at) VALUES ('x1', 201, datetime('now'))`)

	b.handleUpdate(cbQ(testAdminChat, 2, "admin_workers"))
	b.handleUpdate(cbQ(testAdminChat, 2, "admin_blocked"))
	b.handleUpdate(cbQ(testAdminChat, 2, "admin_emails"))
}

func TestCallbackEmailsFlow(t *testing.T) {
	b, db, done := newTestBot(t, false)
	defer done()
	_, _ = listingsdb.AddEmailsBatch(db, testAdminChat, [][2]string{
		{"mail1@gmail.com", "p1"},
		{"mail2@gmail.com", "p2"},
	})
	_ = listingsdb.MarkEmailBlocked(db, testAdminChat, "mail2@gmail.com")

	b.handleUpdate(cbQ(testAdminChat, 3, "emails_list_0"))
	safe := emailToCallbackSafe("mail2@gmail.com")
	b.handleUpdate(cbQ(testAdminChat, 3, "emails_unblock_0_"+safe))

	var bl int
	_ = db.QueryRow(`SELECT COALESCE(blocked,0) FROM emails WHERE user_id = ? AND email = ?`, testAdminChat, "mail2@gmail.com").Scan(&bl)
	if bl != 0 {
		t.Fatal("expected unblocked")
	}

	safe1 := emailToCallbackSafe("mail1@gmail.com")
	b.handleUpdate(cbQ(testAdminChat, 3, "emails_del_0_"+safe1))
	var n int
	_ = db.QueryRow(`SELECT COUNT(*) FROM emails WHERE email = 'mail1@gmail.com'`).Scan(&n)
	if n != 0 {
		t.Fatal("email should be deleted")
	}
}

func TestCallbackEmailsExport(t *testing.T) {
	b, db, done := newTestBot(t, false)
	defer done()
	_, _ = listingsdb.AddEmailsBatch(db, testAdminChat, [][2]string{{"ex@gmail.com", "sec"}})
	b.handleUpdate(cbQ(testAdminChat, 4, "emails_export"))
}

// SMTP: callbacks emails_test и emails_test_all вызывают mailer.SendTestEmail (Gmail).
// Их не гоняем в CI; при необходимости проверяйте вручную с тестовой почтой.

func TestMessageStart(t *testing.T) {
	b, _, done := newTestBot(t, false)
	defer done()
	b.handleUpdate(msgCommandStart(testAdminChat))
}

func TestMessageEmailsDialog(t *testing.T) {
	b, db, done := newTestBot(t, false)
	defer done()
	b.dlgStep = "emails"
	b.handleUpdate(msgText(testAdminChat, "a@gmail.com:x\nb@gmail.com;y"))
	var n int
	_ = db.QueryRow(`SELECT COUNT(*) FROM emails WHERE user_id = ?`, testAdminChat).Scan(&n)
	if n != 2 {
		t.Fatalf("want 2 emails, got %d", n)
	}
}

func TestMessageEmailsDialogInvalid(t *testing.T) {
	b, _, done := newTestBot(t, false)
	defer done()
	b.dlgStep = "emails"
	b.handleUpdate(msgText(testAdminChat, "not an email line"))
}

func TestMessageTplNameEmpty(t *testing.T) {
	b, _, done := newTestBot(t, false)
	defer done()
	b.dlgStep = "tpl_name"
	b.handleUpdate(msgText(testAdminChat, "   "))
	if b.dlgStep != "tpl_name" {
		t.Fatalf("step should stay tpl_name, got %q", b.dlgStep)
	}
}

func TestMessageTemplateDialog(t *testing.T) {
	b, db, done := newTestBot(t, false)
	defer done()
	b.dlgStep = "tpl_name"
	b.handleUpdate(msgText(testAdminChat, "My Template"))
	if b.dlgStep != "tpl_subject" {
		t.Fatalf("dlgStep=%q", b.dlgStep)
	}
	b.handleUpdate(msgText(testAdminChat, "Subj {title}"))
	if b.dlgStep != "tpl_body" {
		t.Fatalf("dlgStep=%q", b.dlgStep)
	}
	b.handleUpdate(msgText(testAdminChat, "Hello {title} — {price}"))
	var cnt int
	var subj string
	_ = db.QueryRow(`SELECT COUNT(*) FROM email_templates WHERE user_id = ? AND name = 'My Template'`, testAdminChat).Scan(&cnt)
	if cnt != 1 {
		t.Fatalf("templates count %d", cnt)
	}
	_ = db.QueryRow(`SELECT subject_template FROM email_templates WHERE user_id = ? AND name = 'My Template'`, testAdminChat).Scan(&subj)
	if subj != "Subj {title}" {
		t.Fatalf("subject=%q", subj)
	}
}

func TestMessageTemplateEdit(t *testing.T) {
	b, db, done := newTestBot(t, false)
	defer done()
	id, err := listingsdb.AddEmailTemplate(db, testAdminChat, "E1", "S {title}", "old")
	if err != nil {
		t.Fatal(err)
	}
	b.dlgStep = "tpl_body"
	b.dlgTplName = "E1"
	b.dlgTplSubject = "S {title}"
	b.dlgEditID = id
	b.handleUpdate(msgText(testAdminChat, "new body"))
	var body string
	_ = db.QueryRow(`SELECT body FROM email_templates WHERE id = ?`, id).Scan(&body)
	if body != "new body" {
		t.Fatalf("body=%q", body)
	}
}

func TestCallbackTemplatesDelete(t *testing.T) {
	b, db, done := newTestBot(t, false)
	defer done()
	id, err := listingsdb.AddEmailTemplate(db, testAdminChat, "A", "", "b")
	if err != nil {
		t.Fatal(err)
	}
	b.handleUpdate(cbQ(testAdminChat, 6, fmt.Sprintf("tpl_del_%d", id)))
	var c int
	_ = db.QueryRow(`SELECT COUNT(*) FROM email_templates WHERE id = ?`, id).Scan(&c)
	if c != 0 {
		t.Fatal("template should be gone")
	}
}

func TestCallbackTplEdbodyOpensBodyDialog(t *testing.T) {
	b, db, done := newTestBot(t, false)
	defer done()
	id, err := listingsdb.AddEmailTemplate(db, testAdminChat, "N", "sub here", "body text")
	if err != nil {
		t.Fatal(err)
	}
	b.handleUpdate(cbQ(testAdminChat, 7, fmt.Sprintf("tpl_edbody_%d", id)))
	if b.dlgStep != "tpl_body" || b.dlgEditID != id || b.dlgTplSubject != "sub here" {
		t.Fatalf("dlg step=%q edit=%d subj=%q", b.dlgStep, b.dlgEditID, b.dlgTplSubject)
	}
}

func TestCallbackEmailsMenuBranches(t *testing.T) {
	b, _, done := newTestBot(t, false)
	defer done()
	b.handleUpdate(cbQ(testAdminChat, 8, "emails_add"))
	b.handleUpdate(cbQ(testAdminChat, 8, "emails_upload"))
	b.handleUpdate(cbQ(testAdminChat, 8, "admin_emails"))
}

func TestCallbackUnknown(t *testing.T) {
	b, _, done := newTestBot(t, false)
	defer done()
	b.handleUpdate(cbQ(testAdminChat, 9, "totally_unknown_cb"))
}
