package adminbot

import (
	"database/sql"
	"fmt"
	"net/http"
	"sync"
	"time"

	tgbotapi "github.com/go-telegram-bot-api/telegram-bot-api/v5"

	"github.com/marktplaats-scraper/scraper-golang/internal/prettylog"
)

// Bot админ-панель: один процесс, опрос Telegram Updates.
type Bot struct {
	db     *sql.DB
	api    *tgbotapi.BotAPI
	client *tgbotapi.BotAPI
	cfg    Config
	// ownerID — владелец пула почт и шаблонов (ADMIN_CHAT_ID как user_id воркера в Python).
	ownerID int64

	dlgMu      sync.Mutex
	dlgStep       string // "", "emails", "tpl_name", "tpl_subject", "tpl_body", "tpl_editsubj"
	dlgTplName    string
	dlgTplSubject string
	dlgEditID     int64
}

// NewBot открывает API; tgHTTP — клиент для Bot API (например с прокси); nil = прямое подключение, таймаут 120s.
// client — второй бот для уведомления воркера при одобрении (если токен задан).
func NewBot(db *sql.DB, cfg Config, tgHTTP *http.Client) (*Bot, error) {
	if tgHTTP == nil {
		tgHTTP = &http.Client{Timeout: 120 * time.Second}
	}
	ep := cfg.APIEndpoint
	if ep == "" {
		ep = tgbotapi.APIEndpoint
	}
	api, err := tgbotapi.NewBotAPIWithClient(cfg.AdminBotToken, ep, tgHTTP)
	if err != nil {
		return nil, err
	}
	var client *tgbotapi.BotAPI
	if cfg.ClientBotToken != "" && cfg.ClientBotToken != cfg.AdminBotToken {
		c, err := tgbotapi.NewBotAPIWithClient(cfg.ClientBotToken, ep, tgHTTP)
		if err == nil {
			client = c
		}
	}
	return &Bot{
		db:      db,
		api:     api,
		client:  client,
		cfg:     cfg,
		ownerID: cfg.AdminChatID,
	}, nil
}

func (b *Bot) clearDialog() {
	b.dlgMu.Lock()
	b.dlgStep = ""
	b.dlgTplName = ""
	b.dlgTplSubject = ""
	b.dlgEditID = 0
	b.dlgMu.Unlock()
}

func (b *Bot) isAdminChat(chatID int64) bool {
	return chatID == b.cfg.AdminChatID
}

// Run блокирует поток до остановки процесса (long polling).
func (b *Bot) Run() {
	prettylog.Banner("Telegram · админ-бот", fmt.Sprintf("@%s · admin_chat_id=%d · long polling 60s", b.api.Self.UserName, b.cfg.AdminChatID))
	prettylog.Admin("ожидание апдейтов", "GetUpdatesChan")
	u := tgbotapi.NewUpdate(0)
	u.Timeout = 60
	updates := b.api.GetUpdatesChan(u)
	for up := range updates {
		b.handleUpdate(up)
	}
}
