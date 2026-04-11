package clientbot

import (
	"database/sql"
	"fmt"
	"net/http"
	"sync"
	"time"

	tgbotapi "github.com/go-telegram-bot-api/telegram-bot-api/v5"

	"github.com/marktplaats-scraper/scraper-golang/internal/prettylog"
)

// Bot воркерский Telegram-бот.
type Bot struct {
	db         *sql.DB
	api        *tgbotapi.BotAPI
	adminAPI   *tgbotapi.BotAPI // уведомления админу отдельным токеном (как Python ADMIN_BOT_TOKEN)
	cfg        Config
	httpClient *http.Client // тот же транспорт, что у Bot API — и для скачивания файлов getFile

	dlgMu        sync.Mutex
	dlgStep      string // "", worker_emails, worker_tpl_name, worker_tpl_subject, worker_tpl_editsubj, worker_tpl_body, worker_bulk_csv
	dlgTplName   string
	dlgTplSubject string
	dlgTplEditID int64
	dlgBulkDelay time.Duration
}

// NewBot client = CLIENT_BOT_TOKEN; при заданном AdminBotToken — второй API для sendMessage админу.
func NewBot(db *sql.DB, cfg Config, tgHTTP *http.Client) (*Bot, error) {
	if tgHTTP == nil {
		tgHTTP = &http.Client{Timeout: 120 * time.Second}
	}
	ep := cfg.APIEndpoint
	if ep == "" {
		ep = tgbotapi.APIEndpoint
	}
	api, err := tgbotapi.NewBotAPIWithClient(cfg.ClientBotToken, ep, tgHTTP)
	if err != nil {
		return nil, err
	}
	var adminAPI *tgbotapi.BotAPI
	if cfg.AdminBotToken != "" && cfg.AdminBotToken != cfg.ClientBotToken {
		a, err := tgbotapi.NewBotAPIWithClient(cfg.AdminBotToken, ep, tgHTTP)
		if err == nil {
			adminAPI = a
		} else {
			prettylog.Warnf("клиент-бот: ADMIN_BOT_TOKEN не поднялся (уведомления только через этот бот): %v", err)
		}
	}
	return &Bot{db: db, api: api, adminAPI: adminAPI, cfg: cfg, httpClient: tgHTTP}, nil
}

func (b *Bot) clearDialog() {
	b.dlgMu.Lock()
	b.dlgStep = ""
	b.dlgTplName = ""
	b.dlgTplSubject = ""
	b.dlgTplEditID = 0
	b.dlgBulkDelay = 0
	b.dlgMu.Unlock()
}

func (b *Bot) notifyAPI() *tgbotapi.BotAPI {
	if b.adminAPI != nil {
		return b.adminAPI
	}
	return b.api
}

// Run long polling.
func (b *Bot) Run() {
	prettylog.Banner("Telegram · клиент-бот (воркеры)", fmt.Sprintf("@%s · long polling 60s", b.api.Self.UserName))
	prettylog.Worker("ожидание апдейтов", "GetUpdatesChan")
	u := tgbotapi.NewUpdate(0)
	u.Timeout = 60
	ch := b.api.GetUpdatesChan(u)
	for up := range ch {
		b.handleUpdate(up)
	}
}
