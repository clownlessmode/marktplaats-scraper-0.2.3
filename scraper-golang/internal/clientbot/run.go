package clientbot

import (
	"context"
	"flag"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	tgbotapi "github.com/go-telegram-bot-api/telegram-bot-api/v5"

	"github.com/marktplaats-scraper/scraper-golang/internal/envload"
	"github.com/marktplaats-scraper/scraper-golang/internal/listingsdb"
	"github.com/marktplaats-scraper/scraper-golang/internal/prettylog"
	"github.com/marktplaats-scraper/scraper-golang/internal/proxy"
)

type tgLibLogger struct{}

func (tgLibLogger) Printf(format string, v ...any) {
	prettylog.Workerf(strings.TrimSuffix(format, "\n"), v...)
}

func (tgLibLogger) Println(v ...any) {
	prettylog.Worker(strings.TrimSpace(fmt.Sprint(v...)), "")
}

// RunCLI точка входа cmd/clientbot.
func RunCLI() {
	envload.LoadDefaults()
	_ = tgbotapi.SetLogger(tgLibLogger{})
	dbPath := flag.String("db", "", "путь к SQLite (bot.db)")
	token := flag.String("token", "", "переопределить CLIENT_BOT_TOKEN / BOT_TOKEN")
	adminChat := flag.String("admin-chat", "", "ADMIN_CHAT_ID для уведомлений о новых воркерах")
	proxiesFile := flag.String("proxies", "proxies.txt", "прокси для Bot API (как у adminbot)")
	skipProxyCheck := flag.Bool("skip-proxy-check", false, "")
	probeURL := flag.String("proxy-probe", "https://example.com", "для Telegram лучше https://api.telegram.org — проверка того же хоста, что getUpdates")
	proxyConc := flag.Int("proxy-concurrency", 8, "")
	flag.Parse()

	var cfg Config
	cfg.DBPath = *dbPath
	cfg.ClientBotToken = *token
	if *adminChat != "" {
		if id, err := strconv.ParseInt(*adminChat, 10, 64); err == nil {
			cfg.AdminChatID = id
		}
	}
	cfg.LoadFromEnv()
	if err := cfg.validate(); err != nil {
		prettylog.Fatal(err.Error())
	}

	prettylog.Section("Клиент-бот · старт")
	prettylog.Worker("SQLite", cfg.DBPath)

	db, err := listingsdb.Open(cfg.DBPath)
	if err != nil {
		prettylog.Fatalf("SQLite: %v", err)
	}
	defer db.Close()

	var tgHTTP *http.Client
	entries, proxyPath, err := proxy.LoadProxiesSearch(*proxiesFile)
	if err != nil {
		prettylog.Fatalf("прокси: %v", err)
	}
	if len(entries) > 0 {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
		defer cancel()
		var chosen *proxy.Entry
		if *skipProxyCheck {
			chosen = &entries[0]
			prettylog.Proxy("Telegram (клиент) · без проверки", chosen.Mask()+" · "+proxyPath)
		} else {
			prettylog.Proxyf("Telegram (клиент) · проверка прокси · %s", *probeURL)
			chosen, err = proxy.PickFirstWorking(ctx, entries, *probeURL, *proxyConc, func(e proxy.Entry, err error) {
				prettylog.Pagef("прокси не подошёл %s · %v", e.Mask(), err)
			})
			if err != nil {
				prettylog.Fatalf("прокси: %v", err)
			}
			prettylog.OK("клиент через прокси", chosen.Mask()+" · getUpdates и скачивание файлов (.csv) — один HTTP-клиент")
		}
		tgHTTP, err = proxy.HTTPClient(chosen, 120*time.Second)
		if err != nil {
			prettylog.Fatalf("прокси HTTP-клиент: %v", err)
		}
	} else {
		prettylog.Browser("Telegram Bot API (клиент)", "напрямую")
		prettylog.Warn("прокси не задан", "при EOF/обрыве до api.telegram.org добавьте proxies.txt (или -proxies путь). Проверка: -proxy-probe https://api.telegram.org")
	}

	b, err := NewBot(db, cfg, tgHTTP)
	if err != nil {
		prettylog.Fatalf("Telegram API: %v", err)
	}
	b.Run()
}
