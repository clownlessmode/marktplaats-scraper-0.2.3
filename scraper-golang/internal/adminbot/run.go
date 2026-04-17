package adminbot

import (
	"context"
	"flag"
	"net/http"
	"strconv"
	"time"

	"github.com/marktplaats-scraper/scraper-golang/internal/envload"
	"github.com/marktplaats-scraper/scraper-golang/internal/listingsdb"
	"github.com/marktplaats-scraper/scraper-golang/internal/prettylog"
	"github.com/marktplaats-scraper/scraper-golang/internal/proxy"
)

// RunCLI точка входа из cmd/adminbot: флаги, .env, long polling.
func RunCLI() {
	envload.LoadDefaults()
	dbPath := flag.String("db", "", "путь к SQLite (как data/bot.db в Python)")
	token := flag.String("token", "", "переопределить ADMIN_BOT_TOKEN")
	adminChat := flag.String("admin-chat", "", "переопределить ADMIN_CHAT_ID (числовой id чата)")
	proxiesFile := flag.String("proxies", "proxies.txt", "файл прокси для Bot API (пусто/нет файла — без прокси; ищется также scraper-golang/proxies.txt)")
	skipProxyCheck := flag.Bool("skip-proxy-check", false, "взять первую строку из файла прокси без проверки")
	probeURL := flag.String("proxy-probe", "https://api.telegram.org", "URL проверки прокси (тот же хост, что Bot API)")
	proxyConc := flag.Int("proxy-concurrency", 8, "параллельных проверок прокси (как у marktplaats-playwright)")
	flag.Parse()

	var cfg Config
	cfg.DBPath = *dbPath
	cfg.AdminBotToken = *token
	if *adminChat != "" {
		if id, err := strconv.ParseInt(*adminChat, 10, 64); err == nil {
			cfg.AdminChatID = id
		}
	}
	cfg.LoadFromEnv()
	if err := cfg.validate(); err != nil {
		prettylog.Fatal(err.Error())
	}

	prettylog.Section("Админ-бот · старт")
	prettylog.Admin("SQLite", cfg.DBPath)

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
			prettylog.Proxy("Telegram Bot API · без проверки", chosen.Mask()+" · "+proxyPath)
		} else {
			prettylog.Proxyf("Telegram Bot API · проверка прокси · probe=%s · потоков=%d", *probeURL, *proxyConc)
			chosen, err = proxy.PickFirstWorking(ctx, entries, *probeURL, *proxyConc, func(e proxy.Entry, err error) {
				prettylog.Pagef("прокси не подошёл %s · %v", e.Mask(), err)
			})
			if err != nil {
				prettylog.Fatalf("прокси: %v", err)
			}
			prettylog.OK("Telegram через прокси", chosen.Mask()+" · "+proxyPath)
		}
		tgHTTP, err = proxy.HTTPClient(chosen, 120*time.Second)
		if err != nil {
			prettylog.Fatalf("прокси HTTP-клиент: %v", err)
		}
	} else {
		prettylog.Browser("Telegram Bot API", "напрямую (файл прокси пуст или не найден)")
	}

	b, err := NewBot(db, cfg, tgHTTP)
	if err != nil {
		prettylog.Fatalf("Telegram API (getMe): %v", err)
	}
	b.Run()
}
