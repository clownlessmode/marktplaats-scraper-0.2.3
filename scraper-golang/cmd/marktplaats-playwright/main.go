package main

import (
	"context"
	"database/sql"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/playwright-community/playwright-go"

	"github.com/marktplaats-scraper/scraper-golang/internal/envload"
	"github.com/marktplaats-scraper/scraper-golang/internal/listingsdb"
	"github.com/marktplaats-scraper/scraper-golang/internal/mailer"
	"github.com/marktplaats-scraper/scraper-golang/internal/marktplaats"
	"github.com/marktplaats-scraper/scraper-golang/internal/prettylog"
	"github.com/marktplaats-scraper/scraper-golang/internal/proxy"
)

const defaultURL = "https://www.marktplaats.nl/"

func main() {
	envload.LoadDefaults()

	headless := flag.Bool("headless", false, "запуск Chromium без окна (headless)")
	nonHeadless := flag.Bool("non-headless", false, "запуск Chromium с видимым окном; перекрывает -headless")
	url := flag.String("url", defaultURL, "страница для открытия (игнорируется с -scrape)")
	proxiesFile := flag.String("proxies", "proxies.txt", "файл со списком прокси; если файла нет — без прокси")
	skipProxyCheck := flag.Bool("skip-proxy-check", false, "не проверять прокси перед стартом, взять первую строку (не рекомендуется)")
	probeURL := flag.String("proxy-probe", "https://example.com", "URL для проверки прокси перед запуском браузера")
	proxyConc := flag.Int("proxy-concurrency", 16, "число одновременных проверок прокси до первого успеха (1 = по очереди, лог каждой ошибки)")

	scrape := flag.Bool("scrape", false, "режим парсера Marktplaats (категории + объявления), как scraper-python")
	limit := flag.Int("limit", 5, "максимум новых объявлений за один -scrape (0 = по счётчику категории)")
	fast := flag.Bool("fast", false, "быстрый режим: только __NEXT_DATA__ (как MpScraper fast=True в Python)")
	skipCount := flag.Bool("skip-count", false, "не запрашивать число объявлений по категории (skip_count)")
	parentIdx := flag.Int("parent-index", 0, "индекс родительской категории с главной (с нуля)")
	scrapeBase := flag.String("scrape-base", marktplaats.BaseURL, "базовый URL сайта для скрапера")
	dbPath := flag.String("db", "", "путь к SQLite (таблица listings как в telegram_bot/database.py); пусто — не сохранять")
	maxAgeHours := flag.Float64("max-age-hours", 0, "максимальный возраст объявления в часах; при превышении — как CategoryStale в Python (0 = не ограничивать; типично 3)")
	sendMail := flag.Bool("send-mail", false, "после скрапа — рассылка писем продавцам (как try_send_listing_email / Gmail SMTP); нужны -db и -mail-user-id")
	mailUserID := flag.Int64("mail-user-id", 0, "Telegram user_id воркера (таблица emails в той же SQLite, что и -db)")
	mailDelay := flag.Duration("mail-delay", 0, "пауза между письмами (например 3s)")
	skipMailRCPT := flag.Bool("skip-mail-rcpt-verify", false, "не проверять существование @gmail.com продавца через SMTP RCPT (быстрее)")
	scrapeLoop := flag.Bool("scrape-loop", false, "бесконечные проходы по всем родительским категориям; между проходами пауза -scrape-loop-interval")
	scrapeLoopInterval := flag.Duration("scrape-loop-interval", 30*time.Minute, "пауза между полными проходами при -scrape-loop")
	scrapeWorkers := flag.Int("scrape-workers", 1, "число параллельных браузерных контекстов; общая очередь родительских категорий")
	scrapeAllParents := flag.Bool("scrape-all-parents", false, "обработать все родительские категории (иначе только -parent-index)")

	flag.Parse()

	scrapeAllParentsEff := *scrapeAllParents
	if *scrapeLoop {
		scrapeAllParentsEff = true
	}
	usePool := *scrape && (*scrapeWorkers > 1 || scrapeAllParentsEff || *scrapeLoop)
	if *scrape && *scrapeWorkers < 1 {
		prettylog.Fatal("-scrape-workers должен быть ≥ 1")
	}
	headlessRun := *headless && !*nonHeadless

	progStart := time.Now()
	defer prettylog.TotalElapsed(progStart)

	if *scrape {
		prettylog.Banner("Marktplaats — скрапер", "режим парсинга")
	} else {
		prettylog.Banner("Marktplaats — Playwright", "открытие страницы")
	}

	if os.Getenv("PLAYWRIGHT_SKIP_BROWSER_DOWNLOAD") == "1" {
		prettylog.PlaywrightWarn("PLAYWRIGHT_SKIP_BROWSER_DOWNLOAD=1 — при необходимости: go run github.com/playwright-community/playwright-go/cmd/playwright@latest install chromium")
	}

	entries, usedPath, err := resolveProxiesFile(*proxiesFile)
	if err != nil {
		prettylog.Fatalf("файл прокси: %v", err)
	}
	var pwProxy *playwright.Proxy
	var poolProxyRing *proxyRing // пул: прокси только на контекстах + ротация при блокировках/таймаутах
	if len(entries) > 0 {
		prettylog.Proxy(fmt.Sprintf("загружено %d записей", len(entries)), usedPath)
		if *skipProxyCheck {
			pwProxy = entries[0].Playwright()
			poolProxyRing = newProxyRing(entries, 0)
			prettylog.Warn("проверка прокси отключена", entries[0].Mask())
		} else {
			if *proxyConc > 1 {
				prettylog.Proxyf("параллельная проверка до первого успеха · потоков: %d", *proxyConc)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
			defer cancel()
			donePick := prettylog.Timer("прокси: проверка до первого рабочего")
			chosen, err := proxy.PickFirstWorking(ctx, entries, *probeURL, *proxyConc, func(e proxy.Entry, err error) {
				prettylog.Pagef("прокси не подошёл %s → %v", e.Mask(), err)
			})
			donePick()
			if err != nil {
				prettylog.Fatalf("нет рабочего прокси (проверка %s). Исправьте список, увеличьте -proxy-concurrency или укажите -skip-proxy-check", *probeURL)
			}
			pwProxy = chosen.Playwright()
			poolProxyRing = newProxyRing(entries, chosenEntryIndex(entries, *chosen))
			prettylog.OK("прокси выбран", chosen.Mask())
		}
	} else if usedPath != "" {
		prettylog.Proxy("файл без записей", usedPath)
		prettylog.Browser("подключение", "напрямую, без прокси")
	} else {
		prettylog.Proxy("файл proxies.txt не найден", "напрямую")
	}

	policy := marktplaats.LeanResourcePolicy()
	prettylog.Section("Запуск браузера")
	donePW := prettylog.Timer("браузер: Chromium, stealth, маршрут ресурсов")
	var sess *marktplaats.PlaywrightSession
	var pb *marktplaats.PlaywrightBrowser
	if usePool {
		var err error
		// Прокси только на BrowserContext — иначе при смене прокси остаётся launch-level endpoint.
		pb, err = marktplaats.StartPlaywrightBrowser(headlessRun, nil)
		donePW()
		if err != nil {
			prettylog.Fatalf("%v", err)
		}
		defer pb.Shutdown()
		prettylog.Browser("пул", fmt.Sprintf("один Chromium · до %d параллельных контекстов", *scrapeWorkers))
	} else {
		var err error
		sess, err = marktplaats.StartPlaywrightSession(headlessRun, pwProxy, policy)
		donePW()
		if err != nil {
			prettylog.Fatalf("%v", err)
		}
		defer sess.Shutdown()
	}
	prettylog.Browser("сеть: по умолчанию не грузим", "image, stylesheet, font, media, manifest, ws, sse, ping (route); document/script/fetch/xhr — да")

	if *scrape {
		var db *sql.DB
		if *dbPath != "" {
			var derr error
			db, derr = listingsdb.Open(*dbPath)
			if derr != nil {
				prettylog.Fatalf("SQLite: %v", derr)
			}
			defer db.Close()
			prettylog.Scrape("SQLite", *dbPath)
			if mailer.DevEnvironment() {
				if err := listingsdb.ClearListings(db); err != nil {
					prettylog.Fatalf("SQLite: очистка listings (режим dev): %v", err)
				}
				prettylog.Scrape("режим dev", "таблица listings очищена перед скрапом (ENVIRONMENT=dev)")
			}
		}
		var maxAge *float64
		if *maxAgeHours > 0 {
			v := *maxAgeHours
			maxAge = &v
			prettylog.Scrapef("фильтр по возрасту · объявления старше %.1f ч отбрасываются (как в Python)", v)
		}
		if *sendMail {
			if db == nil {
				prettylog.Fatal("-send-mail требует -db (та же SQLite, где почты и шаблоны воркера)")
			}
			if *mailUserID == 0 {
				prettylog.Fatal("-send-mail требует -mail-user-id (Telegram user_id воркера)")
			}
			prettylog.Scrape(mailer.MailModeLogLine(), "")
		}
		m := mailOpts{
			send:     *sendMail,
			userID:   *mailUserID,
			delay:    *mailDelay,
			skipRCPT: *skipMailRCPT,
		}
		if usePool {
			if !*scrapeLoop {
				defer prettylog.Timer("скрап: пул (категории + объявления + таблица)")()
			}
			if scrapeAllParentsEff || *scrapeWorkers > 1 || *scrapeLoop {
				prettylog.Scrapef("режим пула · воркеров: %d · все родители: %t · бесконечный цикл: %t",
					*scrapeWorkers, scrapeAllParentsEff, *scrapeLoop)
			}
			if *scrapeLoop {
				prettylog.Scrapef("пауза между циклами · %s", (*scrapeLoopInterval).String())
			}
			ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
			defer stop()
			for cycle := 1; ; cycle++ {
				select {
				case <-ctx.Done():
					prettylog.Warn("останов по сигналу", "")
					return
				default:
				}
				if *scrapeLoop {
					prettylog.Section(fmt.Sprintf("Цикл скрапа %d", cycle))
				}
				list, saved, _, audits, perr := runScrapePool(ctx, pb, scrapePoolParams{
					baseURL:    *scrapeBase,
					limit:      *limit,
					fast:       *fast,
					skipCount:  *skipCount,
					db:         db,
					maxAge:     maxAge,
					workers:    *scrapeWorkers,
					allParents: scrapeAllParentsEff,
					parentIdx:  *parentIdx,
					proxyRing:  poolProxyRing,
					policy:     policy,
				})
				if perr != nil {
					prettylog.Warnf("ошибка прохода: %v", perr)
				}
				printPoolResults(audits, list, saved, *scrapeWorkers > 1, db)
				sendMailAfterScrape(db, m, list)
				if !*scrapeLoop {
					break
				}
				prettylog.Scrapef("следующий цикл через %s (Ctrl+C — выход)", (*scrapeLoopInterval).String())
				select {
				case <-ctx.Done():
					prettylog.Warn("останов по сигналу", "")
					return
				case <-time.After(*scrapeLoopInterval):
				}
			}
			return
		}
		runScrape(sess, *scrapeBase, *limit, *fast, *skipCount, *parentIdx, db, maxAge, m)
		return
	}

	doneNav := prettylog.Timer("переход на целевой URL")
	resp, err := sess.Page.Goto(*url, playwright.PageGotoOptions{
		WaitUntil: playwright.WaitUntilStateDomcontentloaded,
		Timeout:   playwright.Float(120_000),
	})
	doneNav()
	if err != nil {
		prettylog.Fatalf("переход на %s: %v", *url, err)
	}
	if resp != nil {
		prettylog.Browserf("ответ HTTP %d", resp.Status())
		prettylog.Browser("итоговый URL", sess.Page.URL())
	}
	title, _ := sess.Page.Title()
	prettylog.Browser("заголовок страницы", title)

	if headlessRun {
		prettylog.OK("готово (headless)", "")
		return
	}
	prettylog.Warn("окно открыто", "выход — Ctrl+C")
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, syscall.SIGTERM)
	<-sig
}

type mailOpts struct {
	send     bool
	userID   int64
	delay    time.Duration
	skipRCPT bool
}

func runScrape(sess *marktplaats.PlaywrightSession, base string, limit int, fast, skipCount bool, parentIdx int, db *sql.DB, maxAge *float64, m mailOpts) {
	defer prettylog.Timer("скрап: полный цикл (категории + объявления + таблица)")()

	sc := marktplaats.NewScraper(sess.Page)
	sc.BaseURL = base
	sc.Fast = fast
	sc.SkipCount = skipCount
	sc.TimeoutMS = 30_000
	stats := &marktplaats.TimingStats{}
	sc.Stats = stats

	prettylog.Section("Категории")
	prettylog.Scrapef("главная → родительские категории (%s)", sc.BaseURL)
	doneParents := prettylog.Timer("категории: главная, HTML и __CONFIG__")
	parents, err := sc.GetParentCategories()
	doneParents()
	if err != nil {
		prettylog.Fatalf("родительские категории: %v", err)
	}
	prettylog.OKf("найдено родительских категорий: %d", len(parents))
	if len(parents) == 0 {
		prettylog.Fatal("родительских категорий нет")
	}
	if parentIdx < 0 || parentIdx >= len(parents) {
		prettylog.Fatalf("parent-index %d вне диапазона [0,%d)", parentIdx, len(parents))
	}
	p := parents[parentIdx]
	prettylog.Scrape("выбрана категория", fmt.Sprintf("[%d] id=%d", parentIdx, p.ID))
	prettylog.Scrape("URL", p.URL)

	prettylog.Section("Сбор объявлений")
	if fast {
		prettylog.Scrape("режим", "быстрый (__NEXT_DATA__ только)")
	} else {
		prettylog.Scrape("режим", "полный (страница каждого объявления)")
	}

	savedDB := 0
	var listingAudits []marktplaats.ListingAuditRow
	opts := marktplaats.GetListingsOptions{
		SkipCount:   skipCount,
		MaxAgeHours: maxAge,
		AuditTrail:  &listingAudits,
	}
	if db != nil {
		existing, lerr := listingsdb.LoadItemIDs(db)
		if lerr != nil {
			prettylog.Fatalf("SQLite: загрузка item_id: %v", lerr)
		}
		opts.ExistingIDs = existing
		prettylog.Scrapef("в БД уже объявлений (дубликаты пропускаются): %d", len(existing))
		opts.OnBatch = func(batch []marktplaats.Listing) {
			for _, item := range batch {
				if uerr := listingsdb.Upsert(db, item); uerr != nil {
					prettylog.Warnf("SQLite upsert %s: %v", item.ItemID, uerr)
					continue
				}
				savedDB++
			}
		}
	}
	doneListings := prettylog.Timer("объявления: подкатегории, счётчик, страницы поиска")
	list, err := sc.GetListings(p, limit, opts)
	doneListings()
	if err != nil {
		var cs *marktplaats.CategoryStaleError
		if errors.As(err, &cs) {
			prettylog.Warnf("категория прервана по возрасту объявлений: %v", err)
			prettylog.Warnf("сохранено объявлений до остановки: %d", len(cs.Listings))
			list = cs.Listings
		} else {
			prettylog.Fatalf("получение объявлений: %v", err)
		}
	}

	nav, wNext, nNav, nWait := stats.Snapshot()
	if nNav > 0 {
		prettylog.TimingNote(fmt.Sprintf("сумма Goto/навигаций (%d переходов)", nNav), nav)
	}
	if nWait > 0 {
		prettylog.TimingNote(fmt.Sprintf("сумма ожиданий #__NEXT_DATA__ (%d раз)", nWait), wNext)
	}

	prettylog.Section("Результат")
	now := time.Now()
	auditRows := make([]prettylog.ScrapedListingAudit, 0, len(listingAudits))
	for i, a := range listingAudits {
		priceStr := ""
		if a.PriceCents != 0 {
			priceStr = fmt.Sprintf("€%.2f", float64(a.PriceCents)/100)
		}
		auditRows = append(auditRows, prettylog.ScrapedListingAudit{
			N:        i + 1,
			ID:       a.ItemID,
			Title:    a.Title,
			Price:    priceStr,
			TimeLine: marktplaats.ListingTimeSummary(a.ListedTSRaw, now),
			Passed:   a.Passed,
			Status:   a.Status,
			URL:      a.ListingURL,
		})
	}
	prettylog.ScrapedListingsAuditTable(auditRows)

	rows := make([]prettylog.ResultRow, 0, len(list))
	for i, l := range list {
		rows = append(rows, prettylog.ResultRow{
			N:        i + 1,
			ID:       l.ItemID,
			Title:    l.Title,
			EUR:      float64(l.PriceCents) / 100,
			URL:      l.ListingURL,
			TimeLine: marktplaats.ListingTimeSummary(l.ListedTS, now),
		})
	}
	prettylog.ScrapeTable(rows)
	prettylog.OKf("скрап завершён, объявлений: %d", len(list))
	if db != nil {
		prettylog.OKf("записей добавлено/обновлено в SQLite за эту сессию: %d", savedDB)
	}

	sendMailAfterScrape(db, m, list)
}

func sendMailAfterScrape(db *sql.DB, m mailOpts, list []marktplaats.Listing) {
	if !m.send || db == nil || len(list) == 0 {
		return
	}
	prettylog.Section("Почта (Gmail SMTP)")
	prettylog.Scrapef("воркер user_id=%d · объявлений в рассылке: %d", m.userID, len(list))
	doneMail := prettylog.Timer("рассылка писем продавцам")
	st := mailer.BulkSendListings(db, m.userID, list, m.delay, m.skipRCPT)
	doneMail()
	prettylog.OKf("писем отправлено: %d", st.OK)
	if st.NotExists > 0 {
		prettylog.Warnf("пропуск (RCPT: ящик не существует): %d", st.NotExists)
	}
	if st.Fail > 0 {
		prettylog.Warnf("ошибок / нет шаблона или почт: %d", st.Fail)
	}
}

// resolveProxiesFile loads proxies.txt from cwd or scraper-golang/proxies.txt when default name is used.
func resolveProxiesFile(name string) ([]proxy.Entry, string, error) {
	isDefault := name == "proxies.txt"
	try := []string{name}
	if isDefault {
		try = append(try, filepath.Join("scraper-golang", "proxies.txt"))
	}
	for _, p := range try {
		st, err := os.Stat(p)
		if err != nil || st.IsDir() {
			continue
		}
		entries, err := proxy.LoadFile(p)
		if err != nil {
			return nil, p, err
		}
		return entries, p, nil
	}
	if !isDefault {
		return nil, "", fmt.Errorf("файл прокси не найден: %q", name)
	}
	return nil, "", nil
}
