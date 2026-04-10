package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/playwright-community/playwright-go"

	"github.com/marktplaats-scraper/scraper-golang/internal/listingsdb"
	"github.com/marktplaats-scraper/scraper-golang/internal/marktplaats"
	"github.com/marktplaats-scraper/scraper-golang/internal/prettylog"
)

// scrapePoolParams общий скрап: очередь родительских категорий и N параллельных контекстов.
type scrapePoolParams struct {
	baseURL    string
	limit      int
	fast       bool
	skipCount  bool
	db         *sql.DB
	maxAge     *float64
	workers    int
	allParents bool
	parentIdx  int
	proxyRing  *proxyRing // nil — без прокси; при ошибках скрапа Advance() и новый контекст
	policy     marktplaats.BrowserResourcePolicy
}

func runScrapePool(_ context.Context, pb *marktplaats.PlaywrightBrowser, p scrapePoolParams) ([]marktplaats.Listing, int, *marktplaats.TimingStats, []marktplaats.ListingAuditRow, error) {
	if pb == nil {
		return nil, 0, nil, nil, errors.New("браузер не задан")
	}
	if p.workers < 1 {
		return nil, 0, nil, nil, errors.New("workers < 1")
	}

	stats := &marktplaats.TimingStats{}
	policy := p.policy

	var coPage playwright.Page
	var coCtx playwright.BrowserContext
	defer func() {
		if coPage != nil {
			_ = coPage.Close()
		}
		if coCtx != nil {
			_ = coCtx.Close()
		}
	}()

	maxCat := maxProxyAttempts(0)
	if p.proxyRing != nil {
		maxCat = maxProxyAttempts(p.proxyRing.Len())
	}
	prettylog.Section("Категории")
	prettylog.Scrapef("главная → родительские категории (%s)", p.baseURL)
	var parents []marktplaats.Category
	var err error
	for catTry := 0; catTry < maxCat; catTry++ {
		pw := (*playwright.Proxy)(nil)
		if p.proxyRing != nil {
			pw = p.proxyRing.Playwright()
		}
		coPage, coCtx, err = pb.NewStealthPage(pw, policy)
		if err != nil {
			return nil, 0, stats, nil, err
		}
		coSc := marktplaats.NewScraper(coPage)
		coSc.BaseURL = p.baseURL
		coSc.TimeoutMS = 30_000
		coSc.Stats = stats

		doneParents := prettylog.Timer("категории: главная, HTML и __CONFIG__")
		parents, err = coSc.GetParentCategories()
		doneParents()
		if err == nil {
			break
		}
		_ = coPage.Close()
		_ = coCtx.Close()
		coPage, coCtx = nil, nil
		if p.proxyRing == nil || !shouldRotatePlaywrightErr(err) {
			return nil, 0, stats, nil, err
		}
		prettylog.Warnf("категории: смена прокси · %v", err)
		p.proxyRing.Advance()
		prettylog.Proxy("следующий прокси (категории)", p.proxyRing.Mask())
	}
	if err != nil {
		return nil, 0, stats, nil, err
	}
	prettylog.OKf("найдено родительских категорий: %d", len(parents))
	if len(parents) == 0 {
		return nil, 0, stats, nil, errors.New("родительских категорий нет")
	}

	var toRun []marktplaats.Category
	if p.allParents {
		toRun = parents
	} else {
		if p.parentIdx < 0 || p.parentIdx >= len(parents) {
			return nil, 0, stats, nil, fmt.Errorf("parent-index %d вне диапазона [0,%d)", p.parentIdx, len(parents))
		}
		toRun = []marktplaats.Category{parents[p.parentIdx]}
	}

	prettylog.Section("Сбор объявлений (пул)")
	mode := "полный (страница каждого объявления)"
	if p.fast {
		mode = "быстрый (__NEXT_DATA__ только)"
	}
	prettylog.Scrape("режим", mode)
	prettylog.Scrapef("воркеров (контекстов): %d · родительских категорий в очереди: %d", p.workers, len(toRun))

	sharedSeen := make(map[string]struct{})
	if p.db != nil {
		existing, lerr := listingsdb.LoadItemIDs(p.db)
		if lerr != nil {
			return nil, 0, stats, nil, fmt.Errorf("SQLite: загрузка item_id: %w", lerr)
		}
		for id := range existing {
			sharedSeen[id] = struct{}{}
		}
		prettylog.Scrapef("в БД уже объявлений (дубликаты пропускаются): %d", len(existing))
	}

	var listingAudits []marktplaats.ListingAuditRow
	var auditMu sync.Mutex
	itemMu := sync.Mutex{}
	var batchMu sync.Mutex
	savedDB := 0

	jobs := make(chan marktplaats.Category, len(toRun))
	for _, c := range toRun {
		jobs <- c
	}
	close(jobs)

	var wg sync.WaitGroup
	var listMu sync.Mutex
	var allList []marktplaats.Listing
	var firstErr error
	var errOnce sync.Once

	doneListings := prettylog.Timer("объявления: пул, подкатегории, страницы поиска")

	for w := 0; w < p.workers; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for parent := range jobs {
				maxAtt := maxProxyAttempts(0)
				if p.proxyRing != nil {
					maxAtt = maxProxyAttempts(p.proxyRing.Len())
				}
				var list []marktplaats.Listing
				var gerr error
				var staleNote string
				for attempt := 0; attempt < maxAtt; attempt++ {
					pw := (*playwright.Proxy)(nil)
					if p.proxyRing != nil {
						pw = p.proxyRing.Playwright()
					}
					page, bctx, werr := pb.NewStealthPage(pw, policy)
					if werr != nil {
						errOnce.Do(func() { firstErr = werr })
						prettylog.Warnf("воркер %d: контекст · %v", workerID, werr)
						gerr = werr
						break
					}
					sc := marktplaats.NewScraper(page)
					sc.BaseURL = p.baseURL
					sc.Fast = p.fast
					sc.SkipCount = p.skipCount
					sc.TimeoutMS = 30_000
					sc.Stats = stats

					opts := marktplaats.GetListingsOptions{
						SkipCount:   p.skipCount,
						MaxAgeHours: p.maxAge,
						ExistingIDs: sharedSeen,
						ItemIDsMu:   &itemMu,
						AuditTrail:  &listingAudits,
						AuditMu:     &auditMu,
					}
					if p.db != nil {
						opts.OnBatch = func(batch []marktplaats.Listing) {
							batchMu.Lock()
							defer batchMu.Unlock()
							for _, item := range batch {
								if uerr := listingsdb.Upsert(p.db, item); uerr != nil {
									prettylog.Warnf("SQLite upsert %s: %v", item.ItemID, uerr)
									continue
								}
								savedDB++
							}
						}
					}

					list, gerr = sc.GetListings(parent, p.limit, opts)
					_ = page.Close()
					_ = bctx.Close()
					if gerr == nil {
						break
					}
					var cs *marktplaats.CategoryStaleError
					if errors.As(gerr, &cs) {
						staleNote = gerr.Error()
						list = cs.Listings
						gerr = nil
						break
					}
					if p.proxyRing == nil || !shouldRotatePlaywrightErr(gerr) {
						break
					}
					prettylog.Warnf("воркер %d · id=%d: ошибка, смена прокси · %v", workerID, parent.ID, gerr)
					p.proxyRing.Advance()
					prettylog.Proxy("следующий прокси (воркер)", p.proxyRing.Mask())
				}
				if gerr != nil {
					prettylog.Warnf("воркер %d · id=%d: %v", workerID, parent.ID, gerr)
					errOnce.Do(func() { firstErr = gerr })
					continue
				}
				if staleNote != "" {
					prettylog.Warnf("воркер %d · id=%d: прервано по возрасту · %s", workerID, parent.ID, staleNote)
				}
				prettylog.OKf("воркер %d · родитель id=%d · объявлений за категорию: %d", workerID, parent.ID, len(list))
				if len(list) > 0 {
					listMu.Lock()
					allList = append(allList, list...)
					listMu.Unlock()
				}
			}
		}(w)
	}
	wg.Wait()
	doneListings()

	nav, wNext, nNav, nWait := stats.Snapshot()
	if nNav > 0 {
		prettylog.TimingNote(fmt.Sprintf("сумма Goto/навигаций (%d переходов)", nNav), nav)
	}
	if nWait > 0 {
		prettylog.TimingNote(fmt.Sprintf("сумма ожиданий #__NEXT_DATA__ (%d раз)", nWait), wNext)
	}

	return allList, savedDB, stats, listingAudits, firstErr
}

func printPoolResults(listingAudits []marktplaats.ListingAuditRow, list []marktplaats.Listing, savedDB int, multiWorker bool, db *sql.DB) {
	prettylog.Section("Результат")
	now := time.Now()
	if !multiWorker {
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
	} else {
		prettylog.Scrapef("аудит (несколько воркеров): всего строк в ленте %d — полная таблица отключена", len(listingAudits))
	}

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
	prettylog.OKf("скрап завершён, объявлений (сумма по категориям): %d", len(list))
	if db != nil {
		prettylog.OKf("записей добавлено/обновлено в SQLite за этот проход: %d", savedDB)
	}
}
