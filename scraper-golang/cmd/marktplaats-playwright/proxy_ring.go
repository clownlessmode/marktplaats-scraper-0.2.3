package main

import (
	"sync"

	"github.com/playwright-community/playwright-go"

	"github.com/marktplaats-scraper/scraper-golang/internal/proxy"
)

// proxyRing — общий индекс для пула: при ошибке скрапа Advance() и новый контекст со следующим прокси.
type proxyRing struct {
	mu   sync.Mutex
	list []proxy.Entry
	i    int
}

func newProxyRing(list []proxy.Entry, startIdx int) *proxyRing {
	if len(list) == 0 {
		return nil
	}
	if startIdx < 0 || startIdx >= len(list) {
		startIdx = 0
	}
	return &proxyRing{list: list, i: startIdx}
}

func chosenEntryIndex(entries []proxy.Entry, chosen proxy.Entry) int {
	for i := range entries {
		e := entries[i]
		if e.Server == chosen.Server && e.Username == chosen.Username && e.Password == chosen.Password {
			return i
		}
	}
	return 0
}

func (r *proxyRing) Len() int {
	if r == nil {
		return 0
	}
	return len(r.list)
}

// Playwright — текущий слот (без сдвига).
func (r *proxyRing) Playwright() *playwright.Proxy {
	if r == nil || len(r.list) == 0 {
		return nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.list[r.i%len(r.list)].Playwright()
}

// Mask текущего прокси для логов.
func (r *proxyRing) Mask() string {
	if r == nil || len(r.list) == 0 {
		return ""
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.list[r.i%len(r.list)].Mask()
}

// Advance переключает на следующий прокси в списке.
func (r *proxyRing) Advance() {
	if r == nil || len(r.list) == 0 {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.i = (r.i + 1) % len(r.list)
}
