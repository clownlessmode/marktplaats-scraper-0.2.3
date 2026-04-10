package marktplaats

import (
	"sync"
	"time"
)

// TimingStats накапливает длительности внутри одного Scraper (навигация, ожидание __NEXT_DATA__).
type TimingStats struct {
	mu           sync.Mutex
	Navigate     time.Duration
	WaitNextData time.Duration
	Navigations  int
	WaitNextN    int
}

func (t *TimingStats) addNav(d time.Duration) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.Navigate += d
	t.Navigations++
}

func (t *TimingStats) addWaitNext(d time.Duration) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.WaitNextData += d
	t.WaitNextN++
}

// Snapshot копия для логов после скрапа.
func (t *TimingStats) Snapshot() (nav, waitNext time.Duration, nNav, nWait int) {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.Navigate, t.WaitNextData, t.Navigations, t.WaitNextN
}
