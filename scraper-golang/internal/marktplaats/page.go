package marktplaats

import (
	"strings"
	"time"

	"github.com/playwright-community/playwright-go"
)

// Scraper drives one Playwright page against Marktplaats (Python MpScraper subset).
type Scraper struct {
	Page      playwright.Page
	BaseURL   string
	TimeoutMS float64 // default navigation / wait timeouts
	Fast      bool    // true: only __NEXT_DATA__ list rows; false: open each listing page

	SkipCount bool
	// Stats заполняется при навигации и ожидании __NEXT_DATA__ (для логов длительности).
	Stats *TimingStats

	adaptiveDelay time.Duration
}

// NewScraper returns a scraper; BaseURL defaults to marktplaats.nl (Python).
func NewScraper(page playwright.Page) *Scraper {
	return &Scraper{
		Page:          page,
		BaseURL:       BaseURL,
		TimeoutMS:     30_000,
		adaptiveDelay: 300 * time.Millisecond,
	}
}

func (s *Scraper) sleepAdaptive() {
	time.Sleep(s.adaptiveDelay)
}

func (s *Scraper) waitNextData() error {
	t0 := time.Now()
	t := max(15_000, s.TimeoutMS)
	_, err := s.Page.WaitForSelector("#"+NextDataScriptID, playwright.PageWaitForSelectorOptions{
		Timeout: playwright.Float(t),
		State:   playwright.WaitForSelectorStateAttached,
	})
	if s.Stats != nil {
		s.Stats.addWaitNext(time.Since(t0))
	}
	return err
}

// NextDataText returns raw JSON inside #__NEXT_DATA__ (Python soup + execute_script fallback).
func (s *Scraper) NextDataText() (string, error) {
	h, err := s.Page.QuerySelector("#" + NextDataScriptID)
	if err == nil && h != nil {
		txt, e := h.InnerText()
		if e == nil && strings.TrimSpace(txt) != "" {
			return txt, nil
		}
	}
	v, err := s.Page.Evaluate(`() => {
		const el = document.getElementById('__NEXT_DATA__');
		return el ? el.textContent : null;
	}`)
	if err != nil {
		return "", err
	}
	if v == nil {
		return "", nil
	}
	sv, _ := v.(string)
	return sv, nil
}

// HTMLContent returns document HTML (for goquery).
func (s *Scraper) HTMLContent() (string, error) {
	return s.Page.Content()
}

func (s *Scraper) navigate(u string) error {
	t0 := time.Now()
	_, err := s.Page.Goto(u, playwright.PageGotoOptions{
		WaitUntil: playwright.WaitUntilStateDomcontentloaded,
		Timeout:   playwright.Float(max(60_000, s.TimeoutMS)),
	})
	if s.Stats != nil {
		s.Stats.addNav(time.Since(t0))
	}
	return err
}

func max(a, b float64) float64 {
	if a > b {
		return a
	}
	return b
}

// PageLooksBlocked heuristics from Python (empty scripts / keywords).
func PageLooksBlocked(html string) bool {
	low := strings.ToLower(html)
	for _, p := range []string{"rate limit", "too many requests", "blocked", "captcha"} {
		if strings.Contains(low, p) {
			return true
		}
	}
	return strings.Count(low, "<script") < 2
}
