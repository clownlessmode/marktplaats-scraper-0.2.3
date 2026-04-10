package marktplaats

import "github.com/playwright-community/playwright-go"

const defaultUserAgent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"

// PlaywrightSession holds browser + optional context + page; call Shutdown when done.
type PlaywrightSession struct {
	PW      *playwright.Playwright
	Browser playwright.Browser
	Context playwright.BrowserContext
	Page    playwright.Page
}

// Shutdown closes page, context, browser, stops Playwright driver.
func (s *PlaywrightSession) Shutdown() {
	if s.Page != nil {
		_ = s.Page.Close()
		s.Page = nil
	}
	if s.Context != nil {
		_ = s.Context.Close()
		s.Context = nil
	}
	if s.Browser != nil {
		_ = s.Browser.Close()
		s.Browser = nil
	}
	if s.PW != nil {
		_ = s.PW.Stop()
		s.PW = nil
	}
}

// LeanResourcePolicy политика по умолчанию: минимум трафика, document/script/xhr/fetch не трогаем.
func LeanResourcePolicy() BrowserResourcePolicy {
	return BrowserResourcePolicy{
		BlockImages:       true,
		BlockStylesheets:  true,
		BlockFonts:        true,
		BlockExtras:       true,
	}
}

// BrowserResourcePolicy что резать на сети: для скрапа достаточно document + script + xhr/fetch;
// картинки/CSS/шрифты и «мусорные» типы режут трафик без влияния на __NEXT_DATA__ в HTML.
type BrowserResourcePolicy struct {
	BlockImages       bool
	BlockStylesheets  bool // CSS
	BlockFonts        bool
	// BlockExtras: media, manifest, субтитры, websocket, eventsource, ping — для текстового скрапа не нужны.
	BlockExtras bool
}

func installResourceRoutes(page playwright.Page, p BrowserResourcePolicy) error {
	return page.Route("**/*", func(route playwright.Route) {
		rt := route.Request().ResourceType()
		if p.BlockImages && rt == "image" {
			_ = route.Abort()
			return
		}
		if p.BlockStylesheets && rt == "stylesheet" {
			_ = route.Abort()
			return
		}
		if p.BlockFonts && rt == "font" {
			_ = route.Abort()
			return
		}
		if p.BlockExtras {
			switch rt {
			case "media", "manifest", "texttrack", "websocket", "eventsource", "ping":
				_ = route.Abort()
				return
			}
		}
		_ = route.Continue()
	})
}

// StartPlaywrightSession launches Chromium with stealth (same baseline as cmd/marktplaats-playwright).
// По policy режутся типы из BrowserResourcePolicy. Не режем: document, script, xhr, fetch — иначе ломается Next.js и данные в DOM.
func StartPlaywrightSession(headless bool, pwProxy *playwright.Proxy, policy BrowserResourcePolicy) (*PlaywrightSession, error) {
	pb, err := StartPlaywrightBrowser(headless, pwProxy)
	if err != nil {
		return nil, err
	}
	page, bctx, err := pb.NewStealthPage(nil, policy)
	if err != nil {
		pb.Shutdown()
		return nil, err
	}
	return &PlaywrightSession{PW: pb.PW, Browser: pb.Browser, Context: bctx, Page: page}, nil
}
