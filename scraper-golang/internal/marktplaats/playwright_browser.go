package marktplaats

import (
	"fmt"

	stealth "github.com/jonfriesen/playwright-go-stealth"
	"github.com/playwright-community/playwright-go"
)

// PlaywrightBrowser один процесс Chromium; параллельные воркеры создают отдельные BrowserContext через NewStealthPage.
type PlaywrightBrowser struct {
	PW      *playwright.Playwright
	Browser playwright.Browser
}

// StartPlaywrightBrowser запускает драйвер и Chromium (опционально с прокси на уровне браузера).
// Контексты с отдельным прокси: Launch без прокси и передавайте proxy в NewStealthPage.
func StartPlaywrightBrowser(headless bool, launchProxy *playwright.Proxy) (*PlaywrightBrowser, error) {
	pw, err := playwright.Run()
	if err != nil {
		return nil, fmt.Errorf("playwright-драйвер: %w", err)
	}
	launch := playwright.BrowserTypeLaunchOptions{
		Headless: playwright.Bool(headless),
		Args: []string{
			"--disable-blink-features=AutomationControlled",
		},
	}
	if launchProxy != nil {
		launch.Proxy = launchProxy
	}
	browser, err := pw.Chromium.Launch(launch)
	if err != nil {
		_ = pw.Stop()
		return nil, fmt.Errorf("запуск chromium: %w", err)
	}
	return &PlaywrightBrowser{PW: pw, Browser: browser}, nil
}

// NewStealthPage новый изолированный контекст + вкладка, stealth и маршруты по policy.
// contextProxy не nil — прокси только для этого контекста (браузер без прокси на launch).
// contextProxy nil — без поля Proxy в контексте (наследование с launch, если прокси был на Launch).
func (pb *PlaywrightBrowser) NewStealthPage(contextProxy *playwright.Proxy, policy BrowserResourcePolicy) (playwright.Page, playwright.BrowserContext, error) {
	if pb == nil || pb.Browser == nil {
		return nil, nil, fmt.Errorf("браузер не инициализирован")
	}
	opts := playwright.BrowserNewContextOptions{
		UserAgent: playwright.String(defaultUserAgent),
		Locale:    playwright.String("nl-NL"),
	}
	if contextProxy != nil {
		opts.Proxy = contextProxy
	}
	bctx, err := pb.Browser.NewContext(opts)
	if err != nil {
		return nil, nil, fmt.Errorf("новый контекст: %w", err)
	}
	page, err := bctx.NewPage()
	if err != nil {
		_ = bctx.Close()
		return nil, nil, fmt.Errorf("новая вкладка: %w", err)
	}
	if err := stealth.InjectWithOptions(page, stealth.Options{ChromeStealth: true}); err != nil {
		_ = page.Close()
		_ = bctx.Close()
		return nil, nil, fmt.Errorf("stealth-скрипт: %w", err)
	}
	if policy.BlockImages || policy.BlockStylesheets || policy.BlockFonts || policy.BlockExtras {
		if err := installResourceRoutes(page, policy); err != nil {
			_ = page.Close()
			_ = bctx.Close()
			return nil, nil, fmt.Errorf("блокировка ресурсов (route): %w", err)
		}
	}
	return page, bctx, nil
}

// Shutdown закрывает браузер и останавливает Playwright (все контексты должны быть уже закрыты).
func (pb *PlaywrightBrowser) Shutdown() {
	if pb == nil {
		return
	}
	if pb.Browser != nil {
		_ = pb.Browser.Close()
		pb.Browser = nil
	}
	if pb.PW != nil {
		_ = pb.PW.Stop()
		pb.PW = nil
	}
}
