// Package prettylog — цветной вывод в терминал (учитывает NO_COLOR и TTY).
package prettylog

import (
	"fmt"
	"os"
	"strings"
	"time"
	"unicode/utf8"
)

const (
	ansiReset  = "\033[0m"
	ansiBold   = "\033[1m"
	ansiDim    = "\033[2m"
	ansiRed    = "\033[31m"
	ansiGreen  = "\033[32m"
	ansiYellow = "\033[33m"
	ansiBlue   = "\033[34m"
	ansiCyan   = "\033[36m"
	ansiGray   = "\033[90m"
	ansiHiCyan = "\033[96m"
	ansiHiGrn  = "\033[92m"
	ansiHiRed  = "\033[91m"
	ansiHiMagenta = "\033[95m"
)

var colorOn bool

func init() {
	colorOn = useColor()
}

func useColor() bool {
	if os.Getenv("NO_COLOR") != "" {
		return false
	}
	if strings.EqualFold(os.Getenv("TERM"), "dumb") {
		return false
	}
	fi, err := os.Stdout.Stat()
	if err != nil {
		return false
	}
	return (fi.Mode() & os.ModeCharDevice) != 0
}

func c(open, s string) string {
	if !colorOn {
		return s
	}
	return open + s + ansiReset
}

func dim(s string) string   { return c(ansiDim+ansiGray, s) }
func bold(s string) string  { return c(ansiBold, s) }
func red(s string) string   { return c(ansiHiRed, s) }
func green(s string) string { return c(ansiHiGrn, s) }
func yellow(s string) string { return c(ansiYellow, s) }
func cyan(s string) string  { return c(ansiHiCyan+ansiBold, s) }
func magenta(s string) string { return c(ansiHiMagenta, s) }
func blue(s string) string  { return c(ansiBlue, s) }

const tagW = 12

func padTag(tag string) string {
	n := utf8.RuneCountInString(tag)
	if n >= tagW {
		return tag
	}
	return tag + strings.Repeat(" ", tagW-n)
}

func timestamp() string {
	return dim(time.Now().Format("15:04:05"))
}

// line печатает: время │ ТЕГ │ основной текст [· приглушённые детали]
func line(tagStyled, msg string, detail string) {
	sep := dim("│")
	fmt.Fprintf(os.Stdout, "%s %s %s %s", timestamp(), sep, tagStyled, msg)
	if detail != "" {
		fmt.Fprintf(os.Stdout, " %s %s", dim("·"), dim(detail))
	}
	fmt.Fprintln(os.Stdout)
}

// --- Публичные теги ---

func Proxy(msg, detail string) { line(cyan(padTag("ПРОКСИ")), msg, detail) }
func Proxyf(format string, a ...any) { Proxy(fmt.Sprintf(format, a...), "") }

func Browser(msg, detail string) { line(blue(padTag("БРАУЗЕР")), msg, detail) }
func Browserf(format string, a ...any) { Browser(fmt.Sprintf(format, a...), "") }

func Scrape(msg, detail string) { line(green(padTag("СКРАП")), msg, detail) }
func Scrapef(format string, a ...any) { Scrape(fmt.Sprintf(format, a...), "") }

func Page(msg, detail string) { line(magenta(padTag("СТРАНИЦА")), msg, detail) }
func Pagef(format string, a ...any) { Page(fmt.Sprintf(format, a...), "") }

// Admin — события Telegram-админбота (кнопки, сообщения, исходящие действия).
func Admin(msg, detail string) { line(cyan(padTag("АДМИНБОТ")), msg, detail) }
func Adminf(format string, a ...any) { Admin(fmt.Sprintf(format, a...), "") }

// Worker — события Telegram-бота для воркеров (смена, почты, шаблоны, рассылка).
func Worker(msg, detail string) { line(green(padTag("ВОРКЕРБОТ")), msg, detail) }
func Workerf(format string, a ...any) { Worker(fmt.Sprintf(format, a...), "") }

func Warn(msg, detail string) { line(yellow(padTag("ВНИМАНИЕ")), msg, detail) }
func Warnf(format string, a ...any) { Warn(fmt.Sprintf(format, a...), "") }

func OK(msg, detail string) { line(green(padTag("ГОТОВО")), msg, detail) }
func OKf(format string, a ...any) { OK(fmt.Sprintf(format, a...), "") }

// PlaywrightWarn — предупреждения драйвера Playwright.
func PlaywrightWarn(msg string) {
	line(yellow(padTag("PLAYWRIGHT")), msg, "")
}

// Fatal печатает строку ОШИБКА в stderr и завершает процесс.
func Fatal(msg string) {
	sep := dim("│")
	tag := red(padTag("ОШИБКА"))
	if colorOn {
		fmt.Fprintf(os.Stderr, "%s %s %s %s%s%s\n", dim(time.Now().Format("15:04:05")), sep, tag, ansiHiRed+ansiBold, msg, ansiReset)
	} else {
		fmt.Fprintf(os.Stderr, "%s | ОШИБКА | %s\n", time.Now().Format("15:04:05"), msg)
	}
	os.Exit(1)
}

func Fatalf(format string, a ...any) {
	Fatal(fmt.Sprintf(format, a...))
}

// Section — визуальный разделитель с заголовком.
func Section(title string) {
	w := 56
	rule := dim(strings.Repeat("─", w))
	fmt.Fprintln(os.Stdout, rule)
	fmt.Fprintf(os.Stdout, "  %s\n", bold(title))
	fmt.Fprintln(os.Stdout, rule)
}

// Banner — стартовая плашка.
func Banner(title, subtitle string) {
	top := dim("╭" + strings.Repeat("─", 54) + "╮")
	mid := dim("│")
	bot := dim("╰" + strings.Repeat("─", 54) + "╯")
	fmt.Fprintln(os.Stdout, top)
	fmt.Fprintf(os.Stdout, "%s %s%s\n", mid, bold(title), ansiReset)
	if subtitle != "" {
		fmt.Fprintf(os.Stdout, "%s %s%s\n", mid, dim(subtitle), ansiReset)
	}
	fmt.Fprintln(os.Stdout, bot)
}

// ResultRow — одна строка итоговой таблицы.
type ResultRow struct {
	N        int
	ID       string
	Title    string
	EUR      float64
	URL      string
	TimeLine string // дата публикации и возраст (если пусто — строка в таблице не выводится)
}

// ScrapeTable красиво печатает собранные объявления.
func ScrapeTable(rows []ResultRow) {
	if len(rows) == 0 {
		Warn("объявлений нет", "")
		return
	}
	fmt.Fprintln(os.Stdout)
	title := fmt.Sprintf("Итог: %d объявлений", len(rows))
	fmt.Fprintln(os.Stdout, green(bold("▶ "+title)))
	rule := dim(strings.Repeat("─", 58))
	fmt.Fprintln(os.Stdout, rule)
	for _, r := range rows {
		t := r.Title
		if utf8.RuneCountInString(t) > 42 {
			t = string([]rune(t)[:41]) + "…"
		}
		fmt.Fprintf(os.Stdout, "  %s %s%s\n", dim(fmt.Sprintf("%2d.", r.N)), bold(fmt.Sprintf("%-14s", truncateRunes(r.ID, 14))), ansiReset)
		fmt.Fprintf(os.Stdout, "     %s %s%s\n", cyan(fmt.Sprintf("€%.2f", r.EUR)), ansiReset, t)
		if strings.TrimSpace(r.TimeLine) != "" {
			fmt.Fprintf(os.Stdout, "     %s %s%s\n", dim("опубликовано ·"), yellow(r.TimeLine), ansiReset)
		}
		fmt.Fprintf(os.Stdout, "     %s%s\n", dim("↳ "), dim(r.URL))
		fmt.Fprintln(os.Stdout)
	}
	fmt.Fprintln(os.Stdout, rule)
	fmt.Fprintln(os.Stdout, dim("  конец списка"))
	fmt.Fprintln(os.Stdout)
}

func truncateRunes(s string, max int) string {
	if utf8.RuneCountInString(s) <= max {
		return s
	}
	return string([]rune(s)[:max-1]) + "…"
}

// ScrapedListingAudit строка ленты: время + проходит ли порог / статус.
type ScrapedListingAudit struct {
	N        int
	ID       string
	Title    string
	Price    string
	TimeLine string
	Passed   bool
	Status   string
	URL      string
}

// ScrapedListingsAuditTable всегда выводит таблицу просмотренных объявлений (даже если итог пустой).
func ScrapedListingsAuditTable(rows []ScrapedListingAudit) {
	fmt.Fprintln(os.Stdout)
	title := "Лента объявлений (время · проходит · статус)"
	fmt.Fprintln(os.Stdout, cyan(bold("▶ "+title)))
	rule := dim(strings.Repeat("─", 62))
	fmt.Fprintln(os.Stdout, rule)
	if len(rows) == 0 {
		fmt.Fprintln(os.Stdout, dim("  (ни одной строки выдачи не разобрано — проверьте лимит/категорию/ошибки выше)"))
		fmt.Fprintln(os.Stdout, rule)
		fmt.Fprintln(os.Stdout)
		return
	}
	for _, r := range rows {
		t := r.Title
		if utf8.RuneCountInString(t) > 38 {
			t = string([]rune(t)[:37]) + "…"
		}
		passCol := red("нет")
		if r.Passed {
			passCol = green("да")
		}
		fmt.Fprintf(os.Stdout, "  %s %s%s\n", dim(fmt.Sprintf("%2d.", r.N)), bold(truncateRunes(r.ID, 14)), ansiReset)
		if strings.TrimSpace(t) != "" {
			fmt.Fprintf(os.Stdout, "     %s%s\n", dim(t), ansiReset)
		}
		fmt.Fprintf(os.Stdout, "     %s  %s  %s%s\n", passCol, dim("проходит ·"), dim(r.Status), ansiReset)
		if strings.TrimSpace(r.TimeLine) != "" {
			fmt.Fprintf(os.Stdout, "     %s %s%s\n", dim("время ·"), yellow(r.TimeLine), ansiReset)
		}
		if strings.TrimSpace(r.Price) != "" {
			fmt.Fprintf(os.Stdout, "     %s%s\n", cyan(r.Price), ansiReset)
		}
		fmt.Fprintf(os.Stdout, "     %s%s\n", dim("↳ "), dim(r.URL))
		fmt.Fprintln(os.Stdout)
	}
	fmt.Fprintln(os.Stdout, rule)
	fmt.Fprintf(os.Stdout, "  %s\n", dim(fmt.Sprintf("всего строк в ленте: %d", len(rows))))
	fmt.Fprintln(os.Stdout)
}
