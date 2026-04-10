package marktplaats

import (
	"fmt"
	"strings"
	"time"
)

// ParseListedTime разбирает дату из API/страницы (ISO и варианты).
func ParseListedTime(listedTS string) (time.Time, error) {
	listedTS = strings.TrimSpace(listedTS)
	if listedTS == "" {
		return time.Time{}, fmt.Errorf("пустая дата")
	}
	ts := strings.ReplaceAll(listedTS, "Z", "+00:00")
	layouts := []string{
		time.RFC3339Nano,
		time.RFC3339,
		"2006-01-02T15:04:05.999999999-07:00",
		"2006-01-02 15:04:05+00:00",
		"2006-01-02T15:04:05",
		"2006-01-02 15:04:05",
	}
	var t time.Time
	var err error
	for _, layout := range layouts {
		t, err = time.Parse(layout, ts)
		if err == nil {
			return t.UTC(), nil
		}
	}
	return time.Time{}, err
}

// Marktplaats в __NEXT_DATA__ часто отдаёт «Vandaag», «Gisteren» вместо ISO (см. Python mpscraper).
func dutchListingDateHint(s string) (display string, ok bool) {
	low := strings.ToLower(strings.TrimSpace(s))
	switch low {
	case "vandaag":
		return "Vandaag — сегодня · в API нет часа (-fast); точная дата без флага -fast", true
	case "gisteren":
		return "Gisteren — вчера · в API нет часа (-fast); точная дата без флага -fast", true
	case "eergisteren":
		return "Eergisteren — позавчера · в API нет часа", true
	default:
		return "", false
	}
}

// ListingTimeSummary — строка для логов/таблицы: абсолютное время + «N ч назад».
func ListingTimeSummary(listedTS string, now time.Time) string {
	listedTS = strings.TrimSpace(listedTS)
	if listedTS == "" {
		return "без даты в выдаче"
	}
	if hint, ok := dutchListingDateHint(listedTS); ok {
		return hint
	}
	t, err := ParseListedTime(listedTS)
	if err != nil {
		s := listedTS
		if len([]rune(s)) > 40 {
			s = string([]rune(s)[:39]) + "…"
		}
		return "дата как есть: " + s
	}
	u := t.UTC()
	n := now.UTC()
	abs := u.Format("02.01.2006 15:04 UTC")
	h := n.Sub(u).Hours()
	if h < 0 {
		return abs + " · только что"
	}
	var rel string
	switch {
	case h < 1.0/60:
		rel = "только что"
	case h < 1:
		rel = fmt.Sprintf("%.0f мин назад", h*60)
	default:
		rel = fmt.Sprintf("%.1f ч назад", h)
	}
	return abs + " · " + rel
}

// AgeHoursSinceListed — сколько часов прошло с даты публикации до now (UTC).
// ok == false, если строку разобрать нельзя (как в Python: такое объявление не отбрасываем по возрасту).
func AgeHoursSinceListed(listedTS string, now time.Time) (hours float64, ok bool) {
	t, err := ParseListedTime(listedTS)
	if err != nil {
		return 0, false
	}
	u := t.UTC()
	n := now.UTC()
	if !u.Before(n) {
		return 0, true
	}
	return n.Sub(u).Hours(), true
}

// ExceedsMaxAge возвращает true, если возраст известен и строго больше порога.
func ExceedsMaxAge(listedTS string, maxHours float64, now time.Time) bool {
	if maxHours <= 0 {
		return false
	}
	h, ok := AgeHoursSinceListed(listedTS, now)
	return ok && h > maxHours
}
