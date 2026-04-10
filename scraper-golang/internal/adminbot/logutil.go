package adminbot

import (
	"strings"
	"unicode/utf8"
)

// previewRunes укорачивает текст для логов (тело письма, длинные HTML).
func previewRunes(s string, max int) string {
	s = strings.TrimSpace(s)
	if s == "" {
		return "∅"
	}
	if max <= 0 {
		max = 80
	}
	if utf8.RuneCountInString(s) <= max {
		return s
	}
	r := []rune(s)
	if len(r) > max {
		return string(r[:max-1]) + "…"
	}
	return s
}

// previewOneLine убирает переводы строк для одной строки лога.
func previewOneLine(s string, max int) string {
	s = strings.ReplaceAll(s, "\r", " ")
	s = strings.ReplaceAll(s, "\n", " ↵ ")
	return previewRunes(s, max)
}
