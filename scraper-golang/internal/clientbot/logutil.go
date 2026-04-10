package clientbot

import (
	"strings"
	"unicode/utf8"
)

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
	return string(r[:max-1]) + "…"
}

func previewOneLine(s string, max int) string {
	s = strings.ReplaceAll(s, "\r", " ")
	s = strings.ReplaceAll(s, "\n", " ↵ ")
	return previewRunes(s, max)
}
