package main

import (
	"errors"
	"strings"

	"github.com/marktplaats-scraper/scraper-golang/internal/marktplaats"
)

// shouldRotatePlaywrightErr — ошибки, после которых имеет смысл следующий прокси / новый контекст.
func shouldRotatePlaywrightErr(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, marktplaats.ErrForbidden) {
		return true
	}
	s := strings.ToLower(err.Error())
	if strings.Contains(s, "timeout") {
		return true
	}
	if strings.Contains(s, "err_aborted") {
		return true
	}
	if strings.Contains(s, "target closed") {
		return true
	}
	if strings.Contains(s, "frame was detached") {
		return true
	}
	if strings.Contains(s, "net::err") {
		return true
	}
	return false
}

// maxProxyAttempts — число попыток с переключением прокси (минимум 3, максимум 20).
func maxProxyAttempts(n int) int {
	if n <= 0 {
		return 1
	}
	if n < 3 {
		return 3
	}
	if n > 20 {
		return 20
	}
	return n
}
