package mailer

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"
)

// NotifyAdminBlocked уведомление в Telegram как _notify_admin_email_blocked.
func NotifyAdminBlocked(blockedEmail, reason string) {
	chat := adminChatID()
	token := adminBotToken()
	if chat == "" || token == "" {
		return
	}
	text := fmt.Sprintf("🚫 <b>Почта заблокирована</b>\n\n%s\n\nПричина: %s", blockedEmail, reason)
	payload := map[string]string{
		"chat_id":    chat,
		"text":       text,
		"parse_mode": "HTML",
	}
	body, _ := json.Marshal(payload)
	url := fmt.Sprintf("https://api.telegram.org/bot%s/sendMessage", token)
	req, err := http.NewRequest(http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return
	}
	req.Header.Set("Content-Type", "application/json")
	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return
	}
	defer resp.Body.Close()
}

func isNetworkLikeError(err error) bool {
	if err == nil {
		return false
	}
	s := strings.ToLower(err.Error())
	return strings.Contains(s, "connection") || strings.Contains(s, "unreachable") ||
		strings.Contains(s, "timeout") || strings.Contains(s, "proxy") || strings.Contains(s, "socks") ||
		strings.Contains(s, "network") || strings.Contains(s, "refused")
}
