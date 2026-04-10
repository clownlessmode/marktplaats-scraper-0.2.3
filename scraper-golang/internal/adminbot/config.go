// Package adminbot — Telegram админ-бот (порты: SQLite через listingsdb, опционально клиентский бот для уведомлений).
package adminbot

import (
	"os"
	"strconv"
	"strings"
)

// Config зависимости и секреты (из флагов + .env).
type Config struct {
	DBPath         string
	AdminBotToken  string
	AdminChatID    int64
	ClientBotToken string
	// APIEndpoint шаблон URL как у tgbotapi.APIEndpoint ("https://api.telegram.org/bot%s/%s").
	// Пусто — дефолт Telegram; для юнит-тестов или self-hosted Bot API можно задать свой базовый URL.
	APIEndpoint string
}

// LoadConfigFromEnv подставляет пустые поля из переменных окружения (как telegram_bot/config).
func (c *Config) LoadFromEnv() {
	if c.AdminBotToken == "" {
		c.AdminBotToken = strings.TrimSpace(os.Getenv("ADMIN_BOT_TOKEN"))
	}
	if c.ClientBotToken == "" {
		c.ClientBotToken = strings.TrimSpace(os.Getenv("CLIENT_BOT_TOKEN"))
		if c.ClientBotToken == "" {
			c.ClientBotToken = strings.TrimSpace(os.Getenv("BOT_TOKEN"))
		}
	}
	if c.AdminChatID == 0 {
		s := strings.TrimSpace(os.Getenv("ADMIN_CHAT_ID"))
		if s != "" {
			if id, err := strconv.ParseInt(s, 10, 64); err == nil {
				c.AdminChatID = id
			}
		}
	}
}

func (c *Config) validate() error {
	if c.AdminBotToken == "" {
		return errNoToken
	}
	if c.AdminChatID == 0 {
		return errNoAdminChat
	}
	if c.DBPath == "" {
		return errNoDB
	}
	return nil
}
