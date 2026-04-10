package clientbot

import (
	"os"
	"strconv"
	"strings"
)

// Config токены и пути (флаги + .env).
type Config struct {
	DBPath          string
	ClientBotToken  string
	AdminBotToken   string
	AdminChatID     int64
	// APIEndpoint шаблон URL как tgbotapi.APIEndpoint; пусто — Telegram; для тестов / self-hosted.
	APIEndpoint string
}

func (c *Config) LoadFromEnv() {
	if c.ClientBotToken == "" {
		c.ClientBotToken = strings.TrimSpace(os.Getenv("CLIENT_BOT_TOKEN"))
		if c.ClientBotToken == "" {
			c.ClientBotToken = strings.TrimSpace(os.Getenv("BOT_TOKEN"))
		}
	}
	if c.AdminBotToken == "" {
		c.AdminBotToken = strings.TrimSpace(os.Getenv("ADMIN_BOT_TOKEN"))
	}
	if c.AdminChatID == 0 {
		s := strings.TrimSpace(os.Getenv("ADMIN_CHAT_ID"))
		if s != "" {
			if id, err := strconv.ParseInt(s, 10, 64); err == nil {
				c.AdminChatID = id
			}
		}
	}
	if c.DBPath == "" {
		c.DBPath = strings.TrimSpace(os.Getenv("BOT_DB_PATH"))
		if c.DBPath == "" {
			c.DBPath = strings.TrimSpace(os.Getenv("DB_PATH"))
		}
	}
}

func (c *Config) validate() error {
	if strings.TrimSpace(c.ClientBotToken) == "" {
		return errNoClientToken
	}
	if strings.TrimSpace(c.DBPath) == "" {
		return errNoDB
	}
	return nil
}
