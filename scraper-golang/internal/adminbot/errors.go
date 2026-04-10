package adminbot

import "errors"

var (
	errNoToken     = errors.New("adminbot: нет токена (ADMIN_BOT_TOKEN или флаг -token)")
	errNoAdminChat = errors.New("adminbot: нет ADMIN_CHAT_ID")
	errNoDB        = errors.New("adminbot: не указан путь к SQLite (-db)")
)
