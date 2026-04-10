package clientbot

import "errors"

var (
	errNoClientToken = errors.New("clientbot: нет CLIENT_BOT_TOKEN (или BOT_TOKEN) в .env / флаг -token")
	errNoDB          = errors.New("clientbot: укажите путь к SQLite (-db)")
)
