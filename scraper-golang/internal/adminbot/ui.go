package adminbot

import (
	"strings"

	tgbotapi "github.com/go-telegram-bot-api/telegram-bot-api/v5"
)

func kbMain() tgbotapi.InlineKeyboardMarkup {
	return tgbotapi.NewInlineKeyboardMarkup(
		tgbotapi.NewInlineKeyboardRow(tgbotapi.NewInlineKeyboardButtonData("📋 Ожидают подтверждения", "admin_pending")),
		tgbotapi.NewInlineKeyboardRow(tgbotapi.NewInlineKeyboardButtonData("👥 Воркеры", "admin_workers")),
		tgbotapi.NewInlineKeyboardRow(tgbotapi.NewInlineKeyboardButtonData("🚫 Заблокированные", "admin_blocked")),
		tgbotapi.NewInlineKeyboardRow(tgbotapi.NewInlineKeyboardButtonData("📧 Почты", "admin_emails")),
		tgbotapi.NewInlineKeyboardRow(tgbotapi.NewInlineKeyboardButtonData("📝 Шаблоны", "admin_templates")),
	)
}

func kbBackMain() tgbotapi.InlineKeyboardMarkup {
	return tgbotapi.NewInlineKeyboardMarkup(
		tgbotapi.NewInlineKeyboardRow(tgbotapi.NewInlineKeyboardButtonData("◀️ Назад", "admin_main")),
	)
}

func emailToCallbackSafe(email string) string {
	e := strings.TrimSpace(strings.ToLower(email))
	e = strings.ReplaceAll(e, "_", "__")
	e = strings.ReplaceAll(e, "@", "_a_")
	e = strings.ReplaceAll(e, ":", "_c_")
	return e
}

// decodeEmailFromCallback обратное к emailToCallbackSafe (как в admin_bot.py).
func decodeEmailFromCallback(safe string) string {
	s := safe
	s = strings.ReplaceAll(s, "_c_", ":")
	s = strings.ReplaceAll(s, "_a_", "@")
	s = strings.ReplaceAll(s, "__", "_")
	return s
}
