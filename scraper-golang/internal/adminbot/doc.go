// Package adminbot — админский Telegram-бот (аналог telegram_bot/admin_bot.py).
//
// Архитектура слоёв:
//
//   - Домен без внешних зависимостей: парсинг почт (parsers.go), переменные шаблонов (templates_domain.go).
//   - Данные: пакет listingsdb (SQLite, общая схема с Python-ботом и скрапером).
//   - Интеграции: Telegram long polling (bot.go, handlers.go, screens.go), тест SMTP (mailer.SendTestEmail).
//   - Сборка и конфиг: config.go, run.go.
//
// Владелец пула почт и шаблонов — user_id = ADMIN_CHAT_ID (как _admin_user_id() в Python).

package adminbot
