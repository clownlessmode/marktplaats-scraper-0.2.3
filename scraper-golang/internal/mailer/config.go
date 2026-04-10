package mailer

import (
	"fmt"
	"os"
	"strings"
)

// DevEnvironment: ENVIRONMENT=dev (или MP_ENVIRONMENT=dev) — как в Python: письма только на TEST_MAIL.
// Любое другое значение или пустая переменная = прод: реальные адреса seller@gmail.com и проверка RCPT.
func DevEnvironment() bool {
	v := strings.ToLower(strings.TrimSpace(os.Getenv("ENVIRONMENT")))
	if v == "" {
		v = strings.ToLower(strings.TrimSpace(os.Getenv("MP_ENVIRONMENT")))
	}
	return v == "dev"
}

// MailModeLogLine краткая строка для логов при -send-mail.
func MailModeLogLine() string {
	if DevEnvironment() {
		return fmt.Sprintf("почта: DEV — все письма на %s (TEST_MAIL / MP_TEST_MAIL)", testRecipient())
	}
	return "почта: PROD — получатель {имя_продавца}@gmail.com, перед отправкой проверка RCPT (как в Python)"
}

func testRecipient() string {
	s := strings.TrimSpace(os.Getenv("TEST_MAIL"))
	if s == "" {
		s = strings.TrimSpace(os.Getenv("MP_TEST_MAIL"))
	}
	if s == "" {
		return "eclipselucky@gmail.com"
	}
	return s
}

// TestRecipient адрес для тестовых писем (dev / кнопки админ-бота).
func TestRecipient() string { return testRecipient() }

func adminChatID() string {
	return strings.TrimSpace(os.Getenv("ADMIN_CHAT_ID"))
}

func adminBotToken() string {
	t := strings.TrimSpace(os.Getenv("ADMIN_BOT_TOKEN"))
	if t == "" {
		t = strings.TrimSpace(os.Getenv("CLIENT_BOT_TOKEN"))
	}
	if t == "" {
		t = strings.TrimSpace(os.Getenv("BOT_TOKEN"))
	}
	return t
}
